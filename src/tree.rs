mod record;
mod wal;
mod state;
mod recovery;


use std::path::Path;

use crate::tree::record::{Record, NodeRecord, BranchCreateRecord, RecordError};
use crate::tree::state::{State, StateError, NodeId, BranchId};
use crate::tree::wal::{Wal, WalError};
use crate::tree::recovery::{recover, RecoveryError};

/// Errors exposed by Tree API
#[derive(Debug)]
pub enum TreeError {
    Wal(WalError),
    State(StateError),
    Record(RecordError),
    Recovery(RecoveryError),
    InvalidRange,
}

impl From<WalError> for TreeError {
    fn from(e: WalError) -> Self {
        TreeError::Wal(e)
    }
}

impl From<StateError> for TreeError {
    fn from(e: StateError) -> Self {
        TreeError::State(e)
    }
}

impl From<RecordError> for TreeError {
    fn from(e: RecordError) -> Self {
        TreeError::Record(e)
    }
}

impl From<RecoveryError> for TreeError {
    fn from(e: RecoveryError) -> Self {
        TreeError::Recovery(e)
    }
}

pub struct Tree {
    pub document_uuid: u128,
    wal: Wal,
    state: State,
}

impl Tree {
    /// Create a new tree with a fresh WAL.
    ///
    /// Initializes:
    /// - branch_id = 0
    /// - tail_node_id = 0
    pub fn create_tree(path: &Path, document_uuid: u128) -> Result<Self, TreeError> {
        let wal = Wal::open(path)?;
        let state = State::new();

        let tree = Self {
            document_uuid,
            wal,
            state,
        };

        let record = Record::BranchCreate(BranchCreateRecord {
            branch_id: 0,
            parent_node_id: 0,
        });

        tree.wal.append(&record.encode())?;
        tree.state.insert_branch(0, 0);

        Ok(tree)
    }

    /// Open existing tree and recover state from WAL
    pub fn open_tree(path: &Path, document_uuid: u128) -> Result<Self, TreeError> {
        let wal = Wal::open(path)?;
        let state = recover(&wal)?;

        Ok(Self {
            document_uuid,
            wal,
            state,
        })
    }


    pub fn append_to_branch(
        &self,
        branch_id: BranchId,
        payload: Vec<u8>,
    ) -> Result<NodeId, TreeError> {
        let branch = self.state.get_branch(branch_id)?; 

        // lock branch (serialize appends)
        let mut tail_guard = branch.tail_node_id.lock().unwrap();

        let prev_node_id = *tail_guard;

        let node_id = self.state.next_node_id();

        let record = Record::Node(NodeRecord {
            node_id,
            branch_id,
            prev_node_id,
            payload,
        });

        let encoded = record.encode();
        let offset = self.wal.append(&encoded)?;

        // update in-memory state
        *tail_guard = node_id;
        self.state.insert_node(node_id, offset);

        Ok(node_id)
    }

    
    pub fn create_branch_from_parent_node(
        &self,
        parent_node_id: NodeId,
    ) -> Result<BranchId, TreeError> {
        if !self.state.node_exists(parent_node_id) {
            return Err(StateError::NodeNotFound(parent_node_id).into());
        }

        let branch_id = self.state.next_branch_id();

        let record = Record::BranchCreate(BranchCreateRecord {
            branch_id,
            parent_node_id,
        });

        self.wal.append(&record.encode())?;

        self.state.insert_branch(branch_id, parent_node_id);

        Ok(branch_id)
    }


    pub fn get_nodes_in_range(
        &self,
        head_node_id: NodeId,
        tail_node_id: NodeId,
    ) -> Result<Vec<Vec<u8>>, TreeError> {
        if !self.state.node_exists(head_node_id)
            || !self.state.node_exists(tail_node_id)
        {
            return Err(TreeError::InvalidRange);
        }

        let mut payloads = Vec::new();
        let mut curr = tail_node_id;

        loop {
            let offset = self.state.get_node_offset(curr)?;

            let buf = self.wal.read_at(offset)?;
            let record = Record::decode(&buf)?;

            let (payload, prev) = match record {
                Record::Node(n) => (n.payload, n.prev_node_id),
                Record::BranchCreate(_) => return Err(TreeError::InvalidRange),
            };

            payloads.push(payload);

            if curr == head_node_id {
                break;
            }

            curr = prev;

            if curr == 0 { // head is not ancestor of tail
                return Err(TreeError::InvalidRange);
            }
        }

        payloads.reverse();
        Ok(payloads)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("tree_test_{}_{}", name, std::process::id()));
        let _ = fs::remove_file(&path);
        path
    }

    // ------------------------------------------------------------
    // TEST 1: create_tree initializes root branch
    //
    // Verifies:
    // - branch 0 exists
    // - tail is 0
    // ------------------------------------------------------------
    #[test]
    fn test_create_tree() {
        let path = temp_path("create");
        let tree = Tree::create_tree(&path, 123).unwrap();

        let tail = tree.state.get_tail_node(0).unwrap();
        assert_eq!(tail, 0);
    }

    // ------------------------------------------------------------
    // TEST 2: append_to_branch basic functionality
    //
    // Verifies:
    // - node is appended
    // - tail updates
    // ------------------------------------------------------------
    #[test]
    fn test_append_to_branch() {
        let path = temp_path("append");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let node_id = tree.append_to_branch(0, b"hello".to_vec()).unwrap();

        let tail = tree.state.get_tail_node(0).unwrap();
        assert_eq!(tail, node_id);
    }

    // ------------------------------------------------------------
    // TEST 3: multiple appends maintain chain
    //
    // Verifies:
    // - multiple appends form correct linked structure
    // ------------------------------------------------------------
    #[test]
    fn test_multiple_appends() {
        let path = temp_path("multi_append");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let n1 = tree.append_to_branch(0, b"a".to_vec()).unwrap();
        let n2 = tree.append_to_branch(0, b"b".to_vec()).unwrap();
        let n3 = tree.append_to_branch(0, b"c".to_vec()).unwrap();

        let res = tree.get_nodes_in_range(n1, n3).unwrap();

        assert_eq!(res, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    // ------------------------------------------------------------
    // TEST 4: create_branch_from_parent_node
    //
    // Verifies:
    // - new branch starts at parent node
    // ------------------------------------------------------------
    #[test]
    fn test_branch_creation() {
        let path = temp_path("branch");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let n1 = tree.append_to_branch(0, b"a".to_vec()).unwrap();

        let new_branch = tree.create_branch_from_parent_node(n1).unwrap();

        let tail = tree.state.get_tail_node(new_branch).unwrap();
        assert_eq!(tail, n1);
    }

    // ------------------------------------------------------------
    // TEST 5: branches are independent
    //
    // Verifies:
    // - appending to one branch does not affect another
    // ------------------------------------------------------------
    #[test]
    fn test_branch_independence() {
        let path = temp_path("branch_independent");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let n1 = tree.append_to_branch(0, b"a".to_vec()).unwrap();
        let b2 = tree.create_branch_from_parent_node(n1).unwrap();

        let n2 = tree.append_to_branch(0, b"b".to_vec()).unwrap();
        let n3 = tree.append_to_branch(b2, b"x".to_vec()).unwrap();

        let tail_main = tree.state.get_tail_node(0).unwrap();
        let tail_branch = tree.state.get_tail_node(b2).unwrap();

        assert_eq!(tail_main, n2);
        assert_eq!(tail_branch, n3);
    }

    // ------------------------------------------------------------
    // TEST 6: invalid branch append fails
    //
    // Verifies:
    // - appending to non-existent branch errors
    // ------------------------------------------------------------
    #[test]
    fn test_append_invalid_branch() {
        let path = temp_path("invalid_branch");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let res = tree.append_to_branch(999, b"oops".to_vec());

        assert!(res.is_err());
    }

    // ------------------------------------------------------------
    // TEST 7: invalid range query
    //
    // Verifies:
    // - querying non-existent nodes fails
    // ------------------------------------------------------------
    #[test]
    fn test_invalid_range() {
        let path = temp_path("invalid_range");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let res = tree.get_nodes_in_range(1, 2);
        assert!(res.is_err());
    }

    // ------------------------------------------------------------
    // TEST 8: range not ancestor fails
    //
    // Verifies:
    // - head must be ancestor of tail
    // ------------------------------------------------------------
    #[test]
    fn test_non_ancestor_range() {
        let path = temp_path("non_ancestor");
        let tree = Tree::create_tree(&path, 1).unwrap();

        let n1 = tree.append_to_branch(0, b"a".to_vec()).unwrap();
        let n2 = tree.append_to_branch(0, b"b".to_vec()).unwrap();

        let b2 = tree.create_branch_from_parent_node(n1).unwrap();
        let n3 = tree.append_to_branch(b2, b"x".to_vec()).unwrap();

        // n2 is not ancestor of n3
        let res = tree.get_nodes_in_range(n2, n3);

        assert!(res.is_err());
    }

    // ------------------------------------------------------------
    // TEST 9: recovery end-to-end
    //
    // Verifies:
    // - state is reconstructed after reopening
    // ------------------------------------------------------------
    #[test]
    fn test_recovery_end_to_end() {
        let path = temp_path("recovery");

        {
            let tree = Tree::create_tree(&path, 1).unwrap();

            tree.append_to_branch(0, b"a".to_vec()).unwrap();
            tree.append_to_branch(0, b"b".to_vec()).unwrap();
        }

        // reopen
        let tree = Tree::open_tree(&path, 1).unwrap();

        let res = tree.get_nodes_in_range(1, 2).unwrap();
        assert_eq!(res, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    // ------------------------------------------------------------
    // TEST 10: branching + recovery together
    //
    // Verifies:
    // - branches persist across recovery
    // ------------------------------------------------------------
    #[test]
    fn test_branch_recovery() {
        let path = temp_path("branch_recovery");

        let branch_id;

        {
            let tree = Tree::create_tree(&path, 1).unwrap();

            let n1 = tree.append_to_branch(0, b"a".to_vec()).unwrap();
            branch_id = tree.create_branch_from_parent_node(n1).unwrap();

            tree.append_to_branch(branch_id, b"x".to_vec()).unwrap();
        }

        let tree = Tree::open_tree(&path, 1).unwrap();

        let tail = tree.state.get_tail_node(branch_id).unwrap();
        assert_eq!(tail, 2); // node_id after append
    }
}
