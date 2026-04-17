use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::tree::wal::Offset;

pub type NodeId = u64;
pub type BranchId = u64;

#[derive(Debug)]
pub enum StateError {
    BranchNotFound(BranchId),
    NodeNotFound(NodeId),
}

pub struct BranchInfo {
    pub tail_node_id: Mutex<NodeId>,
}

impl BranchInfo {
    pub fn new(tail_node_id: NodeId) -> Self {
        Self {
            tail_node_id: Mutex::new(tail_node_id),
        }
    }

    pub fn get_tail(&self) -> NodeId {
        *self.tail_node_id.lock().unwrap()
    }

    pub fn set_tail(&self, new_tail: NodeId) {
        let mut tail = self.tail_node_id.lock().unwrap();
        *tail = new_tail;
    }
}

pub struct NodeIndex {
    node_index: RwLock<HashMap<NodeId, Offset>>,
}

impl NodeIndex {
    fn new() -> Self {
        Self {
            node_index: RwLock::new(HashMap::new())
        }
    }

    fn insert_node(&self, node_id: NodeId, offset: Offset) {
        let mut map = self.node_index.write().unwrap();
        map.insert(node_id, offset);
    }

    fn get_node_offset(
        &self,
        node_id: NodeId,
    ) -> Result<Offset, StateError> {
        let map = self.node_index.read().unwrap();

        map.get(&node_id)
            .copied()
            .ok_or(StateError::NodeNotFound(node_id))
    }

    fn node_exists(&self, node_id: NodeId) -> bool {
        let map = self.node_index.read().unwrap();
        map.contains_key(&node_id)
    }
}


pub struct BranchIndex {
    pub branch_index: RwLock<HashMap<BranchId, Arc<BranchInfo>>>
}

impl BranchIndex {
    fn new() -> Self {
        Self {
            branch_index: RwLock::new(HashMap::new())
        }
    }

    fn insert_branch(&self, branch_id: BranchId, tail_node_id: NodeId) {
        let mut map = self.branch_index.write().unwrap();

        map.insert(
            branch_id,
            Arc::new(BranchInfo::new(tail_node_id)),
        );
    }

    fn get_branch(
        &self,
        branch_id: BranchId,
    ) -> Result<Arc<BranchInfo>, StateError> {
        let map = self.branch_index.read().unwrap(); 
        map.get(&branch_id).cloned().ok_or(StateError::BranchNotFound(branch_id))
    }

    fn get_tail_node(
        &self,
        branch_id: BranchId,
    ) -> Result<NodeId, StateError> {
        let branch = self.get_branch(branch_id)?;
        Ok(branch.get_tail())
    }

    fn set_tail_node(
        &self,
        branch_id: BranchId,
        node_id: NodeId,
    ) -> Result<(), StateError> {
        let branch = self.get_branch(branch_id)?;
        branch.set_tail(node_id);
        Ok(())
    }

}

/// In-memory state (index over WAL)
pub struct State {
    node_index: NodeIndex,
    branch_index: BranchIndex,

    last_node_id: AtomicU64,
    last_branch_id: AtomicU64,
}

impl State {
    pub fn new() -> Self {
        Self {
            node_index: NodeIndex::new(),
            branch_index: BranchIndex::new(),
            last_node_id: AtomicU64::new(0),
            last_branch_id: AtomicU64::new(0),
        }
    }

    pub fn insert_node(&self, node_id: NodeId, offset: Offset) {
        self.node_index.insert_node(node_id, offset);
    }

    pub fn get_node_offset(
        &self,
        node_id: NodeId,
    ) -> Result<Offset, StateError> {
        self.node_index.get_node_offset(node_id)
    }

    pub fn node_exists(&self, node_id: NodeId) -> bool {
        self.node_index.node_exists(node_id)
    }

    // ------------------------------------------------------------
    // Branch index
    // ------------------------------------------------------------

    pub fn insert_branch(&self, branch_id: BranchId, tail_node_id: NodeId) {
        self.branch_index.insert_branch(branch_id, tail_node_id);
    }

    pub fn get_branch(
        &self,
        branch_id: BranchId,
    ) -> Result<Arc<BranchInfo>, StateError> {
        self.branch_index.get_branch(branch_id)
    }

    pub fn get_tail_node(
        &self,
        branch_id: BranchId,
    ) -> Result<NodeId, StateError> {
        self.branch_index.get_tail_node(branch_id)
    }

    pub fn set_tail_node(
        &self,
        branch_id: BranchId,
        node_id: NodeId,
    ) -> Result<(), StateError> {
        self.branch_index.set_tail_node(branch_id, node_id)
    }

    
    pub fn next_node_id(&self) -> NodeId {
        self.last_node_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn next_branch_id(&self) -> BranchId {
        self.last_branch_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn set_last_branch_id(&self, branch_id: BranchId) {
        self.last_branch_id.store(branch_id, Ordering::SeqCst);
    }

    pub fn set_last_node_id(&self, branch_id: BranchId) {
        self.last_node_id.store(branch_id, Ordering::SeqCst);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------
    // TEST 1: Node insert + lookup
    //
    // Verifies:
    // - inserting a node stores correct offset
    // - strict lookup succeeds
    // ------------------------------------------------------------
    #[test]
    fn test_node_insert_and_lookup() {
        let state = State::new();

        state.insert_node(1, 100);

        let offset = state.get_node_offset(1).unwrap();
        assert_eq!(offset, 100);
    }

    // ------------------------------------------------------------
    // TEST 2: NodeNotFound error
    //
    // Verifies:
    // - strict lookup returns error if node missing
    // ------------------------------------------------------------
    #[test]
    fn test_node_not_found() {
        let state = State::new();

        let result = state.get_node_offset(42);

        assert!(matches!(result, Err(StateError::NodeNotFound(42))));
    }

    // ------------------------------------------------------------
    // TEST 3: node_exists correctness
    //
    // Verifies:
    // - existence check behaves correctly
    // ------------------------------------------------------------
    #[test]
    fn test_node_exists() {
        let state = State::new();

        assert!(!state.node_exists(1));

        state.insert_node(1, 123);

        assert!(state.node_exists(1));
    }

    // ------------------------------------------------------------
    // TEST 4: Branch insert + get tail
    //
    // Verifies:
    // - branch insertion works
    // - tail node is correctly initialized
    // ------------------------------------------------------------
    #[test]
    fn test_branch_insert_and_get_tail() {
        let state = State::new();

        state.insert_branch(1, 10);

        let tail = state.get_tail_node(1).unwrap();
        assert_eq!(tail, 10);
    }

    // ------------------------------------------------------------
    // TEST 5: BranchNotFound error
    //
    // Verifies:
    // - accessing missing branch returns error
    // ------------------------------------------------------------
    #[test]
    fn test_branch_not_found() {
        let state = State::new();

        let result = state.get_tail_node(99);

        assert!(matches!(result, Err(StateError::BranchNotFound(99))));
    }

    // ------------------------------------------------------------
    // TEST 6: set_tail_node updates correctly
    //
    // Verifies:
    // - tail node is updated after set
    // ------------------------------------------------------------
    #[test]
    fn test_set_tail_node() {
        let state = State::new();

        state.insert_branch(1, 10);
        state.set_tail_node(1, 20).unwrap();

        let tail = state.get_tail_node(1).unwrap();
        assert_eq!(tail, 20);
    }

    // ------------------------------------------------------------
    // TEST 7: set_tail_node error on missing branch
    //
    // Verifies:
    // - setting tail on non-existent branch fails
    // ------------------------------------------------------------
    #[test]
    fn test_set_tail_node_missing_branch() {
        let state = State::new();

        let result = state.set_tail_node(1, 10);

        assert!(matches!(result, Err(StateError::BranchNotFound(1))));
    }

    // ------------------------------------------------------------
    // TEST 8: ID generation monotonicity
    //
    // Verifies:
    // - node IDs increase monotonically
    // - branch IDs increase monotonically
    // ------------------------------------------------------------
    #[test]
    fn test_id_generation() {
        let state = State::new();

        let n1 = state.next_node_id();
        let n2 = state.next_node_id();

        assert!(n2 > n1);

        let b1 = state.next_branch_id();
        let b2 = state.next_branch_id();

        assert!(b2 > b1);
    }

    // ------------------------------------------------------------
    // TEST 9: set_last_ids correctness
    //
    // Verifies:
    // - setting last IDs updates future allocations
    // ------------------------------------------------------------
    #[test]
    fn test_set_last_ids() {
        let state = State::new();

        state.set_last_node_id(100);
        state.set_last_branch_id(50);

        let next_node = state.next_node_id();
        let next_branch = state.next_branch_id();

        assert_eq!(next_node, 101);
        assert_eq!(next_branch, 51);
    }

    // ------------------------------------------------------------
    // TEST 10: Multiple branches independent
    //
    // Verifies:
    // - updating one branch does not affect another
    // ------------------------------------------------------------
    #[test]
    fn test_multiple_branches_independent() {
        let state = State::new();

        state.insert_branch(1, 10);
        state.insert_branch(2, 20);

        state.set_tail_node(1, 100).unwrap();

        let tail1 = state.get_tail_node(1).unwrap();
        let tail2 = state.get_tail_node(2).unwrap();

        assert_eq!(tail1, 100);
        assert_eq!(tail2, 20);
    }
}


