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
                _ => return Err(TreeError::InvalidRange),
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

