use crate::tree::record::{Record, RecordError};
use crate::tree::state::{State, StateError, NodeId, BranchId};
use crate::tree::wal::{Wal, WalError, Offset};

/// Errors that can occur during recovery.
#[derive(Debug)]
pub enum RecoveryError {
    Wal(WalError),
    Record(RecordError),
    State(StateError),
}

impl From<WalError> for RecoveryError {
    fn from(e: WalError) -> Self {
        RecoveryError::Wal(e)
    }
}

impl From<RecordError> for RecoveryError {
    fn from(e: RecordError) -> Self {
        RecoveryError::Record(e)
    }
}

impl From<StateError> for RecoveryError {
    fn from(e: StateError) -> Self {
        RecoveryError::State(e)
    }
}

/// Rebuild in-memory state from the WAL.
///
/// # Behavior
///
/// - Scans the WAL from offset 0 → end
/// - Decodes each record
/// - Reconstructs:
///     - node_index
///     - branch_index
///     - tail_node_id per branch
///     - last_node_id / last_branch_id
///
/// # Fault Tolerance
///
/// - If a record is partially written (e.g., crash during append),
///   recovery stops safely at that point
///
/// # Returns
///
/// - Fully reconstructed [`State`]
pub fn recover(wal: &Wal) -> Result<State, RecoveryError> {
    let state = State::new();

    let mut offset: Offset = 0;
    let mut last_valid_offset = 0;


    let mut max_node_id: NodeId = 0;
    let mut max_branch_id: BranchId = 0;

    loop {
        // Try to read record at current offset
        let buf = match wal.read_at(offset) {
            Ok(buf) => buf,
            Err(_) => {
                // Most likely hit end or partial record → stop recovery
                break;
            }
        };

        let record = match Record::decode(&buf) {
            Ok(r) => r,
            Err(_) => {
                // Corrupt or partial record → stop recovery safely
                break;
            }
        };

        match record {
            Record::Node(n) => {
                // Insert node → offset mapping
                state.insert_node(n.node_id, offset);

                // Update branch tail
                state.set_tail_node(n.branch_id, n.node_id)?;

                // Track max node id
                if n.node_id > max_node_id {
                    max_node_id = n.node_id;
                }
            }
            Record::BranchCreate(b) => {
                state.insert_branch(b.branch_id, b.parent_node_id);

                // Track max branch id
                if b.branch_id > max_branch_id {
                    max_branch_id = b.branch_id;
                }
            }
        }

        // Advance offset using record length from header
        let record_length = match extract_record_length(&buf) {
            Ok(len) => len,
            Err(_) => break,
        };
        offset += record_length as u64;
        last_valid_offset = offset
    }

    // Restore ID generators
    state.set_last_node_id(max_node_id);
    state.set_last_branch_id(max_branch_id);

    wal.truncate(last_valid_offset)?;

    Ok(state)
}

/// Extract record length from encoded buffer.
///
/// Assumes:
/// - byte[1..5] = u32 record_length (little-endian)
fn extract_record_length(buf: &[u8]) -> Result<u32, RecoveryError> {
    if buf.len() < 5 {
        return Err(RecoveryError::Record(RecordError::UnexpectedEOF));
    }

    let bytes: [u8; 4] = buf[1..5]
        .try_into()
        .map_err(|_| RecoveryError::Record(RecordError::LengthMismatch))?;

    Ok(u32::from_le_bytes(bytes))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::record::{Record, NodeRecord, BranchCreateRecord};
    use crate::tree::wal::Wal;
    use std::fs;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("recovery_test_{}_{}", name, std::process::id()));
        let _ = fs::remove_file(&path);
        path
    }

    // ------------------------------------------------------------
    // TEST 1: Empty WAL
    //
    // Verifies:
    // - recovery on empty WAL produces empty state
    // ------------------------------------------------------------
    #[test]
    fn test_recover_empty() {
        let path = temp_path("empty");
        let wal = Wal::open(&path).unwrap();

        let state = recover(&wal).unwrap();

        assert!(!state.node_exists(1));
        assert!(state.get_tail_node(1).is_err());
    }

    // ------------------------------------------------------------
    // TEST 2: Single branch + single node
    //
    // Verifies:
    // - branch and node reconstructed correctly
    // ------------------------------------------------------------
    #[test]
    fn test_recover_single_branch_and_node() {
        let path = temp_path("single");
        let wal = Wal::open(&path).unwrap();

        let b = Record::BranchCreate(BranchCreateRecord {
            branch_id: 1,
            parent_node_id: 0,
        });

        let n = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: b"hello".to_vec(),
        });

        wal.append(&b.encode()).unwrap();
        wal.append(&n.encode()).unwrap();

        let state = recover(&wal).unwrap();

        assert_eq!(state.get_tail_node(1).unwrap(), 1);
        assert!(state.node_exists(1));
    }

    // ------------------------------------------------------------
    // TEST 3: Multiple nodes update tail
    //
    // Verifies:
    // - latest node becomes branch tail
    // ------------------------------------------------------------
    #[test]
    fn test_recover_multiple_nodes_tail_updates() {
        let path = temp_path("multi_nodes");
        let wal = Wal::open(&path).unwrap();

        let b = Record::BranchCreate(BranchCreateRecord {
            branch_id: 1,
            parent_node_id: 0,
        });

        wal.append(&b.encode()).unwrap();

        for i in 1..=3 {
            let n = Record::Node(NodeRecord {
                node_id: i,
                branch_id: 1,
                prev_node_id: i - 1,
                payload: vec![i as u8],
            });

            wal.append(&n.encode()).unwrap();
        }

        let state = recover(&wal).unwrap();

        assert_eq!(state.get_tail_node(1).unwrap(), 3);
    }

    // ------------------------------------------------------------
    // TEST 4: Multiple branches independent
    //
    // Verifies:
    // - branches maintain independent tails
    // ------------------------------------------------------------
    #[test]
    fn test_recover_multiple_branches() {
        let path = temp_path("multi_branches");
        let wal = Wal::open(&path).unwrap();

        wal.append(&Record::BranchCreate(BranchCreateRecord {
            branch_id: 1,
            parent_node_id: 0,
        }).encode()).unwrap();

        wal.append(&Record::BranchCreate(BranchCreateRecord {
            branch_id: 2,
            parent_node_id: 0,
        }).encode()).unwrap();

        wal.append(&Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: vec![],
        }).encode()).unwrap();

        wal.append(&Record::Node(NodeRecord {
            node_id: 2,
            branch_id: 2,
            prev_node_id: 0,
            payload: vec![],
        }).encode()).unwrap();

        let state = recover(&wal).unwrap();

        assert_eq!(state.get_tail_node(1).unwrap(), 1);
        assert_eq!(state.get_tail_node(2).unwrap(), 2);
    }

    // ------------------------------------------------------------
    // TEST 5: Partial record at end is ignored
    //
    // Verifies:
    // - recovery stops safely at partial write
    // ------------------------------------------------------------
    #[test]
    fn test_recover_partial_record() {
        let path = temp_path("partial");
        let wal = Wal::open(&path).unwrap();

        let full = Record::BranchCreate(BranchCreateRecord {
            branch_id: 1,
            parent_node_id: 0,
        });

        wal.append(&full.encode()).unwrap();

        // create partial record (simulate crash)
        let mut partial = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: b"abc".to_vec(),
        })
        .encode();

        partial.truncate(partial.len() - 3);

        wal.append(&partial).unwrap();

        let state = recover(&wal).unwrap();

        // node should NOT exist because record was partial
        assert!(!state.node_exists(1));

        // branch still exists
        assert_eq!(state.get_tail_node(1).unwrap(), 0);
    }

    // ------------------------------------------------------------
    // TEST 6: ID restoration
    //
    // Verifies:
    // - next IDs continue from max seen in WAL
    // ------------------------------------------------------------
    #[test]
    fn test_recover_id_counters() {
        let path = temp_path("ids");
        let wal = Wal::open(&path).unwrap();

        wal.append(&Record::BranchCreate(BranchCreateRecord {
            branch_id: 5,
            parent_node_id: 0,
        }).encode()).unwrap();

        wal.append(&Record::Node(NodeRecord {
            node_id: 10,
            branch_id: 5,
            prev_node_id: 0,
            payload: vec![],
        }).encode()).unwrap();

        let state = recover(&wal).unwrap();

        let next_node = state.next_node_id();
        let next_branch = state.next_branch_id();

        assert_eq!(next_node, 11);
        assert_eq!(next_branch, 6);
    }
}
