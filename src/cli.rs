use std::io;
use std::path::Path;

use clap::{Parser, Subcommand};

use crate::tree::Tree;

type NodeId = u64;
type BranchId = u64;
#[derive(Parser)]
#[command(name = "wal_tree")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create a new tree
    Create {
        file: String,
        #[arg(long)]
        uuid: Option<String>,
    },

    /// Append payload to a branch
    Append {
        file: String,
        #[arg(long, default_value_t = 0)]
        branch: BranchId,
        #[arg(long)]
        data: String,
    },

    /// Create a new branch from a parent node
    Branch {
        file: String,
        #[arg(long)]
        parent: NodeId,
    },

    /// Read nodes in range [head → tail]
    Read {
        file: String,
        #[arg(long)]
        head: NodeId,
        #[arg(long)]
        tail: NodeId,
    },

    
}

pub fn run_cli() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // ------------------------------------------------------------
        // CREATE
        // ------------------------------------------------------------
        Command::Create { file, uuid } => {
            let uuid = uuid
                .map(|s| u128::from_str_radix(&s, 16).expect("invalid hex uuid"))
                .unwrap_or_else(|| rand::random());

            Tree::create_tree(Path::new(&file), uuid)
                .map_err(map_tree_err)?;

            println!("created tree: {}", file);
        }

        // ------------------------------------------------------------
        // APPEND
        // ------------------------------------------------------------
        Command::Append { file, branch, data } => {
            let tree = Tree::open_tree(Path::new(&file), 0)
                .map_err(map_tree_err)?;

            let node_id = tree
                .append_to_branch(branch, data.into_bytes())
                .map_err(map_tree_err)?;

            println!("appended node_id={}", node_id);
        }

        // ------------------------------------------------------------
        // BRANCH
        // ------------------------------------------------------------
        Command::Branch { file, parent } => {
            let tree = Tree::open_tree(Path::new(&file), 0)
                .map_err(map_tree_err)?;

            let branch_id = tree
                .create_branch_from_parent_node(parent)
                .map_err(map_tree_err)?;

            println!("created branch_id={}", branch_id);
        }

        // ------------------------------------------------------------
        // READ
        // ------------------------------------------------------------
        Command::Read { file, head, tail } => {
            let tree = Tree::open_tree(Path::new(&file), 0)
                .map_err(map_tree_err)?;

            let payloads = tree
                .get_nodes_in_range(head, tail)
                .map_err(map_tree_err)?;

            println!("{} entries:", payloads.len());

            for (i, payload) in payloads.iter().enumerate() {
                match std::str::from_utf8(payload) {
                    Ok(s) => println!("[{}] {}", i, s),
                    Err(_) => println!("[{}] ({} bytes) {:?}", i, payload.len(), payload),
                }
            }
        }
    }

    Ok(())
}

// ------------------------------------------------------------
// Error mapping
// ------------------------------------------------------------

fn map_tree_err(err: crate::tree::TreeError) -> io::Error {
    match err {
        crate::tree::TreeError::Wal(e) => {
            io::Error::new(io::ErrorKind::Other, format!("WAL error: {:?}", e))
        }
        crate::tree::TreeError::State(e) => {
            io::Error::new(io::ErrorKind::Other, format!("State error: {:?}", e))
        }
        crate::tree::TreeError::Recovery(e) => {
            io::Error::new(io::ErrorKind::Other, format!("Recovery error: {:?}", e))
        }
        crate::tree::TreeError::InvalidRange => {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid range")
        }
        crate::tree::TreeError::Record(_) => {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid range")
        }
    }
}

