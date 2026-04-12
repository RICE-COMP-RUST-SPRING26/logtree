use std::io;
use std::path::Path;

use clap::{Parser, Subcommand};

use crate::tree::{print_tree, FilePagesStorage, OnDiskTree, PAGE_SIZE};

#[derive(Parser)]
#[command(name = "ondisktree")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create a new tree file
    Create {
        file: String,
        #[arg(long)]
        uuid: Option<String>,
    },
    /// Print the full tree structure
    Print { file: String },
    /// Append data to a branch
    Append {
        file: String,
        #[arg(long, default_value_t = 0)]
        branch: u32,
        #[arg(long)]
        data: String,
    },
    /// Read a range of entries from a branch
    Read {
        file: String,
        #[arg(long, default_value_t = 0)]
        branch: u32,
        #[arg(long)]
        start: u64,
        #[arg(long)]
        end: u64,
    },
    /// Create a new branch forking from a parent
    AddBranch {
        file: String,
        #[arg(long)]
        parent_branch: u32,
        #[arg(long)]
        parent_seq: u64,
    },
}

pub fn run_cli() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Create { file, uuid } => {
            let storage = FilePagesStorage::open(Path::new(&file), PAGE_SIZE)?;
            let uuid = uuid
                .map(|s| u128::from_str_radix(&s, 16).expect("invalid hex uuid"))
                .unwrap_or_else(|| rand::random());
            OnDiskTree::create(storage, uuid)?;
            println!("created tree: {}", file);
        }
        Command::Print { file } => {
            let storage = FilePagesStorage::open(Path::new(&file), PAGE_SIZE)?;
            print_tree(&storage)?;
        }
        Command::Append { file, branch, data } => {
            let storage = FilePagesStorage::open(Path::new(&file), PAGE_SIZE)?;
            let tree = OnDiskTree::open(storage)?;
            let seq = tree
                .append_to_branch(branch, data.as_bytes())
                .map_err(|e| match e {
                    crate::tree::TreeError::IoError(io) => io,
                    crate::tree::TreeError::BranchNotFound => {
                        io::Error::new(io::ErrorKind::NotFound, "branch not found")
                    }
                })?;
            println!("appended at seq {}", seq);
        }
        Command::Read {
            file,
            branch,
            start,
            end,
        } => {
            let storage = FilePagesStorage::open(Path::new(&file), PAGE_SIZE)?;
            let tree = OnDiskTree::open(storage)?;
            let payloads = tree.read_range(branch, start, end).map_err(|e| match e {
                crate::tree::TreeError::IoError(io) => io,
                crate::tree::TreeError::BranchNotFound => {
                    io::Error::new(io::ErrorKind::NotFound, "branch not found")
                }
            })?;
            println!("{}", payloads.len());
            for (i, payload) in payloads.iter().enumerate() {
                let seq = start + i as u64;
                match std::str::from_utf8(payload) {
                    Ok(s) => println!("[{}] {}", seq, s),
                    Err(_) => println!("[{}] ({} bytes) {:?}", seq, payload.len(), payload),
                }
            }
        }
        Command::AddBranch {
            file,
            parent_branch,
            parent_seq,
        } => {
            let storage = FilePagesStorage::open(Path::new(&file), PAGE_SIZE)?;
            let tree = OnDiskTree::open(storage)?;
            let branch_num = tree
                .add_branch(parent_branch, parent_seq)
                .map_err(|e| match e {
                    crate::tree::TreeError::IoError(io) => io,
                    crate::tree::TreeError::BranchNotFound => {
                        io::Error::new(io::ErrorKind::NotFound, "branch not found")
                    }
                })?;
            println!("created branch {}", branch_num);
        }
    }

    Ok(())
}
