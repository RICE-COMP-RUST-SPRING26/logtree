

use std::fmt;
use std::io;



// ==================== Error type ====================

#[derive(Debug)]
pub enum TreeError {
    BranchNotFound,
    IoError(io::Error),
}

impl From<io::Error> for TreeError {
    fn from(err: io::Error) -> Self {
        TreeError::IoError(err)
    }
}

impl fmt::Display for TreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for TreeError {}

pub type TreeResult<T> = Result<T, TreeError>;
