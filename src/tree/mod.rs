mod branch_page;
mod header_page;
mod log_page;
mod overflow_page;
mod storage;

use std::io;
use std::sync::Mutex;

use crate::tree::branch_page::{BranchDirectoryHeader, BranchesInfo};
use crate::tree::header_page::HeaderPage;
use crate::tree::log_page::{
    LogEntryHeader, LogPageHeader, LOG_ENTRY_HEADER_SIZE, LOG_HEADER_SIZE,
};
use crate::tree::overflow_page::write_overflow;
use crate::tree::storage::PagesStorage;

pub const PAGE_SIZE: u32 = 4096;

pub struct OnDiskTree<S: PagesStorage> {
    storage: S,
    document_uuid: u128,
    branches: BranchesInfo,
}

impl<S: PagesStorage> OnDiskTree<S> {
    pub fn create(storage: S, document_uuid: u128) -> io::Result<Self> {
        let header_pagenum = storage.allocate_page()?;

        // Allocate the first page of the branch directory
        let branch_dir_pagenum = storage.allocate_page()?;
        BranchDirectoryHeader::write(&storage.get_page(branch_dir_pagenum)?)?;

        // Create the root branch
        let branches_info = BranchesInfo::new(branch_dir_pagenum);
        let root_log_pagenum = storage.allocate_page()?;
        branches_info.create_branch(&storage, 0, 0, root_log_pagenum, 0)?;

        // Write the header page
        HeaderPage::write(
            &storage.get_page(header_pagenum)?,
            branch_dir_pagenum,
            document_uuid,
        )?;

        storage.sync()?;

        let branch_mutexes = boxcar::Vec::new();
        branch_mutexes.push(Mutex::new(()));

        Ok(Self {
            storage,
            document_uuid,
            branches: branches_info,
        })
    }

    pub fn open(storage: S) -> io::Result<Self> {
        let header = HeaderPage::read(&storage.get_page(0)?)?;
        let branch_info = BranchesInfo::load(&storage, header.branch_dir_pagenum)?;

        Ok(Self {
            storage,
            document_uuid: header.document_uuid,
            branches: branch_info,
        })
    }

    /// Returns (pagenum, page header)
    pub fn find_node_page(&self, branch_num: u32, seq: u64) -> TreeResult<(u32, LogPageHeader)> {
        let mut pagenum = self.branches.branch_log_pagenum(branch_num)?;
        let mut header = LogPageHeader::read(&self.storage.get_page(pagenum)?)?;

        while header.first_sequence_num > seq {
            pagenum = header.prev_branch_pagenum;
            header = LogPageHeader::read(&self.storage.get_page(pagenum)?)?;
        }

        return Ok((pagenum, header));
    }

    pub fn add_branch(&self, parent_branch: u32, parent_seq: u64) -> TreeResult<u32> {
        let (parent_node_pagenum, _) = self.find_node_page(parent_branch, parent_seq)?;

        let new_log_pagenum = self.storage.allocate_page()?;
        let new_branchnum = self.branches.create_branch(
            &self.storage,
            parent_branch,
            parent_seq,
            new_log_pagenum,
            parent_node_pagenum,
        )?;
        return Ok(new_branchnum);
    }

    pub fn read_range(
        &self,
        branch_num: u32,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<Vec<Vec<u8>>, TreeError> {
        // Find a page whose first element is less than the end num, so we've gone to far
        let (mut log_pagenum, mut log_header) = self.find_node_page(branch_num, end_seq)?;

        let mut payloads_reverse: Vec<Vec<u8>> = vec![];

        // On the current page, collect up until this sequence number
        let mut go_until_seq = end_seq;

        loop {
            let offset = LogEntryHeader::FIRST_OFFSET;
            let page = self.storage.get_page(log_pagenum)?;
            let mut page_payloads = vec![];

            for seq in log_header.first_sequence_num..=go_until_seq {
                let entry = LogEntryHeader::read(&page, offset)?.ok_or(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected end of log page",
                ))?;

                if seq >= start_seq {
                    page_payloads.push(entry.read_payload(&self.storage, &page, offset)?);
                }
            }

            // Add the payloads from this page to the combined vec
            page_payloads.reverse();
            payloads_reverse.extend(page_payloads);

            if log_header.first_sequence_num <= start_seq {
                break;
            }
            // Prepare for the next page
            go_until_seq = log_header.first_sequence_num - 1;
            log_pagenum = log_header.prev_branch_pagenum;
            log_header = LogPageHeader::read(&self.storage.get_page(log_pagenum)?)?;
        }

        return Ok(vec![]);
    }

    pub const MAX_INLINE_BYTES: usize = 1024;
    pub fn append_to_branch(&self, branch_num: u32, payload: &[u8]) -> TreeResult<u64> {
        let mut guard = self.branches.lock_branch_for_append(branch_num)?;
        let mut offset = guard.offset;
        let seq = guard.seq_num;

        let mut log_pagenum = self.branches.branch_log_pagenum(branch_num)?;

        let do_overflow = payload.len() > Self::MAX_INLINE_BYTES;

        let overflow_page_buffer: [u8; 4];
        let data: &[u8] = if do_overflow {
            let overflow_page = write_overflow(&self.storage, payload)?;
            overflow_page_buffer = overflow_page.to_le_bytes();
            &overflow_page_buffer.as_slice()
        } else {
            payload
        };

        // Allocate a new log page if necessary
        let available = PAGE_SIZE - offset;
        if (LOG_ENTRY_HEADER_SIZE + data.len() as u32) > available {
            // Allocate and format the new page
            let new_pagenum = self.storage.allocate_page()?;
            let new_page = self.storage.get_page(new_pagenum)?;
            LogPageHeader::write(&new_page, branch_num, log_pagenum, seq)?;

            // Write it to the branch index entry
            self.branches
                .update_log_pagenum(&self.storage, branch_num, new_pagenum)?;

            // Update the variables
            log_pagenum = new_pagenum;
            offset = LOG_HEADER_SIZE;
        }

        // Write the new entry to the log page
        let page = self.storage.get_page(log_pagenum)?;
        let entry = LogEntryHeader::write_with_data(&page, offset, do_overflow, data)?;

        // Update the cached next offset and next seq
        guard.offset = entry.next_offset(offset);
        guard.seq_num = seq + 1;

        return Ok(seq);
    }
}

// ==================== Error types ====================

pub enum TreeError {
    BranchNotFound,
    IoError(io::Error),
}

impl From<io::Error> for TreeError {
    fn from(err: io::Error) -> Self {
        TreeError::IoError(err)
    }
}

pub type TreeResult<T> = Result<T, TreeError>;
