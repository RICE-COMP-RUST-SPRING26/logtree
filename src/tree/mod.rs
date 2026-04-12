mod branch_page;
mod header_page;
mod log_page;
mod overflow_page;
mod storage;

use std::io;
use std::sync::{Mutex, RwLock};

use crate::tree::branch_page::{BranchDirectoryHeader, BranchesInfo};
use crate::tree::header_page::HeaderPage;
use crate::tree::log_page::{LogEntryHeader, LogPageHeader};
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

        // Create the root log page
        let root_log_pagenum = storage.allocate_page()?;
        LogPageHeader::write(&storage.get_page(root_log_pagenum)?, 0, 0, 0)?;

        // Create the entry in the branch index
        let branches_info = BranchesInfo::new(branch_dir_pagenum);
        branches_info.create_branch_index_entry(&storage, 0, 0, root_log_pagenum)?;

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
            branches: RwLock::new(branches_info),
        })
    }

    pub fn open(storage: S) -> io::Result<Self> {
        let header = HeaderPage::read(&storage.get_page(0)?)?;
        let branch_info = BranchesInfo::load(&storage, header.branch_dir_pagenum)?;

        Ok(Self {
            storage,
            document_uuid: header.document_uuid,
            branches: RwLock::new(branch_info),
        })
    }

    pub fn read_range(
        &self,
        branch_num: u32,
        start_seq: u64,
        end_seq: u64,
    ) -> io::Result<Vec<Vec<u8>>> {
        // Find a page whose first element is less than the end num, so we've gone to far
        let mut log_page = self.branches.branch_log_pagenum(branch_num)?;
        let mut log_page_header = LogPageHeader::read(&self.storage.get_page(log_page)?)?;
        while log_page_header.first_sequence_num > end_seq {
            log_page = log_page_header.prev_branch_pagenum;
            log_page_header = LogPageHeader::read(&self.storage.get_page(log_page)?)?;
        }

        let mut payloads_reverse: Vec<Vec<u8>> = vec![];

        // On the current page, collect up until this sequence number
        let mut go_until_seq = end_seq;

        loop {
            let offset = LogEntryHeader::FIRST_OFFSET;
            let page = self.storage.get_page(log_page)?;
            let mut page_payloads = vec![];

            for seq in log_page_header.first_sequence_num..=go_until_seq {
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

            if log_page_header.first_sequence_num <= start_seq {
                break;
            }
            // Prepare for the next page
            go_until_seq = log_page_header.first_sequence_num - 1;
            log_page = log_page_header.prev_branch_pagenum;
            log_page_header = LogPageHeader::read(&self.storage.get_page(log_page)?)?;
        }

        return Ok(vec![]);
    }
}
