mod branch_page;
mod header_page;
mod log_page;
mod overflow_page;
mod storage;

use std::io;
use std::sync::{Mutex, RwLock};

use crate::tree::branch_page::{BranchDirectoryHeader, BranchesInfo};
use crate::tree::header_page::HeaderPage;
use crate::tree::log_page::LogPageHeader;
use crate::tree::storage::PagesStorage;

pub const PAGE_SIZE: u32 = 4096;

pub struct OnDiskTree<S: PagesStorage> {
    storage: S,
    document_uuid: u128,
    branch_dir_pages: RwLock<BranchesInfo>,
}

impl<S: PagesStorage> OnDiskTree<S> {
    pub fn create(storage: S, document_uuid: u128) -> io::Result<Self> {
        let header_pagenum = storage.allocate_page()?;

        // Allocate the first page of the branch directory
        let branch_dir_pagenum = storage.allocate_page()?;
        BranchDirectoryHeader::write(&storage.get_page(branch_dir_pagenum)?)?;

        let mut branch_info = BranchesInfo {
            pagenums: boxcar::Vec::new(),
            branches: boxcar::Vec::new(),
        };
        branch_info.pagenums.push(branch_dir_pagenum);

        // Create the root log page
        let root_log_pagenum = storage.allocate_page()?;
        LogPageHeader::write(&storage.get_page(root_log_pagenum)?, 0, 0, 0)?;

        // Create the entry in the branch index
        branch_info.create_branch_index_entry(&storage, 0, 0, root_log_pagenum)?;

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
            branch_dir_pages: RwLock::new(branch_info),
        })
    }

    pub fn open(storage: S) -> io::Result<Self> {
        let header = HeaderPage::read(&storage.get_page(0)?)?;
        let branch_info = BranchesInfo::load(&storage, header.branch_dir_pagenum)?;

        Ok(Self {
            storage,
            document_uuid: header.document_uuid,
            branch_dir_pages: RwLock::new(branch_info),
        })
    }
}
