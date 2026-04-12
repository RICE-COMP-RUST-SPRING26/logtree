mod branch_page;
mod header_page;
mod overflow_page;
mod storage;

use std::io;
use std::sync::RwLock;

use crate::tree::branch_page::{BranchDirectoryHeader, BranchesInfo};
use crate::tree::header_page::HeaderPage;
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

        let branch_dir_pagenum = storage.allocate_page()?;
        BranchDirectoryHeader::write(&storage.get_page(branch_dir_pagenum)?)?;

        HeaderPage::write(
            &storage.get_page(header_pagenum)?,
            branch_dir_pagenum,
            document_uuid,
        )?;

        storage.sync()?;

        Ok(Self {
            storage,
            document_uuid,
            branch_dir_pages: RwLock::new(BranchesInfo {
                pagenums: vec![branch_dir_pagenum],
                count: 0,
            }),
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
