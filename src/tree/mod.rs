mod header;
mod storage;

use std::io;
use std::sync::Mutex;

use crate::tree::header::HeaderPage;
use crate::tree::storage::{PageHandle, PagesStorage};

const PAGE_SIZE: u32 = 4096;

pub struct OnDiskTree<S: PagesStorage> {
    storage: S,
    document_uuid: u128,
    branch_dir_pages: Mutex<Vec<u32>>,
}

impl<S: PagesStorage> OnDiskTree<S> {
    pub fn create(storage: S, document_uuid: u128) -> io::Result<Self> {
        let header_pagenum = storage.allocate_page()?;
        let branch_dir_pagenum = Self::create_branch_directory_page(&storage)?;

        HeaderPage::write(
            &storage.get_page(header_pagenum)?,
            branch_dir_pagenum,
            document_uuid,
        )?;

        storage.sync()?;

        Ok(Self {
            storage,
            document_uuid,
            branch_dir_pages: Mutex::new(vec![branch_dir_pagenum]),
        })
    }

    fn find_branch_pages(header: &HeaderPage, storage: &S) -> io::Result<Vec<u32>> {
        // Collect the branch directories
        let mut branch_dir_pages = Vec::<u32>::new();
        let mut current_pagenum = header.branch_dir_pagenum;
        loop {
            branch_dir_pages.push(current_pagenum);

            let page = storage.get_page(current_pagenum)?;
            let next_committed: u8 = page.read_type::<u8>(1)?;
            if next_committed == 0 {
                return Ok(branch_dir_pages);
            }

            current_pagenum = page.read_type(4)?;
        }
    }

    pub fn open(storage: S) -> io::Result<Self> {
        let header = HeaderPage::read(&storage.get_page(0)?)?;
        let branch_pages = Self::find_branch_pages(&header, &storage)?;

        Ok(Self {
            storage,
            document_uuid: header.document_uuid,
            branch_dir_pages: Mutex::new(branch_pages),
        })
    }

    fn create_branch_directory_page(storage: &S) -> io::Result<u32> {
        let pagenum = storage.allocate_page()?;
        // TODO: write page header
        Ok(pagenum)
    }
}
