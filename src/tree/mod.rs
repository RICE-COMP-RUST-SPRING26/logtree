mod storage;
mod header;

use std::io;
use std::sync::Mutex;

use crate::tree::storage::PagesStorage;

const PAGE_SIZE: u32 = 4096;

pub struct OnDiskTree<S: PagesStorage> {
    storage: S,
    document_uuid: u128,
    branch_dir_pages: Mutex<Vec<u32>>,
}

impl<S: PagesStorage> OnDiskTree<S> {
    pub fn create(storage: S, document_uuid: u128) -> io::Result<Self> {
        // Allocate page 0 (file header)
        let header_page = storage.allocate_page()?;
        debug_assert_eq!(header_page, 0);

        // Allocate the first branch directory page
        let branch_dir_pagenum = Self::create_branch_directory_page(&storage)?;

        // Write the file header
        let mut header = [0u8; PAGE_SIZE as usize];
        header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        header[8..12].copy_from_slice(&VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&branch_dir_pagenum.to_le_bytes());
        header[16..32].copy_from_slice(&document_uuid.to_le_bytes());
        storage.write_page(header_page, &header)?;

        storage.sync()?;

        Ok(Self {
            storage,
            document_uuid,
            branch_dir_pages: Mutex::new(vec![branch_dir_pagenum]),
        })
    }

    pub fn open(storage: S) -> io::Result<Self> {
        // Read and validate the file header
        let mut header = [0u8; PAGE_SIZE as usize];
        storage.read_page(0, &mut header)?;

        let magic = u64::from_le_bytes(header[0..8].try_into().unwrap());
        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic"));
        }

        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported version: {}", version),
            ));
        }

        let branch_dir_pagenum = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let document_uuid = u128::from_le_bytes(header[16..32].try_into().unwrap());

        // Walk the branch directory linked list to populate the cache
        let mut branch_dir_pages = vec![branch_dir_pagenum];
        let mut current_pagenum = branch_dir_pagenum;

        loop {
            let mut page = [0u8; PAGE_SIZE as usize];
            storage.read_page(current_pagenum, &mut page)?;

            let next_committed = page[1];
            if next_committed == 0 {
                break;
            }

            let next_pagenum = u64::from_le_bytes(page[8..16].try_into().unwrap());
            if next_pagenum == 0 {
                break;
            }

            current_pagenum = next_pagenum as u32;
            branch_dir_pages.push(current_pagenum);
        }

        Ok(Self {
            storage,
            document_uuid,
            branch_dir_pages: Mutex::new(branch_dir_pages),
        })
    }

    fn create_branch_directory_page(storage: &S) -> io::Result<u32> {
        let pagenum = storage.allocate_page()?;
        // TODO: write page header
        Ok(pagenum)
    }
}
