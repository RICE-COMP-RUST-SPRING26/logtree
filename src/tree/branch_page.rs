use std::io;
use std::mem::size_of;
use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};

use crate::tree::storage::{PageHandle, PagesStorage};
use crate::tree::PAGE_SIZE;

const PAGE_TYPE_BRANCH_DIRECTORY: u8 = 1;
const BRANCH_DIR_HEADER_SIZE: u32 = size_of::<BranchDirectoryHeader>() as u32;
const BRANCH_DIR_ENTRY_SIZE: u32 = size_of::<BranchDirectoryEntry>() as u32;
const BRANCHES_PER_PAGE: u32 = (PAGE_SIZE - BRANCH_DIR_HEADER_SIZE) / BRANCH_DIR_ENTRY_SIZE;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BranchDirectoryHeader {
    pub page_type: u8,
    pub next_page_committed: u8,
    pub _padding: [u8; 6],
    pub next_pagenum: u64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct BranchDirectoryEntry {
    pub branch_committed: u8,
    pub current_op_pagenum: u8,
    pub _padding: [u8; 2],
    pub parent_branch_num: u32,
    pub parent_sequence_num: u64,
    pub latest_op_pagenum_0: u32,
    pub latest_op_pagenum_1: u32,
}

impl BranchDirectoryHeader {
    pub fn read(page: &impl PageHandle) -> io::Result<Self> {
        page.read_type(0)
    }

    pub fn write(page: &impl PageHandle) -> io::Result<()> {
        let header = Self {
            page_type: PAGE_TYPE_BRANCH_DIRECTORY,
            next_page_committed: 0,
            _padding: [0; 6],
            next_pagenum: 0,
        };
        page.write_type(0, &header)
    }
}

impl BranchDirectoryEntry {
    pub fn read(page: &impl PageHandle, index: u32) -> io::Result<Self> {
        let offset = BRANCH_DIR_HEADER_SIZE + index * BRANCH_DIR_ENTRY_SIZE;
        page.read_type(offset)
    }

    // Count how many committed branches exist on the current page
    pub fn committed_count(page: &impl PageHandle) -> io::Result<u32> {
        let mut count = 0;
        for i in 0..BRANCHES_PER_PAGE {
            let entry = Self::read(page, i)?;
            if entry.branch_committed == 0 {
                break;
            }
            count += 1;
        }
        Ok(count)
    }
}

// ==================== BranchesInfo ====================

pub struct BranchesInfo {
    /// Page numbers used for branches
    pub pagenums: Vec<u32>,
    /// Total number of branches
    pub count: u32,
}

impl BranchesInfo {
    pub fn load(storage: &impl PagesStorage, initial_pagenum: u32) -> io::Result<Self> {
        let mut pagenums = Vec::new();
        let mut count = 0;
        let mut current_pagenum = initial_pagenum;

        loop {
            pagenums.push(current_pagenum);

            let page = storage.get_page(current_pagenum)?;
            let header = BranchDirectoryHeader::read(&page)?;
            if header.next_page_committed == 0 {
                count += BranchDirectoryEntry::committed_count(&page)?;
                return Ok(Self { pagenums, count });
            }

            count += BRANCHES_PER_PAGE;
            current_pagenum = header.next_pagenum as u32;
        }
    }

    /// Creates a new branch in the directory, returning the new branch number.
    /// Allocates a new directory page if the current one is full.
    pub fn create_branch(
        &mut self,
        storage: &impl PagesStorage,
        parent_branch_num: u32,
        parent_sequence_num: u64,
        initial_log_pagenum: u32,
    ) -> io::Result<u32> {
        let branch_num = self.count;
        let index_in_page = branch_num % BRANCHES_PER_PAGE;

        // If the current directory page is full, allocate a new one
        // and link it from the previous page.
        if index_in_page == 0 && self.count > 0 {
            let new_pagenum = storage.allocate_page()?;
            BranchDirectoryHeader::write(&storage.get_page(new_pagenum)?)?;

            // Link the previous page to the new one.
            // Write the pointer first, then set committed, so a crash
            // between the two leaves the link invisible.
            let prev_pagenum = *self.pagenums.last().unwrap();
            let prev_page = storage.get_page(prev_pagenum)?;
            prev_page.write_type(4, &new_pagenum)?;
            storage.sync(); // Sync the page before comitting
            prev_page.write_type(1, &1u8)?;

            self.pagenums.push(new_pagenum);
        }

        let last_pagenum = *self.pagenums.last().unwrap();
        let page = storage.get_page(last_pagenum)?;

        // Write the entry with branch_committed = 0 first
        let entry = BranchDirectoryEntry {
            branch_committed: 0,
            current_op_pagenum: 0,
            _padding: [0; 2],
            parent_branch_num,
            parent_sequence_num,
            latest_op_pagenum_0: initial_log_pagenum,
            latest_op_pagenum_1: 0,
        };

        let offset = BRANCH_DIR_HEADER_SIZE + index_in_page * BRANCH_DIR_ENTRY_SIZE;
        page.write_type(offset, &entry)?;
        storage.sync();

        // Now atomically mark the branch as committed
        page.write_type(offset, &1u8)?;

        self.count += 1;
        Ok(branch_num)
    }
}
