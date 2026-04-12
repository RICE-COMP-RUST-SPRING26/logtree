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

    /// Writes the next page pointer, syncs, then atomically marks it as committed.
    pub fn set_next_page(
        page: &impl PageHandle,
        storage: &impl PagesStorage,
        next_pagenum: u32,
    ) -> io::Result<()> {
        // Write the pointer first
        page.write_type(8, &next_pagenum)?;
        storage.sync();
        // Atomically mark as committed
        page.write_type(1, &1u8)?;
        Ok(())
    }
}

impl BranchDirectoryEntry {
    pub fn read(page: &impl PageHandle, index: u32) -> io::Result<Self> {
        let offset = BRANCH_DIR_HEADER_SIZE + index * BRANCH_DIR_ENTRY_SIZE;
        page.read_type(offset)
    }

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

    /// Writes the entry uncommitted, syncs, then atomically marks it as committed.
    pub fn write_and_commit(
        page: &impl PageHandle,
        storage: &impl PagesStorage,
        index: u32,
        parent_branch_num: u32,
        parent_sequence_num: u64,
        initial_log_pagenum: u32,
    ) -> io::Result<()> {
        let offset = BRANCH_DIR_HEADER_SIZE + index * BRANCH_DIR_ENTRY_SIZE;

        let entry = BranchDirectoryEntry {
            branch_committed: 0,
            current_op_pagenum: 0,
            _padding: [0; 2],
            parent_branch_num,
            parent_sequence_num,
            latest_op_pagenum_0: initial_log_pagenum,
            latest_op_pagenum_1: 0,
        };

        page.write_type(offset, &entry)?;
        storage.sync();
        // Atomically mark as committed
        page.write_type(offset, &1u8);
        Ok(())
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

    pub fn create_branch(
        &mut self,
        storage: &impl PagesStorage,
        parent_branch_num: u32,
        parent_sequence_num: u64,
        initial_log_pagenum: u32,
    ) -> io::Result<u32> {
        let branch_num = self.count;
        let index_in_page = branch_num % BRANCHES_PER_PAGE;

        if index_in_page == 0 && self.count > 0 {
            let new_pagenum = storage.allocate_page()?;
            BranchDirectoryHeader::write(&storage.get_page(new_pagenum)?)?;

            let prev_pagenum = *self.pagenums.last().unwrap();
            let prev_page = storage.get_page(prev_pagenum)?;
            BranchDirectoryHeader::set_next_page(&prev_page, storage, new_pagenum)?;

            self.pagenums.push(new_pagenum);
        }

        let last_pagenum = *self.pagenums.last().unwrap();
        let page = storage.get_page(last_pagenum)?;

        BranchDirectoryEntry::write_and_commit(
            &page,
            storage,
            index_in_page,
            parent_branch_num,
            parent_sequence_num,
            initial_log_pagenum,
        )?;

        self.count += 1;
        Ok(branch_num)
    }
}
