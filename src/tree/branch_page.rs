use std::io;
use std::mem::{size_of, offset_of};
use std::sync::atomic::AtomicU32;
use std::sync::{Mutex, MutexGuard};
use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};

use crate::tree::log_page::{LOG_HEADER_SIZE, LogEntryHeader, LogPageHeader};
use crate::tree::storage::{PageHandle, PagesStorage};
use crate::tree::{PAGE_SIZE, TreeError, TreeResult};

pub const PAGE_TYPE_BRANCH_DIRECTORY: u8 = 1;
pub const BRANCH_DIR_HEADER_SIZE: u32 = size_of::<BranchDirectoryHeader>() as u32;
pub const BRANCH_DIR_ENTRY_SIZE: u32 = size_of::<BranchDirectoryEntry>() as u32;
pub const BRANCHES_PER_PAGE: u32 = (PAGE_SIZE - BRANCH_DIR_HEADER_SIZE) / BRANCH_DIR_ENTRY_SIZE;

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
    pub active_latest_log_pagenum: u8,
    pub _padding: [u8; 2],
    pub parent_branch_num: u32,
    pub parent_sequence_num: u64,
    pub latest_log_pagenum_0: u32,
    pub latest_log_pagenum_1: u32,
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
        next_pagenum: u32,
    ) -> io::Result<()> {
        // Write the pointer first
        page.write_type(8, &next_pagenum)?;
        page.sync();
        // Atomically mark as committed
        page.write_type(1, &1u8)?;
        Ok(())
    }
}

impl BranchDirectoryEntry {
    pub fn read(page: &impl PageHandle, index: u32) -> io::Result<Option<Self>> {
        let offset = BRANCH_DIR_HEADER_SIZE + index * BRANCH_DIR_ENTRY_SIZE;
        let data: Self = page.read_type(offset)?;
        if data.branch_committed == 1 {
            return Ok(Some(data));
        }
        assert_eq!(data.branch_committed, 0);
        return Ok(None);
    }

    /// Writes the entry uncommitted, syncs, then atomically marks it as committed.
    pub fn write_then_commit(
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
            active_latest_log_pagenum: 0,
            _padding: [0; 2],
            parent_branch_num,
            parent_sequence_num,
            latest_log_pagenum_0: initial_log_pagenum,
            latest_log_pagenum_1: 0,
        };

        page.write_type(offset, &entry)?;
        storage.sync();
        // Atomically mark as committed
        page.write_type(offset, &1u8);
        Ok(())
    }

    /// Atomically updates the latest log page number via double buffering:
    /// writes to the inactive slot, syncs, then flips the selector.
    pub fn write_latest_log_pagenum(
        page: &impl PageHandle,
        storage: &impl PagesStorage,
        index: u32,
        new_pagenum: u32,
    ) -> io::Result<()> {
        let base = BRANCH_DIR_HEADER_SIZE + index * BRANCH_DIR_ENTRY_SIZE;
        let entry_opt = Self::read(page, index)?;
        let Some(entry) = entry_opt else {
            return Err(io::Error::new(io::ErrorKind::Other, "Branch entry does not exist"));
        };

        let (write_field_offset, new_selector) = match entry.active_latest_log_pagenum {
            0 => {
                // Slot 0 is active, write to slot 1, then flip to 1
                let offset = base + offset_of!(BranchDirectoryEntry, latest_log_pagenum_1) as u32;
                (offset, 1u8)
            }
            1 => {
                // Slot 1 is active, write to slot 0, then flip to 0
                let offset = base + offset_of!(BranchDirectoryEntry, latest_log_pagenum_0) as u32;
                (offset, 0u8)
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::Other, "Invalid active_latest_log_pagenum value"));
            }
        };

        // Write the new value to the inactive slot
        page.write_type(write_field_offset, &new_pagenum)?;
        // Sync to ensure the value is durable before flipping
        storage.sync()?;
        // Flip the selector to make the new slot active
        let selector_offset = base + offset_of!(BranchDirectoryEntry, active_latest_log_pagenum) as u32;
        page.write_type(selector_offset, &new_selector)?;
        Ok(())
    }
}

// ==================== BranchesInfo ====================

pub struct NextNodeInfo {
    pub offset: u32,
    pub seq_num: u64,
}

impl NextNodeInfo {
    pub fn detect(page: &impl PageHandle) -> io::Result<Self> {
        let mut next_offset = LOG_HEADER_SIZE;
        let mut next_seq = LogPageHeader::read(page)?.first_sequence_num;

        while let Some(entry) = LogEntryHeader::read(page, next_offset)? {
            next_offset = entry.next_offset(next_offset);
            next_seq += 1;
        }
        return Ok(NextNodeInfo { offset: next_offset, seq_num: next_seq });
    }
}

pub struct BranchInfo {
    latest_log_pagenum: AtomicU32,
    /// Holds (offset, seq_num) for the last node in the branch
    last_node: Mutex<NextNodeInfo>,
}

impl BranchInfo {
    fn new(initial_log_pagenum: u32, next: NextNodeInfo) -> Self {
        Self {
            latest_log_pagenum: AtomicU32::new(initial_log_pagenum),
            last_node: Mutex::new(next),
        }
    }
}

pub struct BranchesInfo {
    /// Page numbers used for branches
    pagenums: boxcar::Vec<u32>,
    /// Info for each branch
    branches: boxcar::Vec<BranchInfo>,
    /// Must be locked when adding a branch
    add_branch_lock: Mutex<()>,
}

impl BranchesInfo {
    pub fn new(initial_page: u32) -> Self {
        let pagenums = boxcar::Vec::new();
        pagenums.push(initial_page);
        Self {
            pagenums,
            branches: boxcar::Vec::new(),
            add_branch_lock: Mutex::new(()),
        }
    }

    pub fn load(storage: &impl PagesStorage, initial_pagenum: u32) -> io::Result<Self> {
        let pagenums = boxcar::Vec::new();
        let branches = boxcar::Vec::new();
        let mut current_pagenum = initial_pagenum;

        loop {
            pagenums.push(current_pagenum);

            let page = storage.get_page(current_pagenum)?;
            let header = BranchDirectoryHeader::read(&page)?;

            let is_last_page = header.next_page_committed == 0;

            // Full page — all slots are committed
            for i in 0..BRANCHES_PER_PAGE {
                let Some(entry) = BranchDirectoryEntry::read(&page, i)? else {
                    if is_last_page {
                        break;
                    } else {
                        return Err(io::Error::new(io::ErrorKind::Other, "Uncommitted entry on full page"));
                    }
                };
                let pagenum = match entry.active_latest_log_pagenum {
                    0 => entry.latest_log_pagenum_0,
                    1 => entry.latest_log_pagenum_1,
                    _ => return Err(io::Error::new(io::ErrorKind::Other, "Invalid active_latest_log_pagenum value")),
                };

                // Find the next available offset on the log page
                let log_page = storage.get_page(pagenum)?;
                branches.push(BranchInfo::new(pagenum, NextNodeInfo::detect(&log_page)?));
            }

            if is_last_page {
                return Ok(Self { pagenums, branches, add_branch_lock: Mutex::new(()) });
            }

            current_pagenum = header.next_pagenum as u32;
        }
    }

    pub fn create_branch(
        &self,
        storage: &impl PagesStorage,
        parent_branch: u32,
        parent_seq: u64,
        log_pagenum: u32,
        parent_log_pagenum: u32,
    ) -> io::Result<u32> {
        let _guard = self.add_branch_lock.lock();

        let branch_num = self.branches.count() as u32;
        let index_in_page = branch_num % BRANCHES_PER_PAGE;

        // Write the log page header
        let log_page = storage.get_page(log_pagenum)?;
        LogPageHeader::write(&log_page, branch_num, parent_log_pagenum, parent_seq + 1)?;

        // Check if this branch will require creating a new index page
        if index_in_page == 0 && branch_num > 0 {
            // Allocate a new branch directory page and write the header
            let new_pagenum = storage.allocate_page()?;
            BranchDirectoryHeader::write(&storage.get_page(new_pagenum)?)?;

            // Set the 'next page' field of the previous branch directory page
            let prev_pagenum = self.pagenums[self.pagenums.count() - 1];
            let prev_page = storage.get_page(prev_pagenum)?;
            BranchDirectoryHeader::set_next_page(&prev_page, new_pagenum)?;

            self.pagenums.push(new_pagenum);
        }

        // Add the entry to the last page in the branch directory
        let last_pagenum = self.pagenums[self.pagenums.count() - 1];
        let page = storage.get_page(last_pagenum)?;

        // Add the entry, and then commit it
        BranchDirectoryEntry::write_then_commit(
            &page,
            storage,
            index_in_page,
            parent_branch,
            parent_seq,
            log_pagenum,
        )?;

        self.branches.push(BranchInfo::new(log_pagenum, NextNodeInfo {
            offset: LOG_HEADER_SIZE,
            seq_num: parent_seq + 1,
        }));
        Ok(branch_num)
    }

    pub fn lock_branch_for_append(&self, branch_num: u32) -> TreeResult<MutexGuard<NextNodeInfo>> {
        let info = self.branches.get(branch_num as usize).ok_or(TreeError::BranchNotFound)?;
        return Ok(info.last_node.lock().unwrap());
    }

    pub fn update_log_pagenum(
        &self,
        storage: &impl PagesStorage,
        branch_num: u32,
        new_log_pagenum: u32,
    ) -> TreeResult<()> {
        let branch_info = self.branches.get(branch_num as usize)
            .ok_or(TreeError::BranchNotFound)?;

        // Update on disk
        let index_in_page = branch_num % BRANCHES_PER_PAGE;
        let page_index = (branch_num / BRANCHES_PER_PAGE) as usize;
        let pagenum = self.pagenums.get(page_index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Invalid page index"))?;
        let page = storage.get_page(*pagenum)?;
        BranchDirectoryEntry::write_latest_log_pagenum(&page, storage, index_in_page, new_log_pagenum)?;

        // Update in-memory
        branch_info.latest_log_pagenum.store(new_log_pagenum, std::sync::atomic::Ordering::SeqCst);

        return Ok(());
    }

    pub fn branch_log_pagenum(&self, branch_num: u32) -> TreeResult<u32> {
        let branch_info = self.branches.get(branch_num as usize)
            .ok_or(TreeError::BranchNotFound)?;
        Ok(branch_info.latest_log_pagenum.load(std::sync::atomic::Ordering::SeqCst))
    }
}
