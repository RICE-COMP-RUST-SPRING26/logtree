// Add these impl blocks to the respective files, or put this in a new `debug.rs` module.

use crate::tree::branch_page::BranchDirectoryEntry;
use crate::tree::branch_page::BRANCHES_PER_PAGE;
use crate::tree::branch_page::PAGE_TYPE_BRANCH_DIRECTORY;
use crate::tree::log_page::LogEntryHeader;
use crate::tree::log_page::LogPageHeader;
use crate::tree::log_page::LOG_HEADER_SIZE;
use crate::tree::log_page::PAGE_TYPE_LOG;
use crate::tree::overflow_page::OverflowHeader;
use crate::tree::overflow_page::PAGE_TYPE_OVERFLOW;
use crate::tree::storage::PageHandle;
use crate::tree::storage::PagesStorage;
use crate::tree::BranchDirectoryHeader;
use crate::tree::HeaderPage;
use std::io;

impl HeaderPage {
    pub fn print(page: &impl PageHandle) -> io::Result<()> {
        let header = Self::read(page)?;
        println!("=== Header Page ===");
        println!("  version: {}", header.version);
        println!("  document_uuid: {:032x}", header.document_uuid);
        println!("  branch_dir_pagenum: {}", header.branch_dir_pagenum);
        Ok(())
    }
}

impl BranchDirectoryHeader {
    pub fn print(page: &impl PageHandle) -> io::Result<()> {
        let header = Self::read(page)?;
        println!("=== Branch Directory Page ===");
        println!("  next_page_committed: {}", header.next_page_committed);
        println!("  next_pagenum: {}", header.next_pagenum);

        for i in 0..BRANCHES_PER_PAGE {
            let Some(entry) = BranchDirectoryEntry::read(page, i)? else {
                break;
            };
            let active_pagenum = match entry.active_latest_log_pagenum {
                0 => entry.latest_log_pagenum_0,
                1 => entry.latest_log_pagenum_1,
                _ => 0,
            };
            println!("  branch[{}]:", i);
            println!("    parent_branch: {}", entry.parent_branch_num);
            println!("    parent_seq: {}", entry.parent_sequence_num);
            println!("    active_slot: {}", entry.active_latest_log_pagenum);
            println!("    log_pagenum_0: {}", entry.latest_log_pagenum_0);
            println!("    log_pagenum_1: {}", entry.latest_log_pagenum_1);
            println!("    active_log_pagenum: {}", active_pagenum);
        }
        Ok(())
    }
}

impl LogPageHeader {
    pub fn print(page: &impl PageHandle) -> io::Result<()> {
        let header = Self::read(page)?;
        println!("=== Log Page ===");
        println!("  branch_num: {}", header.branch_num);
        println!("  prev_branch_pagenum: {}", header.prev_branch_pagenum);
        println!("  first_sequence_num: {}", header.first_sequence_num);

        let mut offset = LOG_HEADER_SIZE;
        let mut seq = header.first_sequence_num;
        while let Some(entry) = LogEntryHeader::read(page, offset)? {
            println!("  entry[seq={}]:", seq);
            println!("    offset: {}", offset);
            println!("    storage_mode: {:?}", entry.storage_mode);
            println!("    data_length: {}", entry.data_length);
            offset = entry.next_offset(offset);
            seq += 1;
        }
        Ok(())
    }
}

impl OverflowHeader {
    pub fn print(page: &impl PageHandle) -> io::Result<()> {
        let header = Self::read(page)?;
        println!("=== Overflow Page ===");
        println!("  total_length: {}", header.total_length);
        println!("  next_overflow_pagenum: {}", header.next_overflow_pagenum);
        Ok(())
    }
}

pub fn print_tree(storage: &impl PagesStorage) -> io::Result<()> {
    // Page 0 is always the header
    let page0 = storage.get_page(0)?;
    HeaderPage::print(&page0)?;

    let mut pagenum: u32 = 1;
    loop {
        let page = match storage.get_page(pagenum) {
            Ok(p) => p,
            Err(_) => break,
        };

        let page_type: u8 = match page.read_type(0) {
            Ok(t) => t,
            Err(_) => break,
        };

        println!("\n--- Page {} (type={}) ---", pagenum, page_type);
        match page_type {
            x if x == PAGE_TYPE_BRANCH_DIRECTORY => BranchDirectoryHeader::print(&page)?,
            x if x == PAGE_TYPE_LOG => LogPageHeader::print(&page)?,
            x if x == PAGE_TYPE_OVERFLOW => OverflowHeader::print(&page)?,
            _ => println!("  (unknown page type)"),
        }

        pagenum += 1;
    }

    Ok(())
}
