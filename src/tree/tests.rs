use crate::tree::branch_page::{BranchDirectoryEntry, BranchDirectoryHeader, BRANCHES_PER_PAGE};
use crate::tree::header_page::HeaderPage;
use crate::tree::log_page::{LogEntryHeader, LogPageHeader, StorageMode, LOG_HEADER_SIZE};
use crate::tree::overflow_page::{OverflowHeader, OVERFLOW_PAYLOAD_PER_PAGE};
use crate::tree::storage::{PageHandle, PagesStorage};
use crate::tree::*;
use std::path::Path;

fn temp_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("ondisktree_tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[test]
fn test_create_disk_structure() {
    let path = temp_path("test_create_disk.db");
    cleanup(&path);

    let uuid: u128 = 0xDEADBEEF;
    {
        let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
        OnDiskTree::create(storage, uuid).unwrap();
    }

    // Reopen raw storage and inspect pages
    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();

    // Page 0: header
    let header = HeaderPage::read(&storage.get_page(0).unwrap()).unwrap();
    assert_eq!(header.version, 1);
    assert_eq!(header.document_uuid, uuid);
    assert_eq!(header.branch_dir_pagenum, 1);

    // Page 1: branch directory
    let bd_page = storage.get_page(1).unwrap();
    let bd_header = BranchDirectoryHeader::read(&bd_page).unwrap();
    assert_eq!(bd_header.page_type, 1); // PAGE_TYPE_BRANCH_DIRECTORY
    assert_eq!(bd_header.next_page_committed, 0); // no next page

    // Branch 0 should exist
    let entry = BranchDirectoryEntry::read(&bd_page, 0).unwrap().unwrap();
    assert_eq!(entry.parent_branch_num, 0);
    assert_eq!(entry.parent_sequence_num, 0);
    assert_eq!(entry.active_latest_log_pagenum, 0);
    assert_eq!(entry.latest_log_pagenum_0, 2); // first log page

    // Branch 1 should not exist
    assert!(BranchDirectoryEntry::read(&bd_page, 1).unwrap().is_none());

    // Page 2: empty log page for branch 0
    let log_header = LogPageHeader::read(&storage.get_page(2).unwrap()).unwrap();
    assert_eq!(log_header.branch_num, 0);
    assert_eq!(log_header.prev_branch_pagenum, 0);
    assert_eq!(log_header.first_sequence_num, 1);

    // No entries on the log page yet
    assert!(
        LogEntryHeader::read(&storage.get_page(2).unwrap(), LOG_HEADER_SIZE)
            .unwrap()
            .is_none()
    );

    cleanup(&path);
}

#[test]
fn test_append_disk_structure() {
    let path = temp_path("test_append_disk.db");
    cleanup(&path);

    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
    let tree = OnDiskTree::create(storage, 1).unwrap();

    tree.append_to_branch(0, b"hello").unwrap();
    tree.append_to_branch(0, b"world").unwrap();

    // Inspect log page (page 2)
    let storage = &tree.storage;
    let log_page = storage.get_page(2).unwrap();

    // First entry at LOG_HEADER_SIZE
    let entry1 = LogEntryHeader::read(&log_page, LOG_HEADER_SIZE)
        .unwrap()
        .unwrap();
    assert_eq!(entry1.storage_mode, StorageMode::Inline);
    assert_eq!(entry1.data_length, 5);

    // Read the raw inline bytes
    let mut buf = vec![0u8; 5];
    log_page
        .read(
            LOG_HEADER_SIZE + std::mem::size_of::<LogEntryHeader>() as u32,
            &mut buf,
        )
        .unwrap();
    assert_eq!(&buf, b"hello");

    // Second entry follows the first
    let offset2 = entry1.next_offset(LOG_HEADER_SIZE);
    let entry2 = LogEntryHeader::read(&log_page, offset2).unwrap().unwrap();
    assert_eq!(entry2.storage_mode, StorageMode::Inline);
    assert_eq!(entry2.data_length, 5);

    let mut buf2 = vec![0u8; 5];
    log_page
        .read(
            offset2 + std::mem::size_of::<LogEntryHeader>() as u32,
            &mut buf2,
        )
        .unwrap();
    assert_eq!(&buf2, b"world");

    // No third entry
    let offset3 = entry2.next_offset(offset2);
    assert!(LogEntryHeader::read(&log_page, offset3).unwrap().is_none());

    cleanup(&path);
}

#[test]
fn test_overflow_disk_structure() {
    let path = temp_path("test_overflow_disk.db");
    cleanup(&path);

    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
    let tree = OnDiskTree::create(storage, 2).unwrap();

    let big_data = vec![0xAB_u8; 2000];
    tree.append_to_branch(0, &big_data).unwrap();

    let storage = &tree.storage;

    // Log page entry should be overflow mode
    let log_page = storage.get_page(2).unwrap();
    let entry = LogEntryHeader::read(&log_page, LOG_HEADER_SIZE)
        .unwrap()
        .unwrap();
    assert_eq!(entry.storage_mode, StorageMode::Overflow);
    assert_eq!(entry.data_length, 4); // stores a u32 page number

    // Read the overflow page number from the inline data
    let data_offset = LOG_HEADER_SIZE + std::mem::size_of::<LogEntryHeader>() as u32;
    let overflow_pagenum: u32 = log_page.read_type(data_offset).unwrap();

    // Inspect the overflow page
    let overflow_page = storage.get_page(overflow_pagenum).unwrap();
    let overflow_header = OverflowHeader::read(&overflow_page).unwrap();
    assert_eq!(overflow_header.page_type, 3); // PAGE_TYPE_OVERFLOW
    assert_eq!(overflow_header.total_length, 2000);

    // 2000 bytes should fit in one overflow page (payload capacity ~4080)
    assert_eq!(overflow_header.next_overflow_pagenum, 0);

    // Verify first bytes of payload
    let mut first_bytes = vec![0u8; 16];
    overflow_page
        .read(
            std::mem::size_of::<OverflowHeader>() as u32,
            &mut first_bytes,
        )
        .unwrap();
    assert!(first_bytes.iter().all(|&b| b == 0xAB));

    cleanup(&path);
}

#[test]
fn test_overflow_multi_page_disk_structure() {
    let path = temp_path("test_overflow_multi_disk.db");
    cleanup(&path);

    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
    let tree = OnDiskTree::create(storage, 3).unwrap();

    let big_data = vec![0xCD_u8; 10000];
    tree.append_to_branch(0, &big_data).unwrap();

    let storage = &tree.storage;

    // Walk the overflow chain
    let log_page = storage.get_page(2).unwrap();
    let entry = LogEntryHeader::read(&log_page, LOG_HEADER_SIZE)
        .unwrap()
        .unwrap();
    let data_offset = LOG_HEADER_SIZE + std::mem::size_of::<LogEntryHeader>() as u32;
    let first_overflow: u32 = log_page.read_type(data_offset).unwrap();

    let mut current_pagenum = first_overflow;
    let mut total_payload = 0u64;
    let mut page_count = 0u32;

    loop {
        let page = storage.get_page(current_pagenum).unwrap();
        let header = OverflowHeader::read(&page).unwrap();
        assert_eq!(header.total_length, 10000);

        let remaining = 10000 - total_payload;
        let chunk = remaining.min(OVERFLOW_PAYLOAD_PER_PAGE as u64);
        total_payload += chunk;
        page_count += 1;

        // Verify payload bytes on this page
        let mut chunk_buf = vec![0u8; chunk as usize];
        page.read(std::mem::size_of::<OverflowHeader>() as u32, &mut chunk_buf)
            .unwrap();
        assert!(chunk_buf.iter().all(|&b| b == 0xCD));

        if header.next_overflow_pagenum == 0 {
            break;
        }
        current_pagenum = header.next_overflow_pagenum;
    }

    assert_eq!(total_payload, 10000);
    // Should need at least 3 pages (4080 bytes per page)
    assert!(page_count >= 3);

    cleanup(&path);
}

#[test]
fn test_branch_disk_structure() {
    let path = temp_path("test_branch_disk.db");
    cleanup(&path);

    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
    let tree = OnDiskTree::create(storage, 4).unwrap();

    tree.append_to_branch(0, b"main-1").unwrap();
    tree.append_to_branch(0, b"main-2").unwrap();

    let branch_num = tree.add_branch(0, 2).unwrap();
    assert_eq!(branch_num, 1);

    tree.append_to_branch(1, b"branch-data").unwrap();

    let storage = &tree.storage;

    // Branch directory should now have two entries
    let bd_page = storage.get_page(1).unwrap();

    let entry0 = BranchDirectoryEntry::read(&bd_page, 0).unwrap().unwrap();
    assert_eq!(entry0.parent_branch_num, 0);
    assert_eq!(entry0.parent_sequence_num, 0);

    let entry1 = BranchDirectoryEntry::read(&bd_page, 1).unwrap().unwrap();
    assert_eq!(entry1.parent_branch_num, 0);
    assert_eq!(entry1.parent_sequence_num, 2);

    // Branch 1's log page should point back to branch 0's log page
    let branch1_log_pagenum = match entry1.active_latest_log_pagenum {
        0 => entry1.latest_log_pagenum_0,
        1 => entry1.latest_log_pagenum_1,
        _ => panic!("invalid selector"),
    };

    let branch1_log = LogPageHeader::read(&storage.get_page(branch1_log_pagenum).unwrap()).unwrap();
    assert_eq!(branch1_log.branch_num, 1);
    assert_eq!(branch1_log.first_sequence_num, 3); // parent_seq + 1
    assert_eq!(branch1_log.prev_branch_pagenum, 2); // points to branch 0's log page

    // Verify the appended data is on branch 1's log page
    let branch1_page = storage.get_page(branch1_log_pagenum).unwrap();
    let entry = LogEntryHeader::read(&branch1_page, LOG_HEADER_SIZE)
        .unwrap()
        .unwrap();
    assert_eq!(entry.data_length, 11); // "branch-data".len()

    let mut buf = vec![0u8; 11];
    branch1_page
        .read(
            LOG_HEADER_SIZE + std::mem::size_of::<LogEntryHeader>() as u32,
            &mut buf,
        )
        .unwrap();
    assert_eq!(&buf, b"branch-data");

    cleanup(&path);
}

#[test]
fn test_log_page_rollover_disk_structure() {
    let path = temp_path("test_rollover_disk.db");
    cleanup(&path);

    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
    let tree = OnDiskTree::create(storage, 5).unwrap();

    // Fill up a page. Each entry ~= 8 (header) + 100 (data) + padding = ~108 bytes
    // Page capacity ~= (4096 - LOG_HEADER_SIZE) / 108 ≈ 37 entries
    // Append enough to guarantee at least one page rollover
    let data = [0x42u8; 100];
    for _ in 0..50 {
        tree.append_to_branch(0, &data).unwrap();
    }

    let storage = &tree.storage;

    // Branch directory should have been updated to point to a new log page
    let bd_page = storage.get_page(1).unwrap();
    let entry = BranchDirectoryEntry::read(&bd_page, 0).unwrap().unwrap();
    let latest_pagenum = match entry.active_latest_log_pagenum {
        0 => entry.latest_log_pagenum_0,
        1 => entry.latest_log_pagenum_1,
        _ => panic!("invalid selector"),
    };

    // The latest page should not be the original page 2
    assert_ne!(
        latest_pagenum, 2,
        "should have rolled over to a new log page"
    );

    // The latest log page should chain back
    let latest_header = LogPageHeader::read(&storage.get_page(latest_pagenum).unwrap()).unwrap();
    assert_eq!(latest_header.branch_num, 0);
    assert!(latest_header.first_sequence_num > 1);
    assert_ne!(latest_header.prev_branch_pagenum, 0);

    // Walk the chain back to the original page
    let mut pagenum = latest_pagenum;
    let mut pages_seen = 1;
    loop {
        let header = LogPageHeader::read(&storage.get_page(pagenum).unwrap()).unwrap();
        if header.prev_branch_pagenum == 0 {
            // Reached the original page (prev=0 means parent for root branch)
            break;
        }
        pagenum = header.prev_branch_pagenum;
        pages_seen += 1;
    }

    assert!(pages_seen >= 2, "should have at least 2 log pages");

    // Original page (page 2) should have first_sequence_num = 1
    let original = LogPageHeader::read(&storage.get_page(2).unwrap()).unwrap();
    assert_eq!(original.first_sequence_num, 1);

    cleanup(&path);
}

#[test]
fn test_reopen_preserves_disk_structure() {
    let path = temp_path("test_reopen_disk.db");
    cleanup(&path);

    {
        let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
        let tree = OnDiskTree::create(storage, 0xCAFE).unwrap();
        tree.append_to_branch(0, b"before-close").unwrap();
        tree.add_branch(0, 1).unwrap();
        tree.append_to_branch(1, b"on-branch").unwrap();
    }

    // Reopen and verify disk unchanged
    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();

    let header = HeaderPage::read(&storage.get_page(0).unwrap()).unwrap();
    assert_eq!(header.document_uuid, 0xCAFE);

    let bd_page = storage.get_page(header.branch_dir_pagenum).unwrap();
    let entry0 = BranchDirectoryEntry::read(&bd_page, 0).unwrap().unwrap();
    let entry1 = BranchDirectoryEntry::read(&bd_page, 1).unwrap().unwrap();

    // Verify branch 0 log page has the entry
    let b0_pagenum = match entry0.active_latest_log_pagenum {
        0 => entry0.latest_log_pagenum_0,
        1 => entry0.latest_log_pagenum_1,
        _ => panic!(),
    };
    let b0_page = storage.get_page(b0_pagenum).unwrap();
    let b0_entry = LogEntryHeader::read(&b0_page, LOG_HEADER_SIZE)
        .unwrap()
        .unwrap();
    assert_eq!(b0_entry.data_length, 12); // "before-close"

    // Verify branch 1 log page has the entry
    let b1_pagenum = match entry1.active_latest_log_pagenum {
        0 => entry1.latest_log_pagenum_0,
        1 => entry1.latest_log_pagenum_1,
        _ => panic!(),
    };
    let b1_page = storage.get_page(b1_pagenum).unwrap();
    let b1_header = LogPageHeader::read(&b1_page).unwrap();
    assert_eq!(b1_header.branch_num, 1);
    assert_eq!(b1_header.prev_branch_pagenum, b0_pagenum);

    let b1_entry = LogEntryHeader::read(&b1_page, LOG_HEADER_SIZE)
        .unwrap()
        .unwrap();
    assert_eq!(b1_entry.data_length, 9); // "on-branch"

    let mut buf = vec![0u8; 9];
    b1_page
        .read(
            LOG_HEADER_SIZE + std::mem::size_of::<LogEntryHeader>() as u32,
            &mut buf,
        )
        .unwrap();
    assert_eq!(&buf, b"on-branch");

    // Can reopen the tree and continue
    let tree = OnDiskTree::open(storage).unwrap();
    tree.append_to_branch(0, b"after-reopen").unwrap();
    let payloads = tree.read_range(0, 1, 2).unwrap();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0], b"before-close");
    assert_eq!(payloads[1], b"after-reopen");

    cleanup(&path);
}

#[test]
fn test_append_to_invalid_branch() {
    let path = temp_path("test_invalid_branch_disk.db");
    cleanup(&path);

    let storage = FilePagesStorage::open(&path, PAGE_SIZE).unwrap();
    let tree = OnDiskTree::create(storage, 8).unwrap();

    let result = tree.append_to_branch(999, b"nope");
    assert!(result.is_err());

    // Verify the tree is still intact — no garbage pages written
    let bd_page = tree.storage.get_page(1).unwrap();
    assert!(BranchDirectoryEntry::read(&bd_page, 0).unwrap().is_some());
    assert!(BranchDirectoryEntry::read(&bd_page, 1).unwrap().is_none());

    cleanup(&path);
}
