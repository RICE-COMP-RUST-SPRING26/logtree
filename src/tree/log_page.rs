use crate::tree::PAGE_SIZE;
use std::io;
use crate::tree::storage::PageHandle;
use std::mem::size_of;
use zerocopy::{TryFromBytes, IntoBytes, KnownLayout, Immutable};


const PAGE_TYPE_LOG: u8 = 2;
const LOG_HEADER_SIZE: u32 = size_of::<LogPageHeader>() as u32;
const LOG_ENTRY_HEADER_SIZE: u32 = size_of::<LogEntryHeader>() as u32;

// ==================== Log Header ====================

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(u8)]
pub enum LogPageType { LogPage = 2 }

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct LogPageHeader {
    pub page_type: LogPageType,
    pub _padding: [u8; 3],
    pub branch_num: u32,
    pub prev_branch_pagenum: u32,
    pub _padding2: [u8; 4],
    pub first_sequence_num: u64,
}

impl LogPageHeader {
    /// Reads the log page header from the given page.
    pub fn read(page: &impl PageHandle) -> io::Result<Self> {
        page.read_type(0)
    }

    /// Creates a new log page by writing a fresh header.
    pub fn write(
        page: &impl PageHandle,
        branch_num: u32,
        prev_branch_pagenum: u32,
        first_sequence_num: u64,
    ) -> io::Result<()> {
        let header = Self {
            page_type: LogPageType::LogPage,
            _padding: [0; 3],
            branch_num,
            prev_branch_pagenum,
            _padding2: [0; 4],
            first_sequence_num,
        };
        page.write_type(0, &header)
    }
}

// ==================== Log Entry ====================

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum StorageMode {
    Inline = 0,
    Overflow = 1,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct LogEntryHeader {
    pub committed: u8,
    pub storage_mode: StorageMode,
    pub _padding: [u8; 2],
    pub data_length: u32,
}

impl LogEntryHeader {
    /// Reads a log entry header at the given offset.
    /// Returns Ok(None) if the entry is not committed.
    pub fn read(page: &impl PageHandle, offset: u32) -> io::Result<Option<Self>> {
        // Check if there's room for at least the header
        if offset + LOG_ENTRY_HEADER_SIZE > PAGE_SIZE {
            return Ok(None);
        }

        if page.read_type::<u8>(offset)? != 1u8 {
            return Ok(None);
        }

        Ok(Some(page.read_type(offset)?))
    }

    /// Writes a log entry header at the given offset.
    /// Writes with committed = 0, then sets committed = 1 as a separate write.
    pub fn write_with_data(
        page: &impl PageHandle,
        offset: u32,
        storage_mode: StorageMode,
        data_length: u32,
        data: &[u8],
    ) -> io::Result<()> {
        let header = Self {
            committed: 0,
            storage_mode,
            _padding: [0; 2],
            data_length,
        };
        page.write_type(offset, &header)?;

        // Write the data at the end
        let data_offset = offset + (size_of::<Self>() as u32);
        page.write(data_offset, data)?;

        // Atomically mark as committed
        page.write_type(offset, &1u8)?;

        Ok(())
    }
}

