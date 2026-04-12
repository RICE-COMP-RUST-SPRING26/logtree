use crate::tree::{PAGE_SIZE, overflow_page::read_overflow, storage::PagesStorage};
use std::io;
use crate::tree::storage::PageHandle;
use std::mem::size_of;
use zerocopy::{TryFromBytes, IntoBytes, KnownLayout, Immutable};


pub const PAGE_TYPE_LOG: u8 = 2;
pub const LOG_HEADER_SIZE: u32 = size_of::<LogPageHeader>() as u32;
pub const LOG_ENTRY_HEADER_SIZE: u32 = size_of::<LogEntryHeader>() as u32;

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
    pub const FIRST_OFFSET: u32 = LOG_HEADER_SIZE;

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
        is_overflow: bool,
        data: &[u8],
    ) -> io::Result<Self> {
        assert!(offset + LOG_ENTRY_HEADER_SIZE + (data.len() as u32) < PAGE_SIZE);

        let header = Self {
            committed: 0,
            storage_mode: if is_overflow { StorageMode::Overflow } else { StorageMode::Inline },
            _padding: [0; 2],
            data_length: data.len() as u32,
        };
        page.write_type(offset, &header)?;

        // Write the data at the end
        let data_offset = offset + LOG_ENTRY_HEADER_SIZE;
        page.write(data_offset, data)?;

        // Atomically mark as committed
        page.write_type(offset, &1u8)?;

        Ok(header)
    }

    pub fn next_offset(&self, self_offset: u32) -> u32 {
        assert!(self_offset % 4 == 0);
        let mut data_len_padded = self.data_length;
        if data_len_padded % 4 != 0 {
            data_len_padded += 4 - (data_len_padded % 4);
        }

        return self_offset + LOG_ENTRY_HEADER_SIZE + data_len_padded;
    }

    pub fn read_payload(&self, storage: &impl PagesStorage, page: &impl PageHandle, self_offset: u32) -> io::Result<Vec<u8>> {
        assert!(self_offset % 4 == 0);

        let data_offset = self_offset + LOG_ENTRY_HEADER_SIZE;
        match self.storage_mode {
            StorageMode::Inline => {
                let mut buf = vec![0u8; self.data_length as usize];
                page.read(data_offset, buf.as_mut_slice())?;
                Ok(buf)
            },
            StorageMode::Overflow => {
                assert_eq!(self.data_length, 4);
                let first_overflow_pagenum = page.read_type::<u32>(data_offset)?;
                read_overflow(storage, first_overflow_pagenum)
            }
        }
    }
}

