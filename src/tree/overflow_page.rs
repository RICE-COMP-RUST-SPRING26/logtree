use std::io;
use std::mem::size_of;
use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};

use crate::tree::storage::{PageHandle, PagesStorage};
use crate::tree::PAGE_SIZE;

pub const PAGE_TYPE_OVERFLOW: u8 = 3;
pub const OVERFLOW_HEADER_SIZE: u32 = size_of::<OverflowHeader>() as u32;
pub const OVERFLOW_PAYLOAD_PER_PAGE: u32 = PAGE_SIZE - OVERFLOW_HEADER_SIZE;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct OverflowHeader {
    pub page_type: u8,
    pub _padding: [u8; 3],
    pub next_overflow_pagenum: u32,
    pub total_length: u64,
}

impl OverflowHeader {
    pub fn read(page: &impl PageHandle) -> io::Result<Self> {
        page.read_type(0)
        // TODO: validate type?
    }

    pub fn write(page: &impl PageHandle, total_length: u64) -> io::Result<()> {
        let header = Self {
            page_type: PAGE_TYPE_OVERFLOW,
            _padding: [0; 3],
            next_overflow_pagenum: 0,
            total_length,
        };
        page.write_type(0, &header)
    }

    pub fn write_next_overflow_pagenum(page: &impl  PageHandle, value: u32) -> io::Result<()> {
        page.write_type(4, &value)?;
        Ok(())
    }
}

/// Writes a payload across one or more overflow pages.
/// Returns the page number of the first overflow page.
pub fn write_overflow(storage: &impl PagesStorage, payload: &[u8]) -> io::Result<u32> {
    let total_length = payload.len() as u64;
    let mut remaining = payload;

    // Allocate and initialize the first page
    let first_pagenum = storage.allocate_page()?;
    let mut current_page = storage.get_page(first_pagenum)?;
    OverflowHeader::write(&current_page, total_length)?;

    // Write as much as fits on the first page
    let chunk_size = remaining.len().min(OVERFLOW_PAYLOAD_PER_PAGE as usize);
    current_page.write(OVERFLOW_HEADER_SIZE, &remaining[..chunk_size])?;
    remaining = &remaining[chunk_size..];

    // Continue with additional pages as needed
    while !remaining.is_empty() {
        let next_pagenum = storage.allocate_page()?;
        let next_page = storage.get_page(next_pagenum)?;
        OverflowHeader::write(&next_page, total_length)?;

        // Link the previous page to this one
        OverflowHeader::write_next_overflow_pagenum(&current_page, next_pagenum)?;

        let chunk_size = remaining.len().min(OVERFLOW_PAYLOAD_PER_PAGE as usize);
        next_page.write(OVERFLOW_HEADER_SIZE, &remaining[..chunk_size])?;
        remaining = &remaining[chunk_size..];

        current_page = next_page;
    }

    Ok(first_pagenum)
}

/// Reads a complete payload from a chain of overflow pages.
/// Returns the reassembled payload bytes.
pub fn read_overflow(storage: &impl PagesStorage, first_pagenum: u32) -> io::Result<Vec<u8>> {
    let first_page = storage.get_page(first_pagenum)?;
    let header = OverflowHeader::read(&first_page)?;

    let total_length = header.total_length as usize;
    let mut result = Vec::with_capacity(total_length);
    let mut current_pagenum = first_pagenum;

    loop {
        let page = storage.get_page(current_pagenum)?;
        let header = OverflowHeader::read(&page)?;

        // Read as much payload as this page holds
        let remaining = total_length - result.len();
        let chunk_size = remaining.min(OVERFLOW_PAYLOAD_PER_PAGE as usize);
        let mut buf = vec![0u8; chunk_size];
        page.read(OVERFLOW_HEADER_SIZE, &mut buf)?;
        result.extend_from_slice(&buf);

        if header.next_overflow_pagenum == 0 {
            break;
        }
        current_pagenum = header.next_overflow_pagenum;
    }

    Ok(result)
}
