use std::io;
use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};
use crate::tree::storage::PageHandle;

const MAGIC: u64 = 0x4F4E4449534B5452; // "ONDISKTR"
const VERSION: u32 = 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct HeaderPage {
    pub magic: u64,
    pub version: u32,
    pub branch_dir_pagenum: u32,
    pub document_uuid: u128,
}

impl HeaderPage {
    pub fn write(page: &impl PageHandle, branch_dir_pagenum: u32, document_uuid: u128) -> io::Result<Self> {
        // TODO: validate branch_dir_pagenum > 0?
        let header = Self {
            magic: MAGIC,
            version: VERSION,
            branch_dir_pagenum,
            document_uuid,
        };
        page.write_type(0, &header)?;
        return Ok(header);
    }

    pub fn read(page: &impl PageHandle) -> io::Result<Self> {
        let header: Self = page.read_type(0)?;

        if header.magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic"));
        }

        if header.version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported version: {}", header.version),
            ));
        }
        
        // TODO: validate branch_dir_pagenum > 0?
        // TODO: validate document_uuid?
        Ok(header)
    }
}
