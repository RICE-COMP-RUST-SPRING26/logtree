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
    pub fn new(branch_dir_pagenum: u32, document_uuid: u128) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            branch_dir_pagenum,
            document_uuid,
        }
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

        Ok(header)
    }

    pub fn write(&self, page: &impl PageHandle) -> io::Result<()> {
        page.write_type(0, self)
    }
}
