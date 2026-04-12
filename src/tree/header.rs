use crate::tree::storage::PageHandle;
use std::io;

const MAGIC: u64 = 0x4F4E4449534B5452; // "ONDISKTR"
const VERSION: u32 = 1;


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
        let magic = page.read_u64(0)?;
        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic"));
        }

        let version = page.read_u32(8)?;
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported version: {}", version),
            ));
        }

        let branch_dir_pagenum = page.read_u32(12)?;
        let document_uuid = page.read_u128(16)?;

        Ok(Self { magic, version, branch_dir_pagenum, document_uuid })
    }

    pub fn write(&self, page: &impl PageHandle) -> io::Result<()> {
        page.write_u64(0, self.magic)?;
        page.write_u32(8, self.version)?;
        page.write_u32(12, self.branch_dir_pagenum)?;
        page.write_u128(16, self.document_uuid)?;
        Ok(())
    }
}
