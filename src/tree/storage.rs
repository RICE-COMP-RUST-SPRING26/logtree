use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt as UnixFileExt;
use std::path::Path;
use std::sync::Mutex;

use fs4::fs_std::FileExt as LockExt;


pub trait PageHandle {
    fn read(&self, offset: u32, buf: &mut [u8]) -> io::Result<()>;
    fn write(&self, offset: u32, buf: &[u8]) -> io::Result<()>;

    fn read_u8(&self, offset: u32) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read(offset, &mut buf)?;
        Ok(buf[0])
    }

    fn read_u32(&self, offset: u32) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read(offset, &mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64(&self, offset: u32) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read(offset, &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_u128(&self, offset: u32) -> io::Result<u128> {
        let mut buf = [0u8; 16];
        self.read(offset, &mut buf)?;
        Ok(u128::from_le_bytes(buf))
    }

    fn write_u8(&self, offset: u32, val: u8) -> io::Result<()> {
        self.write(offset, &[val])
    }

    fn write_u32(&self, offset: u32, val: u32) -> io::Result<()> {
        self.write(offset, &val.to_le_bytes())
    }

    fn write_u64(&self, offset: u32, val: u64) -> io::Result<()> {
        self.write(offset, &val.to_le_bytes())
    }

    fn write_u128(&self, offset: u32, val: u128) -> io::Result<()> {
        self.write(offset, &val.to_le_bytes())
    }
}

pub trait PagesStorage: Send + Sync {
    type Page<'a>: PageHandle where Self: 'a;

    fn get_page(&self, index: u32) -> io::Result<Self::Page<'_>>;
    fn allocate_page(&self) -> io::Result<u32>;
    fn sync(&self) -> io::Result<()>;
}

// ==================== Implementation for a file ====================

pub struct FilePagesStorage {
    file: File,
    page_size: u32,
    page_count: Mutex<u32>,
}

pub struct FilePageHandle<'a> {
    file: &'a File,
    page_size: u32,
    index: u32,
}

impl FilePagesStorage {
    pub fn open(path: &Path, page_size: u32) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.try_lock_exclusive()?;

        let file_size = file.metadata()?.len();
        let page_count = if file_size == 0 {
            0
        } else {
            (file_size / page_size as u64) as u32
        };

        Ok(Self {
            file,
            page_size,
            page_count: Mutex::new(page_count),
        })
    }
}

impl Drop for FilePagesStorage {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl PageHandle for FilePageHandle<'_> {
    fn read(&self, offset: u32, buf: &mut [u8]) -> io::Result<()> {
        let abs_offset = self.index as u64 * self.page_size as u64 + offset as u64;
        self.file.read_exact_at(buf, abs_offset)
    }

    fn write(&self, offset: u32, buf: &[u8]) -> io::Result<()> {
        let abs_offset = self.index as u64 * self.page_size as u64 + offset as u64;
        self.file.write_all_at(buf, abs_offset)
    }
}

impl PagesStorage for FilePagesStorage {
    type Page<'a> = FilePageHandle<'a>;

    fn get_page(&self, index: u32) -> io::Result<Self::Page<'_>> {
        Ok(FilePageHandle {
            file: &self.file,
            page_size: self.page_size,
            index,
        })
    }

    fn allocate_page(&self) -> io::Result<u32> {
        let mut count = self.page_count.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "lock poisoned")
        })?;
        let page = *count;
        let new_size = (page as u64 + 1) * self.page_size as u64;
        // This automatically sets the new range to zeros
        self.file.set_len(new_size)?;
        *count = page + 1;
        Ok(page)
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}
