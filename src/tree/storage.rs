use zerocopy::{KnownLayout, Immutable, TryFromBytes, IntoBytes};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt as UnixFileExt;
use std::path::Path;
use std::sync::Mutex;

use fs4::fs_std::FileExt as LockExt;


pub trait PageHandle {
    fn sync(&self) -> io::Result<()>;

    fn read(&self, offset: u32, buf: &mut [u8]) -> io::Result<()>;
    fn write(&self, offset: u32, buf: &[u8]) -> io::Result<()>;

    fn read_type<T: TryFromBytes + KnownLayout + Immutable + 'static>(&self, offset: u32) -> io::Result<T> {
        let mut buf = vec![0u8; size_of::<T>()];
        self.read(offset, &mut buf)?;
        let (val, _) = T::try_read_from_prefix(&buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid byte pattern"))?;
        Ok(val)
    }

    fn write_type<T: IntoBytes + Immutable>(&self, offset: u32, val: &T) -> io::Result<()> {
        self.write(offset, val.as_bytes())
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
    fn sync(&self) -> io::Result<()> {
        self.file.sync_data() // TODO: use sync_all() to flush data + metadata
    }

    fn read(&self, offset: u32, buf: &mut [u8]) -> io::Result<()> {
        let abs_offset = self.index as u64 * self.page_size as u64 + offset as u64;
        self.file.read_exact_at(buf, abs_offset)
        // TODO: add validation to ensure reading within page boundaries
    }

    fn write(&self, offset: u32, buf: &[u8]) -> io::Result<()> {
        let abs_offset = self.index as u64 * self.page_size as u64 + offset as u64;
        self.file.write_all_at(buf, abs_offset)
        // TODO: add validation to ensure writing within page boundaries
    }
}

impl PagesStorage for FilePagesStorage {
    type Page<'a> = FilePageHandle<'a>;

    fn get_page(&self, index: u32) -> io::Result<Self::Page<'_>> {
        // TODO: validate that index <= page count
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
        // TODO: use sync_all() to flush data + metadata ow length may be out of sync
        self.file.sync_data() 
    }
}
