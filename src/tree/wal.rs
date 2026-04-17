use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::os::unix::fs::FileExt;

const HEADER_LEN: usize = 6;

pub type Offset = u64;

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    CorruptRecord,
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

pub struct Wal {
    write: Mutex<WalInner>,
    read_file: File, // lock-free reads
}

struct WalInner {
    file: File,
    current_offset: u64,
}

impl Wal {

    /// Opens (or creates) a Write-Ahead Log (WAL) at the given path.
    ///
    /// # Behavior
    ///
    /// - If the file does not exist, it is created.
    /// - If the file exists, it is opened for both reading and writing.
    /// - The current write offset is initialized to the **end of the file**.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if:
    /// - the file cannot be opened or created
    /// - metadata (e.g., file size) cannot be retrieved
    pub fn open(path: &Path) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let size = file.metadata()?.len();

        // clone file handle for reads
        let read_file = file.try_clone()?;

        Ok(Wal {
            write: Mutex::new(WalInner {
                file,
                current_offset: size,
            }),
            read_file,
        })
    }

    /// Appends a serialized record to the end of the WAL.
    ///
    /// # Input
    ///
    /// - `record`: a fully encoded record
    ///
    /// # Returns
    ///
    /// - The byte offset at which the record was written
    ///
    /// # Behavior
    ///
    /// - The record is written at the current end of the file
    /// - The write is immediately followed by `sync_data()` to ensure durability
    /// - The internal offset is advanced by `record.len()`
    ///
    /// # Guarantees
    ///
    /// - **Write ordering is preserved**: records are appended sequentially
    /// - **Durability**: once this function returns `Ok`, the record's bytes are persisted to disk
    /// - The returned offset can be used later with [`read_at`]
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if:
    /// - the internal lock is poisoned (`CorruptRecord`)
    /// - the write or sync operation fails (`Io`)
    ///
    /// # Concurrency
    ///
    /// - This method is internally synchronized and safe to call from multiple threads
    /// - All writes are serialized via a single mutex
    ///
    /// # Important Invariants
    ///
    /// - The WAL is strictly append-only (no overwrites)
    /// - Offsets returned are stable and valid for future reads
    ///
    /// # Notes
    ///
    /// - This function provides **per-write fsync**, which is simple but may be slow.
    ///   More advanced implementations may batch writes (group commit).
    pub fn append(&self, record: &[u8]) -> Result<u64, WalError> {
        let mut inner = self.write.lock().map_err(|_| WalError::CorruptRecord)?;
    
        let offset = inner.current_offset;
    
        inner.file.seek(SeekFrom::Start(offset))?;
        inner.file.write_all(record)?;
        inner.file.sync_data()?;
    
        inner.current_offset += record.len() as u64;
    
        Ok(offset)
    }

    /// Reads a single record from the WAL at the given byte offset.
    /// Reads are **lock-free** and do not interfere with concurrent writes
    /// No shared file cursor is used (offset-based reads).
    /// 
    /// # Input
    ///
    /// - `offset`: byte position 
    ///
    /// # Returns
    ///
    /// - A buffer containing exactly one full record (including header)
    ///
    /// # Format
    ///
    /// The record is expected to follow:
    ///
    /// ```text
    /// [type: u8][record_length: u32][version: u8][body...]
    /// ```
    ///
    /// # Behavior
    ///
    /// 1. Reads the fixed-size header (`HEADER_LEN` bytes) at `offset`
    /// 2. Extracts `record_length` from the header
    /// 3. Allocates a buffer of size `record_length`
    /// 4. Reads the remaining bytes of the record
    ///
    /// # Errors
    ///
    /// Returns [`WalError`] if:
    ///
    /// - `Io`:
    ///   - read fails (e.g., invalid offset, OS error)
    ///   - unexpected EOF occurs during read
    ///
    /// - `CorruptRecord`:
    ///   - header cannot be parsed
    ///   - `record_length < HEADER_LEN`
    ///   - record is structurally invalid
    ///
    /// # Concurrency
    ///
    /// - This method is safe to call concurrently from multiple threads
    /// - Does not acquire the WAL write lock
    /// - May observe partially written records during concurrent writes
    ///
    /// # Partial Write Handling
    ///
    /// If a record is being written concurrently:
    ///
    /// - The header may be readable but the full record may not yet exist
    /// - In this case, the function returns an corruption error
    pub fn read_at(&self, offset: Offset) -> Result<Vec<u8>, WalError> {
        let file = &self.read_file;

        // read header
        let mut header = [0u8; HEADER_LEN];
        Wal::read_exact_at(file, &mut header, offset)?;

        let length_bytes: [u8; 4] = header[1..5]
            .try_into()
            .map_err(|_| WalError::CorruptRecord)?;

        let record_length = u32::from_le_bytes(length_bytes) as usize;

        if record_length < HEADER_LEN {
            return Err(WalError::CorruptRecord);
        }

        let mut buf = vec![0u8; record_length];
        buf[..HEADER_LEN].copy_from_slice(&header); // copy header into buf
        Wal::read_exact_at(file, &mut buf[HEADER_LEN..], offset + HEADER_LEN as u64)?; // read rest of record

        Ok(buf)
    }

    fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<(), WalError> {
        while !buf.is_empty() {
            let n = file.read_at(buf, offset)?; // requires Unix
    
            if n == 0 {
                return Err(WalError::CorruptRecord);
            }
    
            offset += n as u64;
            buf = &mut buf[n..];
        }
    
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    /// Helper: create a clean temporary WAL file path
    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("wal_test_{}_{}", name, std::process::id()));
        let _ = fs::remove_file(&path);
        path
    }

    /// Helper: build a minimal valid record
    fn make_record(payload: &[u8]) -> Vec<u8> {
        // [type=1][length][version=1][payload_len][payload]
        let payload_len = payload.len() as u32;

        let record_length =
            1 + 4 + 1 + // header
            4 +         // payload length
            payload.len();

        let mut buf = Vec::with_capacity(record_length);

        buf.push(1u8); // TYPE_NODE (assumed)
        buf.extend(&(record_length as u32).to_le_bytes());
        buf.push(1u8); // VERSION

        buf.extend(&payload_len.to_le_bytes());
        buf.extend(payload);

        buf
    }

    /// ------------------------------------------------------------
    /// TEST 1: Append → Read roundtrip
    ///
    /// Verifies:
    /// - append returns correct offset
    /// - read_at returns identical bytes
    /// ------------------------------------------------------------
    #[test]
    fn test_append_and_read_roundtrip() {
        let path = temp_path("roundtrip");
        let wal = Wal::open(&path).unwrap();

        let record = make_record(b"hello");

        let offset = wal.append(&record).unwrap();
        let read = wal.read_at(offset).unwrap();

        assert_eq!(record, read);
    }

    /// ------------------------------------------------------------
    /// TEST 2: Multiple appends preserve ordering
    ///
    /// Verifies:
    /// - offsets increase monotonically
    /// - each offset maps to correct record
    /// ------------------------------------------------------------
    #[test]
    fn test_multiple_appends() {
        let path = temp_path("multi");
        let wal = Wal::open(&path).unwrap();

        let r1 = make_record(b"a");
        let r2 = make_record(b"bb");
        let r3 = make_record(b"ccc");

        let o1 = wal.append(&r1).unwrap();
        let o2 = wal.append(&r2).unwrap();
        let o3 = wal.append(&r3).unwrap();

        assert!(o1 < o2 && o2 < o3);

        assert_eq!(wal.read_at(o1).unwrap(), r1);
        assert_eq!(wal.read_at(o2).unwrap(), r2);
        assert_eq!(wal.read_at(o3).unwrap(), r3);
    }

    /// ------------------------------------------------------------
    /// TEST 3: Offset correctness
    ///
    /// Verifies:
    /// - second offset = first offset + first record length
    /// ------------------------------------------------------------
    #[test]
    fn test_offset_progression() {
        let path = temp_path("offset");
        let wal = Wal::open(&path).unwrap();

        let r1 = make_record(b"abc");
        let r2 = make_record(b"defg");

        let o1 = wal.append(&r1).unwrap();
        let o2 = wal.append(&r2).unwrap();

        assert_eq!(o2, o1 + r1.len() as u64);
    }

    /// ------------------------------------------------------------
    /// TEST 4: Corrupt record (invalid length)
    ///
    /// Verifies:
    /// - read_at rejects records with invalid record_length
    /// ------------------------------------------------------------
    #[test]
    fn test_corrupt_length() {
        let path = temp_path("corrupt_length");
        let wal = Wal::open(&path).unwrap();

        let mut bad = vec![1u8];
        bad.extend(&(2u32.to_le_bytes())); // invalid length (< HEADER_LEN)
        bad.push(1u8);

        let offset = wal.append(&bad).unwrap();

        let result = wal.read_at(offset);
        assert!(result.is_err());
    }

    /// ------------------------------------------------------------
    /// TEST 5: Partial record (simulated crash)
    ///
    /// Verifies:
    /// - truncated records are safely rejected
    /// ------------------------------------------------------------
    #[test]
    fn test_partial_record() {
        let path = temp_path("partial");
        let wal = Wal::open(&path).unwrap();

        let mut record = make_record(b"abcdef");
        record.truncate(record.len() - 2); // simulate crash

        let offset = wal.append(&record).unwrap();

        let result = wal.read_at(offset);
        assert!(result.is_err());
    }

    /// ------------------------------------------------------------
    /// TEST 6: Read at invalid offset
    ///
    /// Verifies:
    /// - reading beyond file bounds returns error
    /// ------------------------------------------------------------
    #[test]
    fn test_read_invalid_offset() {
        let path = temp_path("invalid_offset");
        let wal = Wal::open(&path).unwrap();

        let result = wal.read_at(999999);
        assert!(result.is_err());
    }

    /// ------------------------------------------------------------
    /// TEST 7: Empty WAL behavior
    ///
    /// Verifies:
    /// - reading from empty WAL fails safely
    /// ------------------------------------------------------------
    #[test]
    fn test_empty_wal_read() {
        let path = temp_path("empty");
        let wal = Wal::open(&path).unwrap();

        let result = wal.read_at(0);
        assert!(result.is_err());
    }
}

