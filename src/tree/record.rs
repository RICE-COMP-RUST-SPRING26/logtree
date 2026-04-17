use std::convert::TryInto;

pub type NodeID = u64;
pub type BranchID = u64;


const VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug)]
pub enum RecordType {
    Node = 1,
    BranchCreate = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = RecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RecordType::Node),
            2 => Ok(RecordType::BranchCreate),
            other => Err(RecordError::InvalidType(other)),
        }
    }
}

#[derive(Debug)]
pub enum Record {
    Node(NodeRecord),
    BranchCreate(BranchCreateRecord),
}

#[derive(Debug)]
pub struct NodeRecord {
    pub node_id: NodeID,
    pub branch_id: BranchID,
    pub prev_node_id: NodeID,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct BranchCreateRecord {
    pub branch_id: BranchID,
    pub parent_node_id: NodeID,
}

#[derive(Debug)]
pub enum RecordError {
    UnexpectedEOF,
    InvalidType(u8),
    InvalidVersion(u8),
    LengthMismatch,
}

impl Record {

    /// Serializes a [`Record`] into its on-disk binary representation. 
    /// Returned buffer length equals `record_length`.
    ///
    /// ## Node Record Layout
    ///
    /// ```text
    /// [type: u8][length: u32][version: u8]
    /// [node_id: u64]
    /// [branch_id: u64]
    /// [prev_node_id: u64]
    /// [payload_length: u32]
    /// [payload bytes]
    /// ```
    ///
    /// ## BranchCreate Record Layout
    ///
    /// ```text
    /// [type: u8][length: u32][version: u8]
    /// [branch_id: u64]
    /// [parent_node_id: u64]
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Record::Node(n) => Self::encode_node(n),
            Record::BranchCreate(b) => Self::encode_branch(b),
        }
    }

    /// Deserializes a binary buffer into a [`Record`].
    ///
    /// # Input
    ///
    /// A byte slice representing a single WAL record. 
    ///
    /// # Behavior
    ///
    /// 1. Parses the record header:
    ///    - `type` → determines record variant
    ///    - `record_length` → validates full record is present
    ///    - `version` → checked for compatibility
    ///
    /// 2. Validates:
    ///    - buffer contains at least `record_length` bytes
    ///    - version matches supported format
    ///    - type is recognized
    ///
    /// 3. Dispatches to variant-specific decoding logic
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] in the following cases:
    ///
    /// - `UnexpectedEOF`:
    ///   - buffer is smaller than header
    ///   - buffer is smaller than declared `record_length`
    ///   - payload extends beyond buffer
    ///
    /// - `InvalidType(u8)`:
    ///   - `type` byte does not map to a known [`RecordType`]
    ///
    /// - `InvalidVersion(u8)`:
    ///   - record version is unsupported
    ///
    /// - `LengthMismatch`:
    ///   - internal parsing inconsistency (e.g., fixed-size conversion failure)
    pub fn decode(buf: &[u8]) -> Result<Self, RecordError> {
        let mut reader = ByteReader::new(buf);

        // HEADER
        let record_type = RecordType::try_from(reader.read_u8()?)?;
        let record_length: usize = reader.read_u32()? as usize;
        let version = reader.read_u8()?;

        if version != VERSION {
            return Err(RecordError::InvalidVersion(version));
        }

        if buf.len() < record_length {
            return Err(RecordError::UnexpectedEOF);
        }

        match record_type {
            RecordType::Node => Self::decode_node(reader),
            RecordType::BranchCreate => Self::decode_branch(reader),
        }
    }

    fn encode_node(node: &NodeRecord) -> Vec<u8> {
        let payload_len = node.payload.len() as u32;

        let record_length =
            1 + 4 + 1 + // header
            8 + 8 + 8 + // ids
            4 +         // payload length
            payload_len as usize;

        let mut buf = Vec::with_capacity(record_length);

        buf.push(RecordType::Node as u8);
        buf.extend(&(record_length as u32).to_le_bytes());
        buf.push(VERSION);

        buf.extend(&node.node_id.to_le_bytes());
        buf.extend(&node.branch_id.to_le_bytes());
        buf.extend(&node.prev_node_id.to_le_bytes());

        buf.extend(&payload_len.to_le_bytes());
        buf.extend(&node.payload);

        buf
    }

    fn encode_branch(branch: &BranchCreateRecord) -> Vec<u8> {
        let record_length =
            1 + 4 + 1 + // header
            8 + 8;

        let mut buf = Vec::with_capacity(record_length);

        buf.push(RecordType::BranchCreate as u8);
        buf.extend(&(record_length as u32).to_le_bytes());
        buf.push(VERSION);

        buf.extend(&branch.branch_id.to_le_bytes());
        buf.extend(&branch.parent_node_id.to_le_bytes());

        buf
    }

    fn decode_node(mut reader: ByteReader) -> Result<Record, RecordError> {
        let node_id = reader.read_u64()?;
        let branch_id = reader.read_u64()?;
        let prev_node_id = reader.read_u64()?;

        let payload_len = reader.read_u32()? as usize;
        let payload = reader.read_bytes(payload_len)?.to_vec();

        Ok(Record::Node(NodeRecord {
            node_id,
            branch_id,
            prev_node_id,
            payload,
        }))
    }

    fn decode_branch(mut reader: ByteReader) -> Result<Record, RecordError> {
        let branch_id = reader.read_u64()?;
        let parent_node_id = reader.read_u64()?;

        Ok(Record::BranchCreate(BranchCreateRecord {
            branch_id,
            parent_node_id,
        }))
    }
}

struct ByteReader<'a> {
    buf: &'a [u8],
    cursor: usize,
}

impl<'a> ByteReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, cursor: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, RecordError> {
        if self.cursor >= self.buf.len() {
            return Err(RecordError::UnexpectedEOF);
        }

        let val = self.buf[self.cursor];
        self.cursor += 1;
        Ok(val)
    }

    fn read_u32(&mut self) -> Result<u32, RecordError> {
        if self.cursor > self.buf.len() || self.buf.len() - self.cursor < 4 {
            return Err(RecordError::UnexpectedEOF);
        }

        let bytes: [u8; 4] = self.buf[self.cursor..self.cursor + 4]
            .try_into()
            .map_err(|_| RecordError::LengthMismatch)?;

        self.cursor += 4;

        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, RecordError> {
        if self.cursor > self.buf.len() || self.buf.len() - self.cursor < 8 {
            return Err(RecordError::UnexpectedEOF);
        }

        let bytes: [u8; 8] = self.buf[self.cursor..self.cursor + 8]
            .try_into()
            .map_err(|_| RecordError::LengthMismatch)?;

        self.cursor += 8;

        Ok(u64::from_le_bytes(bytes))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], RecordError> {
        if self.cursor > self.buf.len() || self.buf.len() - self.cursor < len {
            return Err(RecordError::UnexpectedEOF);
        }

        let slice = &self.buf[self.cursor..self.cursor + len];
        self.cursor += len;

        Ok(slice)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// ------------------------------------------------------------
    /// TEST 1: Node encode → decode roundtrip
    ///
    /// Verifies:
    /// - Encoding a NodeRecord produces valid bytes
    /// - Decoding those bytes reconstructs the exact same data
    /// ------------------------------------------------------------
    #[test]
    fn test_node_encode_decode_roundtrip() {
        let record = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 2,
            prev_node_id: 0,
            payload: b"hello".to_vec(),
        });

        let encoded = record.encode();
        let decoded = Record::decode(&encoded).expect("decode should succeed");

        match decoded {
            Record::Node(n) => {
                assert_eq!(n.node_id, 1);
                assert_eq!(n.branch_id, 2);
                assert_eq!(n.prev_node_id, 0);
                assert_eq!(n.payload, b"hello");
            }
            _ => panic!("expected Node record"),
        }
    }

    /// ------------------------------------------------------------
    /// TEST 2: BranchCreate encode → decode roundtrip
    ///
    /// Verifies:
    /// - BranchCreateRecord is encoded and decoded correctly
    /// ------------------------------------------------------------
    #[test]
    fn test_branch_encode_decode_roundtrip() {
        let record = Record::BranchCreate(BranchCreateRecord {
            branch_id: 10,
            parent_node_id: 5,
        });

        let encoded = record.encode();
        let decoded = Record::decode(&encoded).expect("decode should succeed");

        match decoded {
            Record::BranchCreate(b) => {
                assert_eq!(b.branch_id, 10);
                assert_eq!(b.parent_node_id, 5);
            }
            _ => panic!("expected BranchCreate record"),
        }
    }

    /// ------------------------------------------------------------
    /// TEST 3: Record length correctness
    ///
    /// Verifies:
    /// - The encoded length matches the record_length field in header
    /// ------------------------------------------------------------
    #[test]
    fn test_record_length_matches_buffer() {
        let record = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: b"abc".to_vec(),
        });

        let encoded = record.encode();

        let length_bytes: [u8; 4] = encoded[1..5].try_into().unwrap();
        let record_length = u32::from_le_bytes(length_bytes) as usize;

        assert_eq!(record_length, encoded.len());
    }

    /// ------------------------------------------------------------
    /// TEST 4: Invalid version detection
    ///
    /// Verifies:
    /// - Decoding fails if version byte is incorrect
    /// ------------------------------------------------------------
    #[test]
    fn test_invalid_version() {
        let record = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: vec![1, 2, 3],
        });

        let mut encoded = record.encode();

        // corrupt version byte (index 5)
        encoded[5] = 99;

        let result = Record::decode(&encoded);
        assert!(matches!(result, Err(RecordError::InvalidVersion(99))));
    }

    /// ------------------------------------------------------------
    /// TEST 5: Invalid type detection
    ///
    /// Verifies:
    /// - Unknown record type is rejected
    /// ------------------------------------------------------------
    #[test]
    fn test_invalid_type() {
        let record = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: vec![1],
        });

        let mut encoded = record.encode();

        // corrupt type byte
        encoded[0] = 99;

        let result = Record::decode(&encoded);
        assert!(matches!(result, Err(RecordError::InvalidType(99))));
    }

    /// ------------------------------------------------------------
    /// TEST 6: Truncated buffer detection
    ///
    /// Verifies:
    /// - Decode fails if buffer is shorter than declared record_length
    /// ------------------------------------------------------------
    #[test]
    fn test_truncated_buffer() {
        let record = Record::Node(NodeRecord {
            node_id: 1,
            branch_id: 1,
            prev_node_id: 0,
            payload: b"abcdef".to_vec(),
        });

        let mut encoded = record.encode();

        // truncate buffer
        encoded.truncate(encoded.len() - 2);

        let result = Record::decode(&encoded);
        assert!(result.is_err());
    }

    /// ------------------------------------------------------------
    /// TEST 7: Payload integrity
    ///
    /// Verifies:
    /// - Arbitrary payload bytes are preserved exactly
    /// ------------------------------------------------------------
    #[test]
    fn test_payload_integrity() {
        let payload = vec![0, 255, 10, 42, 99];

        let record = Record::Node(NodeRecord {
            node_id: 123,
            branch_id: 456,
            prev_node_id: 789,
            payload: payload.clone(),
        });

        let encoded = record.encode();
        let decoded = Record::decode(&encoded).unwrap();

        match decoded {
            Record::Node(n) => {
                assert_eq!(n.payload, payload);
            }
            _ => panic!("expected Node record"),
        }
    }
}
