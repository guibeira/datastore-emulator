use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

// LevelDB log format constants
const BLOCK_SIZE: usize = 32768;
const HEADER_SIZE: usize = 7;

// Physical record types
const FULL_TYPE: u8 = 1;
const FIRST_TYPE: u8 = 2;
const MIDDLE_TYPE: u8 = 3;
const LAST_TYPE: u8 = 4;

#[derive(Debug)]
pub enum CorruptionError {
    Io(io::Error),
    ChecksumMismatch,
    InvalidFragmentSequence(&'static str),
    TruncatedRecord(&'static str),
    UnknownRecordType(u8),
}

impl std::fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CorruptionError::Io(e) => write!(f, "IO error: {}", e),
            CorruptionError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            CorruptionError::InvalidFragmentSequence(msg) => {
                write!(f, "Invalid fragment sequence: {}", msg)
            }
            CorruptionError::TruncatedRecord(msg) => write!(f, "Truncated record at EOF: {}", msg),
            CorruptionError::UnknownRecordType(t) => write!(f, "Unknown record type: {}", t),
        }
    }
}

impl std::error::Error for CorruptionError {}

impl From<io::Error> for CorruptionError {
    fn from(err: io::Error) -> Self {
        CorruptionError::Io(err)
    }
}

/// Block-aware LevelDB log reader.
///
/// LevelDB log format: file is split into 32 KiB blocks. A physical record
/// header is 7 bytes (4 CRC + 2 length + 1 type). When fewer than 7 bytes
/// remain in a block, those bytes form a zero-filled trailer that must be
/// skipped — no record can start there. A previous version of this reader
/// concatenated all blocks into one flat buffer and parsed records without
/// tracking block boundaries, which caused trailers to be misread as
/// headers (UnknownRecordType / TruncatedRecord errors mid-file).
#[derive(Debug)]
pub struct LogReader {
    file: File,
    block: Vec<u8>,
    block_offset: usize,
    eof: bool,
}

impl LogReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> io::Result<Self> {
        let file = File::open(filename)?;
        Ok(LogReader {
            file,
            block: Vec::new(),
            block_offset: 0,
            eof: false,
        })
    }

    /// Load the next 32 KiB block into `self.block`. Returns true if any
    /// bytes were loaded.
    fn load_next_block(&mut self) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        let mut buf = vec![0u8; BLOCK_SIZE];
        let mut total = 0;
        while total < BLOCK_SIZE {
            let n = self.file.read(&mut buf[total..])?;
            if n == 0 {
                break;
            }
            total += n;
        }
        if total == 0 {
            self.eof = true;
            self.block.clear();
            self.block_offset = 0;
            return Ok(false);
        }
        if total < BLOCK_SIZE {
            self.eof = true;
            buf.truncate(total);
        }
        self.block = buf;
        self.block_offset = 0;
        Ok(true)
    }

    /// Read the next physical record, skipping block trailers. Returns
    /// `Ok(None)` only at clean EOF.
    fn read_physical_record(&mut self) -> Result<Option<(u8, Vec<u8>)>, CorruptionError> {
        loop {
            if self.block.is_empty() || self.block.len() - self.block_offset < HEADER_SIZE {
                if !self.block.is_empty() && self.block_offset < self.block.len() {
                    let trailer = &self.block[self.block_offset..];
                    if trailer.iter().any(|&b| b != 0) {
                        return Err(CorruptionError::TruncatedRecord(
                            "Non-zero bytes found in block trailer",
                        ));
                    }
                }
                if !self.load_next_block()? {
                    return Ok(None);
                }
                continue;
            }

            let start = self.block_offset;
            let mut cursor = &self.block[start..start + HEADER_SIZE];
            let checksum = cursor.read_u32::<LittleEndian>()?;
            let length = cursor.read_u16::<LittleEndian>()? as usize;
            let record_type = cursor.read_u8()?;

            // A zero record type indicates the start of a block trailer
            // (zero-padded leftover bytes). Skip to the next block.
            if record_type == 0 && length == 0 && checksum == 0 {
                self.block_offset = self.block.len();
                continue;
            }

            let data_start = start + HEADER_SIZE;
            let data_end = data_start + length;
            if data_end > self.block.len() {
                return Err(CorruptionError::TruncatedRecord(
                    "Record length exceeds block boundary",
                ));
            }

            let data = self.block[data_start..data_end].to_vec();
            self.block_offset = data_end;

            // CRC verification intentionally skipped (mirrors prior behavior;
            // many exports we ingest do not preserve a verifiable CRC).
            let _ = checksum;

            return Ok(Some((record_type, data)));
        }
    }

    fn read_logical_record(&mut self) -> Option<Result<Vec<u8>, CorruptionError>> {
        let mut fragments: Vec<Vec<u8>> = Vec::new();
        loop {
            let physical_record = match self.read_physical_record() {
                Ok(Some(record)) => record,
                Ok(None) => {
                    if !fragments.is_empty() {
                        return Some(Err(CorruptionError::InvalidFragmentSequence(
                            "Log file ended in the middle of a fragmented record",
                        )));
                    }
                    return None;
                }
                Err(e) => return Some(Err(e)),
            };

            let (record_type, data) = physical_record;
            match record_type {
                FULL_TYPE => {
                    if !fragments.is_empty() {
                        return Some(Err(CorruptionError::InvalidFragmentSequence(
                            "FULL record found in the middle of a fragment",
                        )));
                    }
                    return Some(Ok(data));
                }
                FIRST_TYPE => {
                    if !fragments.is_empty() {
                        return Some(Err(CorruptionError::InvalidFragmentSequence(
                            "FIRST record found in the middle of a fragment",
                        )));
                    }
                    fragments.push(data);
                }
                MIDDLE_TYPE => {
                    if fragments.is_empty() {
                        return Some(Err(CorruptionError::InvalidFragmentSequence(
                            "MIDDLE record found without a FIRST record",
                        )));
                    }
                    fragments.push(data);
                }
                LAST_TYPE => {
                    if fragments.is_empty() {
                        return Some(Err(CorruptionError::InvalidFragmentSequence(
                            "LAST record found without a FIRST record",
                        )));
                    }
                    fragments.push(data);
                    return Some(Ok(fragments.concat()));
                }
                _ => return Some(Err(CorruptionError::UnknownRecordType(record_type))),
            }
        }
    }
}

impl Iterator for LogReader {
    type Item = Result<Vec<u8>, CorruptionError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_logical_record()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{LittleEndian, WriteBytesExt};
    use std::io::Write;
    use std::path::PathBuf;

    fn temp_log_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "datastore-emulator-leveldb-test-{}.log",
            uuid::Uuid::new_v4()
        ))
    }

    fn write_full_record(file: &mut File, data: &[u8]) {
        file.write_u32::<LittleEndian>(0).unwrap();
        file.write_u16::<LittleEndian>(data.len() as u16).unwrap();
        file.write_u8(FULL_TYPE).unwrap();
        file.write_all(data).unwrap();
    }

    #[test]
    fn reads_record_after_block_trailer() {
        let path = temp_log_path();
        let mut file = File::create(&path).unwrap();
        let first = vec![b'a'; BLOCK_SIZE - HEADER_SIZE - 3];
        let second = b"after-trailer";

        write_full_record(&mut file, &first);
        file.write_all(&[0; 3]).unwrap();
        write_full_record(&mut file, second);
        drop(file);

        let mut reader = LogReader::new(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), first);
        assert_eq!(reader.next().unwrap().unwrap(), second);
        assert!(reader.next().is_none());

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_non_zero_block_trailer() {
        let path = temp_log_path();
        let mut file = File::create(&path).unwrap();
        let first = vec![b'a'; BLOCK_SIZE - HEADER_SIZE - 3];

        write_full_record(&mut file, &first);
        file.write_all(&[1, 2, 3]).unwrap();
        drop(file);

        let mut reader = LogReader::new(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), first);
        let error = reader.next().unwrap().unwrap_err();
        assert!(matches!(error, CorruptionError::TruncatedRecord(_)));
        assert!(error.to_string().contains("Non-zero bytes"));

        std::fs::remove_file(path).unwrap();
    }
}
