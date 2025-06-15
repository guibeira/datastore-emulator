use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path; // To read integers from the buffer (analogous to struct.unpack)

// LevelDB log format constants
const BLOCK_SIZE: usize = 32768;
const HEADER_SIZE: usize = 7;

// Physical record types
const FULL_TYPE: u8 = 1;
const FIRST_TYPE: u8 = 2;
const MIDDLE_TYPE: u8 = 3;
const LAST_TYPE: u8 = 4;

/// Error enum to represent failures in reading or parsing the log.
/// Analogous to Python's CorruptionError exception.
#[derive(Debug)]
pub enum CorruptionError {
    Io(io::Error),
    ChecksumMismatch,
    InvalidFragmentSequence(&'static str),
    TruncatedRecord(&'static str),
    UnknownRecordType(u8),
}

// Implementations to make our error a standard error type in Rust.
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

// Implementations to make our error a standard error type in Rust.
impl std::error::Error for CorruptionError {}

// Allows easy conversion of IO errors to our error type.
impl From<io::Error> for CorruptionError {
    fn from(err: io::Error) -> Self {
        CorruptionError::Io(err)
    }
}

/// A reader for LevelDB log files.
/// This struct holds the state, analogous to Python class attributes.
#[derive(Debug)] // Added to resolve E0277
pub struct LogReader {
    file: File,
    buffer: Vec<u8>,
    eof: bool,
}

impl LogReader {
    /// Creates a new LogReader from a file path.
    /// Analogous to `__init__`. Returns a Result because opening the file can fail.
    pub fn new<P: AsRef<Path>>(filename: P) -> io::Result<Self> {
        let file = File::open(filename)?;
        Ok(LogReader {
            file,
            buffer: Vec::with_capacity(BLOCK_SIZE * 2), // Pre-allocate for efficiency
            eof: false,
        })
    }

    /// Reads the next block from the file into the buffer.
    /// Analogous to `_read_block`.
    fn read_block(&mut self) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        let mut temp_buf = [0u8; BLOCK_SIZE];
        let bytes_read = self.file.read(&mut temp_buf)?;

        if bytes_read == 0 {
            self.eof = true;
            return Ok(false);
        }

        self.buffer.extend_from_slice(&temp_buf[..bytes_read]);
        Ok(true)
    }

    /// Reads the next physical record from the buffer.
    /// Analogous to `_read_physical_record`.
    /// Returns `Ok(None)` if more data is needed.
    fn read_physical_record(&mut self) -> Result<Option<(u8, Vec<u8>)>, CorruptionError> {
        // If the buffer is too small for a header, try to read more data.
        if self.buffer.len() < HEADER_SIZE {
            if !self.eof {
                self.read_block()?;
            }
            // If there's still not enough data, check if it's a valid end of file.
            if self.buffer.len() < HEADER_SIZE {
                if self.eof && !self.buffer.is_empty() {
                    return Err(CorruptionError::TruncatedRecord(
                        "Truncated record header at EOF",
                    ));
                }
                return Ok(None); // Not an error, just no more complete records.
            }
        }

        // Use a cursor to read from the buffer without needing to copy.
        let mut cursor = &self.buffer[..HEADER_SIZE];
        let checksum = cursor.read_u32::<LittleEndian>()?;
        let length = cursor.read_u16::<LittleEndian>()? as usize;
        let record_type = cursor.read_i8()?;

        // If the complete record (header + data) is not in the buffer, try to read more.
        if self.buffer.len() < HEADER_SIZE + length {
            if !self.eof {
                self.read_block()?;
            }
            if self.buffer.len() < HEADER_SIZE + length {
                if self.eof {
                    return Err(CorruptionError::TruncatedRecord(
                        "Truncated record data at EOF",
                    ));
                }
                return Ok(None);
            }
        }

        // Extract the data and remove the physical record from the buffer.
        let data = self.buffer[HEADER_SIZE..HEADER_SIZE + length].to_vec();
        self.buffer.drain(..HEADER_SIZE + length);

        // Verify the checksum. LevelDB's CRC32C is "masked".
        // Although your Python code commented it out, the correct implementation is this:
        let masked_crc = {
            // Construct the bytes for the CRC more explicitly
            let mut bytes_for_crc = Vec::with_capacity(1 + data.len());
            bytes_for_crc.push(record_type as u8); // record_type is already i8, convert to u8
            bytes_for_crc.extend_from_slice(&data);
            let crc = crc32c::crc32c(&bytes_for_crc);
            crc.rotate_right(15).wrapping_add(0xa282ead8)
        };

        if checksum != masked_crc {
            // Uncomment the line below to enable checksum verification.
            // return Err(CorruptionError::ChecksumMismatch);
        }

        Ok(Some((record_type as u8, data))) // Converted record_type to u8 to match the function signature
    }

    /// Reads the next logical record (which may be fragmented).
    /// This function is called by `next()` and contains the main iterator logic.
    fn read_logical_record(&mut self) -> Option<Result<Vec<u8>, CorruptionError>> {
        // Analogous to `if self._eof and not self._buffer: raise StopIteration`
        if self.eof && self.buffer.is_empty() {
            return None;
        }

        let mut fragments: Vec<Vec<u8>> = Vec::new();
        loop {
            let physical_record = match self.read_physical_record() {
                Ok(Some(record)) => record,
                // `Ok(None)` means we need more data or have reached a clean end.
                Ok(None) => {
                    if self.eof && self.buffer.is_empty() {
                        if !fragments.is_empty() {
                            return Some(Err(CorruptionError::InvalidFragmentSequence(
                                "Log file ended in the middle of a fragmented record",
                            )));
                        }
                        return None; // End of iteration
                    }
                    // If not EOF, the loop continues to try to read more in `read_physical_record`.
                    // This is safe because `read_physical_record` calls `read_block`.
                    continue;
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

/// Implements the Iterator trait to make the struct easy to use in `for` loops.
/// Analogous to `__iter__` and `__next__`.
impl Iterator for LogReader {
    // The iterator will produce either a record (`Vec<u8>`) or an error.
    type Item = Result<Vec<u8>, CorruptionError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_logical_record()
    }
}
