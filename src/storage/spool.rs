use std::fs;
use std::io::Seek;
use std::io::Read;
use std::io::SeekFrom;

use crate::error::Error;
use crate::types::Result;

#[derive(Clone)]
pub struct Spool {
    filepath: String,
    chunk_offset: u64,
    piece_length: u64,
    total_length: u64,
}

impl Spool {
    pub fn new(filepath: &str, piece_length: u64, total_length: u64) -> Result<Self> {
        if total_length == 0 {
            return Err(Error::EmptyFile);
        }

        let chunk_offset = 0;

        Ok(Self {
            filepath: filepath.to_string(),
            piece_length,
            chunk_offset,
            total_length
        })
    }

    // Returns the next chunk of size piece_length
    pub fn read_next(&mut self) -> Result<Vec<u8>> {
        let piece_len = (self.total_length - self.chunk_offset).min(self.piece_length);

        // empty vector indicates the file read is complete
        if piece_len == 0 {
            return Ok(Vec::new());
        }

        // the file was verified as valid in new()
        // I EXPECT some DUMBASS to not delete the file while the yarn is forming
        let mut file = fs::File::open(&self.filepath)?;
        file.seek(SeekFrom::Start(self.chunk_offset as u64))?;

        let mut bytes: Vec<u8> = vec![0u8; piece_len as usize];
        file.take(piece_len as u64).read_exact(&mut bytes)?;

        self.chunk_offset = self.chunk_offset + bytes.len() as u64;
        Ok(bytes)
    }
}