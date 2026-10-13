use std::collections::HashSet;
use std::net::SocketAddr;

use crate::yarn::Yarn;
use crate::error::Error;
use crate::types::Result;

const BLOCK_LENGTH: usize = 16_384;
const ENDGAME_THRESHOLD: u32 = 6;

#[derive(Clone, PartialEq)]
pub enum PieceStatus {
    Complete,
    InFlight { 
        blocks: Vec<Option<Vec<u8>>>, 
        requested_from: Vec<Vec<SocketAddr>> 
    },
    Missing,
}

#[derive(Clone)]
pub struct Bitfield {
    pub bits: Vec<u8>,
    pub piece_count: u32,
}

impl Bitfield {
    pub fn new(piece_count: usize) -> Self {
        let bytes = piece_count.div_ceil(8);
        Self {
            bits: vec![0u8; bytes],
            piece_count: piece_count as u32, 
        }
    }

    pub fn from_bytes(bits: Vec<u8>, piece_count: usize) -> Result<Self> {
        let expected_bytes = piece_count.div_ceil(8);
        if bits.len() > expected_bytes {
            return Err(Error::InvalidBitfieldLength);
        }

        Ok(Self {
            bits,
            piece_count: piece_count as u32
        })
    }

    pub fn has(&self, index: u32) -> bool {
        if index >= self.piece_count {
            return false;
        }

        let byte_index = (index / 8) as usize;
        let bit_offset = 7 - index % 8;
        let mask = 1u8 << bit_offset;

        self.bits[byte_index] & mask != 0
    }

    pub fn set(&mut self, index: u32) {
        if index >= self.piece_count {
            return;
        }

        let byte = (index / 8) as usize;
        let bit_offset = 7 - index % 8;
        let mask = 1u8 << bit_offset;

        self.bits[byte] = self.bits[byte] | mask;
    }

    /// set all pieces in the bitfield to `has`
    pub fn full(&mut self) {
        // fill up the bitfield with 1s
        let byte_count = self.piece_count.div_ceil(8);
        self.bits = vec![0xFFu8; byte_count as usize];

        // truncate valid fields by the trailing amount
        let used = self.piece_count % 8;
        if used != 0 {
            let invalid = 8 - used;        
            self.bits[(byte_count - 1) as usize] = 0xFFu8 << invalid;
        }
    }

    pub fn is_complete(&self) -> bool {
        (0..self.piece_count).all(|i| self.has(i))
    }

    pub fn missing(&self) -> u32 {
        self.piece_count - self.count_ones()
    }

    pub fn count_ones(&self) -> u32 {
        self.bits.iter().map(|b| b.count_ones()).sum()
    }
}

#[derive(Clone)]
pub struct PieceManager {
    pub piece_count: u32,
    pub piece_length: u32,
    pub total_length: u64,
    pub bitfield: Bitfield,
    pub failed_peers: Vec<HashSet<SocketAddr>>,
    pub piece_state: Vec<PieceStatus>
}

impl PieceManager {
    pub fn new(yarn: &Yarn) -> Result<Self> {
        let piece_count = u32::try_from(yarn.info.pieces.len()).map_err(|_| Error::UnexpectedFormat)?;
        let piece_length = yarn.info.piece_length;
        let total_length = yarn.info.total_length;
        let bitfield = Bitfield::new(piece_count as usize);
        let failed_peers = Vec::new();
        let piece_state= vec![PieceStatus::Missing; piece_count as usize];
        
        Ok(Self {
            piece_count,
            piece_length,
            total_length,
            bitfield,
            failed_peers,
            piece_state,
        })
    }

    pub fn resume(yarn: &Yarn, bitfield: Bitfield) -> Result<Self> {
        if bitfield.piece_count != yarn.info.pieces.len() as u32 {
            return Err(Error::UnexpectedFormat);
        }

        let mut piece_manager = Self::new(yarn)?;
        piece_manager.bitfield = bitfield;

        let piece_count = piece_manager.piece_count;
        for i in 0..piece_count {
            if piece_manager.bitfield.has(i) { 
                piece_manager.piece_state[i as usize] = PieceStatus::Complete;
            }
        }

        Ok(piece_manager)
    }

    // --------------------
    // -- UNIT SELECTION --
    // --------------------

    /// piece selection from avaialable pieces
    pub fn choose_piece(&self, pod_bitfield: &Bitfield) -> Option<u32> {
        for (idx, status) in self.piece_state.iter().enumerate() {
            if !pod_bitfield.has(idx as u32) {
                continue;
            }

            match status {
                PieceStatus::Complete => continue,
                PieceStatus::Missing => return Some(idx as u32),
                
                // if the piece is in flight and has a block that is NOT being serviced by any peer, request it
                PieceStatus::InFlight { blocks, requested_from } => {
                    let has_work = blocks.iter().enumerate()
                        .any(|(b_idx, b)| b.is_none() && requested_from[b_idx].is_empty());
                    
                    if has_work {
                        return Some(idx as u32);
                    }
                }
            }
        }

        None
    }

    /// block selection from missing or inflight pieces
    pub fn choose_block(&self, piece: u32) -> Option<u32> {
        match &self.piece_state[piece as usize] {
            PieceStatus::Missing => Some(0), // get the first block
            PieceStatus::InFlight { blocks, requested_from } => { 
                blocks.iter().enumerate()
                    .find(|(i, b)| b.is_none() && requested_from[*i].is_empty())
                    .map(|(i, _)| i as u32)
            },
            PieceStatus::Complete => None,
        }
    }

    // -------------------------
    // -- Piece State Updates --
    // -------------------------

    /// set the piece as requested so duplicate requests dont bundle up
    pub fn mark_requested(&mut self, piece: u32, block: u32, from: SocketAddr) {
        if let PieceStatus::InFlight { requested_from, .. } = &mut self.piece_state[piece as usize] {
            requested_from[block as usize].push(from);
            return;
        }

        if matches!(self.piece_state[piece as usize], PieceStatus::Missing) {
            let block_count = self.block_count_for(piece);
            debug_assert!((block as usize) < block_count, "Block index is out of range");

            let mut requested_from = vec![Vec::new(); block_count];
            requested_from[block as usize].push(from);

            self.piece_state[piece as usize] = PieceStatus::InFlight { 
                blocks: vec![None; block_count], 
                requested_from 
            };
            return;
        }
    }

    /// verify if block data received is valid or not and return the 
    /// piece if all blocks are complete along with a list of peers to 
    /// cancel requests from
    pub fn record_block(
        &mut self, 
        piece: u32,
        block: u32, 
        data: Vec<u8>, 
        from: SocketAddr
    ) -> Result<(Option<Vec<u8>>, Vec<SocketAddr>)> {
        if self.bitfield.has(piece) {
            return Err(Error::PieceAlreadyComplete);
        }

        let piece_start = piece as u64 * self.piece_length as u64;
        let piece_len = (self.total_length - piece_start).min(self.piece_length as u64) as usize;
        let PieceStatus::InFlight { blocks, requested_from } = &mut self.piece_state[piece as usize] else {
            return Err(Error::InvalidPieceOperation);
        };

        if blocks[block as usize].is_some() {
            return Err(Error::BlockAlreadyReceived);
        }

        blocks[block as usize] = Some(data);

        // get all peers that did not send the block in time
        let peers = std::mem::take(&mut requested_from[block as usize]);
        let cancel: Vec<SocketAddr> = peers.into_iter().filter(|addr| *addr != from).collect();

        // check if all blocks are present
        if blocks.iter().all(|b| b.is_some()) {
            let mut piece_bytes = Vec::with_capacity(piece_len);
        
            for b in blocks.iter() {
                piece_bytes.extend_from_slice(b.as_ref().unwrap());
            }

            return Ok((Some(piece_bytes), cancel));
        }

        Ok((None, cancel))
    }

    /// set the piece state as complete
    pub fn mark_complete(&mut self, piece: u32) -> Result<()> {
        let idx = piece as usize;

        match &self.piece_state[idx] {
            PieceStatus::Missing => {
                return Err(Error::PieceCompletionImpossibleWithoutRequest);
            }
            PieceStatus::Complete => {
                return Err(Error::PieceAlreadyComplete);
            }
            PieceStatus::InFlight { .. } => {}
        }

        self.piece_state[idx] = PieceStatus::Complete;
        self.bitfield.set(piece);
        Ok(())
    }

    /// in case a block or piece hash fails, mark the piece as missing
    pub fn mark_failed(&mut self, piece: u32, addr: SocketAddr) -> Result<()> {
        self.failed_peers[piece as usize].insert(addr);

        match &self.piece_state[piece as usize] {
            PieceStatus::InFlight { .. } => {},
            _ => return Err(Error::InvalidFailAttempt),
        }
        
        self.piece_state[piece as usize] = PieceStatus::Missing;
        Ok(())
    }

    pub fn is_endgame(&self) -> bool {
        self.bitfield.missing() <= ENDGAME_THRESHOLD
    }

    pub fn progress(&self) -> (u32, u32) {
        (self.bitfield.count_ones(), self.piece_count)
    }

    pub fn exists(&self, piece_index: u32) -> bool {
        if piece_index >= self.piece_count {
            return false;
        }
        self.piece_state[piece_index as usize] == PieceStatus::Complete
    }

    pub fn block_offset(&self, block: u64) -> u64 {
        block * BLOCK_LENGTH as u64
    }

    pub fn block_length(&self, piece: u32, block: u32) -> u64 {
        let piece_offset = piece as u64 * self.piece_length as u64;
        let block_offset = piece_offset + block as u64 * BLOCK_LENGTH as u64;

        (self.total_length - block_offset).min(BLOCK_LENGTH as u64)
    }

    pub fn block_exists(&self, piece_index: u32, block: u64) -> bool {
        if piece_index >= self.piece_count {
            return false;
        }

        let piece_len = (self.total_length - (piece_index as u64 * self.piece_length as u64))
            .min(self.piece_length as u64);
        let block_count = piece_len.div_ceil(BLOCK_LENGTH as u64);

        if block >= block_count { return false; }

        match &self.piece_state[piece_index as usize] {
            PieceStatus::Complete => return true,
            PieceStatus::Missing => return false,
            PieceStatus::InFlight { blocks, .. } => { 
                return blocks[block as usize].is_some();
            }
        }
    }

    pub fn cancel_request(&mut self, piece: u32, block: u32, from: SocketAddr) {
        if let PieceStatus::InFlight { requested_from, .. } = &mut self.piece_state[piece as usize] {
            if let Some(v) = requested_from.get_mut(block as usize) {
                v.retain(|a| *a != from);
            }
        }
    }

    fn block_count_for(&self, index: u32) -> usize {
        let block_start = index as u64 * self.piece_length as u64;
        let this_len = (self.total_length - block_start).min(self.piece_length as u64);
        this_len.div_ceil(BLOCK_LENGTH as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yarn::{Yarn, Info};

    fn dummy_yarn(piece_count: usize, piece_length: u32) -> Yarn {
        let total = piece_count as u64 * piece_length as u64;
        Yarn {
            author: None, creation_date: None, comments: None,
            info: Info {
                filename: "test.bin".into(),
                total_length: total,
                piece_length,
                pieces: vec![[0u8; 32]; piece_count],
            },
        }
    }

    fn addr(n: u8) -> SocketAddr {
        format!("127.0.0.1:{}", 9000 + n as u16).parse().unwrap()
    }

    #[test]
    fn bitfield_msb_layout() {
        let mut bf = Bitfield::new(8);
        bf.set(0);
        assert_eq!(bf.bits[0], 0b1000_0000);
        bf.set(7);
        assert_eq!(bf.bits[0], 0b1000_0001);
    }

    #[test]
    fn bitfield_set_and_has_above_byte_zero() {
        // This test catches the bounds-check bug.
        let mut bf = Bitfield::new(13);
        bf.set(0);
        bf.set(8);
        bf.set(12);
        assert!(bf.has(0));
        assert!(bf.has(8));
        assert!(bf.has(12));
        assert!(!bf.has(1));
        assert!(!bf.has(13));   // out of range
    }

    #[test]
    fn bitfield_full_13_pieces() {
        let mut bf = Bitfield::new(13);
        bf.full();
        assert_eq!(bf.count_ones(), 13);
        assert!(bf.is_complete());
        assert!(!bf.has(13));
    }

    #[test]
    fn fresh_manager_has_nothing() {
        let y = dummy_yarn(10, 256 * 1024);
        let pm = PieceManager::new(&y).unwrap();
        assert_eq!(pm.progress(), (0, 10));
    }

    #[test]
    fn choose_piece_skips_complete_and_unavailable() {
        let y = dummy_yarn(10, 256 * 1024);
        let mut pm = PieceManager::new(&y).unwrap();
        // set state directly for testing
        pm.piece_state[1] = PieceStatus::Complete;
        pm.bitfield.set(1);

        let mut pod = Bitfield::new(10);
        pod.set(1);   // pod has piece 1, which we also have
        pod.set(5);

        assert_eq!(pm.choose_piece(&pod), Some(5));
    }

    #[test]
    fn choose_piece_skips_fully_inflight() {
        let y = dummy_yarn(3, 256 * 1024);
        let mut pm = PieceManager::new(&y).unwrap();

        // Request every block of piece 0 from peer 1
        for b in 0..16 {
            pm.mark_requested(0, b, addr(1));
        }

        let mut pod = Bitfield::new(3);
        pod.set(0);
        pod.set(1);

        // Piece 0 has no un-requested blocks. Should skip to piece 1.
        assert_eq!(pm.choose_piece(&pod), Some(1));
    }

    #[test]
    fn record_block_assembles_when_complete() {
        let y = dummy_yarn(1, 256 * 1024);
        let mut pm = PieceManager::new(&y).unwrap();

        for b in 0..16 {
            pm.mark_requested(0, b, addr(1));
        }

        for b in 0..15 {
            let (assembled, _) = pm.record_block(0, b, vec![b as u8; 16_384], addr(1)).unwrap();
            assert!(assembled.is_none());
        }

        let (assembled, cancel) = pm.record_block(0, 15, vec![0xAB; 16_384], addr(1)).unwrap();
        assert!(cancel.is_empty());
        assert_eq!(assembled.unwrap().len(), 256 * 1024);
    }

    #[test]
    fn endgame_produces_cancel_list() {
        let y = dummy_yarn(1, 256 * 1024);
        let mut pm = PieceManager::new(&y).unwrap();

        pm.mark_requested(0, 0, addr(1));
        pm.mark_requested(0, 0, addr(2));   // duplicate

        let (_, cancel) = pm.record_block(0, 0, vec![0; 16_384], addr(1)).unwrap();
        assert_eq!(cancel, vec![addr(2)]);
    }
}