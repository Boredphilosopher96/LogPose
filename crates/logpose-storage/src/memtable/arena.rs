//! [`VectorArena`]: a memtable's vectors of one field, contiguous `f32` in fixed blocks.

use crate::dv::CowBitmap;
use logpose_types::RowId;
use std::sync::Arc;

/// Rows per full block of a [`VectorArena`].
pub(crate) const BLOCK_ROWS: usize = 16;

/// The vectors of one field, one per slot, in blocks of [`BLOCK_ROWS`] rows.
///
/// Full blocks are immutable and shared by every clone; only the partial last block (`tail`) is
/// copied on write, at most `BLOCK_ROWS - 1` rows, and only on the first append after a clone
/// (so at most once per group). Null slots are stored as zeros and listed in `nulls`.
#[derive(Clone)]
pub(crate) struct VectorArena {
    dim: u32,
    /// Full blocks of `BLOCK_ROWS * dim` floats each.
    blocks: imbl::Vector<Arc<[f32]>>,
    /// The partial last block: fewer than `BLOCK_ROWS` rows.
    tail: Arc<Vec<f32>>,
    nulls: CowBitmap,
    len: u32,
}

impl VectorArena {
    pub(crate) fn new(dim: u32) -> Self {
        Self {
            dim,
            blocks: imbl::Vector::new(),
            tail: Arc::default(),
            nulls: CowBitmap::default(),
            len: 0,
        }
    }

    pub(crate) fn dim(&self) -> u32 {
        self.dim
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    /// Append the next slot's vector (which must have `dim` components), or a null.
    pub(crate) fn push(&mut self, vector: Option<&[f32]>) {
        let dim = self.dim as usize;
        let tail = Arc::make_mut(&mut self.tail);
        match vector {
            Some(vector) => tail.extend_from_slice(&vector[..dim.min(vector.len())]),
            None => {
                self.nulls.insert(self.len);
            }
        }
        tail.resize(
            tail.len().max((self.len as usize % BLOCK_ROWS + 1) * dim),
            0.0,
        );
        self.len += 1;
        if (self.len as usize).is_multiple_of(BLOCK_ROWS) {
            let full = std::mem::take(tail);
            self.blocks.push_back(Arc::from(full));
        }
    }

    /// The vector at `slot`, `None` when it is null or out of range.
    pub(crate) fn get(&self, slot: RowId) -> Option<&[f32]> {
        if slot >= self.len || self.nulls.contains(slot) {
            return None;
        }
        let dim = self.dim as usize;
        let block = slot as usize / BLOCK_ROWS;
        let start = (slot as usize % BLOCK_ROWS) * dim;
        let rows: &[f32] = match self.blocks.get(block) {
            Some(block) => block,
            None => &self.tail,
        };
        rows.get(start..start + dim)
    }
}
