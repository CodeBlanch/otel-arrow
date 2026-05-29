// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use arrow::{array::*, compute::kernels::filter, datatypes::*};
use otap_df_pdata::schema::consts;

/// Number of u64 words per page. Each page covers 65,536 IDs (one full u16 range).
const ID_BITMAP_PAGE_WORDS: usize = 1024;

/// Number of [`clear`](IdBitmap::clear) cycles a page can remain unused before being evicted
/// (deallocated). A threshold of 16 means a page that hasn't been touched in 16 consecutive
/// `clear()` calls will be freed, preventing unbounded memory growth from adversarial inputs
/// while avoiding thrashing for pages that are used intermittently.
const ID_BITMAP_PAGE_EVICTION_THRESHOLD: u64 = 16;

/// A single page of the [`IdBitmap`], covering 65,536 IDs (8 KiB of bitmap data).
///
/// Each page tracks the generation in which it was last written, enabling the bitmap to evict
/// pages that haven't been touched in several cycles.
struct IdBitmapPage {
    words: [u64; ID_BITMAP_PAGE_WORDS],
    last_used_generation: u64,
}

impl IdBitmapPage {
    /// Creates a new zeroed page stamped with the given generation.
    fn new(generation: u64) -> Self {
        Self {
            words: [0u64; ID_BITMAP_PAGE_WORDS],
            last_used_generation: generation,
        }
    }
}

/// A paged bitmap for fast membership testing of ID values.
///
/// The underlying bitmap data is heap allocated, and the intention of this type is that it
/// can be reused between batches by calling the `clear` method. This method is also called
/// automatically by the `populate` method, allowing the bitmap to be rewritten from some
/// input ID column.
///
/// The ID space is partitioned into pages of 65,536 IDs each (8 KiB per page). For the common
/// case of dense IDs starting near 0 (typical of OTAP batches), there few pages are allocated.
///
/// The motivation for the paged bitmap is to protect against adversarial situations where we
/// receive batches containing few, sparse IDs.
///
/// ## Page lifecycle
///
/// Each page tracks the generation (batch cycle) in which it was last written. On
/// [`clear`](IdBitmap::clear), the generation counter is incremented and pages are evaluated:
///
/// - Pages used within the last [`PAGE_EVICTION_THRESHOLD`] generations are zeroed and retained.
/// - Pages that haven't been used in more than [`PAGE_EVICTION_THRESHOLD`] generations are
///   deallocated, preventing unbounded memory growth from adversarial or unusual input patterns.
///
/// This means pages that are used regularly (even intermittently) stay allocated, while pages
/// from one-off anomalous batches are eventually freed.Collapse commentComment on lines R73 to R83jmacd commented on Mar 16, 2026 jmacdon Mar 16, 2026ContributorMore actionsLooks good.ReactWrite a replyResolve comment
pub struct IdBitmap {
    pages: Vec<Option<Box<IdBitmapPage>>>,
    generation: u64,
}

impl IdBitmap {
    /// Creates a new empty `IdBitmap`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            generation: 0,
        }
    }

    /// Clears all bits in the bitmap, evicting stale pages.
    pub fn clear(&mut self) {
        self.generation += 1;
        for page_slot in &mut self.pages {
            if let Some(page) = page_slot {
                if self.generation - page.last_used_generation > ID_BITMAP_PAGE_EVICTION_THRESHOLD {
                    *page_slot = None;
                } else {
                    page.words.fill(0);
                }
            }
        }
        // Trim trailing None slots to avoid unbounded growth of the outer vec
        while self.pages.last().is_some_and(|p| p.is_none()) {
            let _ = self.pages.pop();
        }
    }

    /// Returns the page index and bit position within the page for the given ID.
    #[inline]
    const fn page_and_bit(id: u32) -> (usize, usize) {
        let page_idx = (id >> 16) as usize;
        let bit_idx = (id & 0xFFFF) as usize;
        (page_idx, bit_idx)
    }

    /// Ensures the page for the given page index exists, allocating it if necessary,
    /// and stamps it with the current generation.
    #[inline]
    fn ensure_page(&mut self, page_idx: usize) -> &mut IdBitmapPage {
        if page_idx >= self.pages.len() {
            self.pages.resize_with(page_idx + 1, || None);
        }
        let generation = self.generation;
        let page =
            self.pages[page_idx].get_or_insert_with(|| Box::new(IdBitmapPage::new(generation)));
        page.last_used_generation = generation;
        page
    }

    /// Inserts an ID into the bitmap.
    #[inline]
    pub fn insert(&mut self, id: u32) {
        let (page_idx, bit_idx) = Self::page_and_bit(id);
        let page = self.ensure_page(page_idx);
        page.words[bit_idx / 64] |= 1 << (bit_idx % 64);
    }

    /// Returns `true` if the bitmap contains the given ID.
    #[inline]
    #[must_use]
    pub fn contains(&self, id: u32) -> bool {
        let (page_idx, bit_idx) = Self::page_and_bit(id);
        match self.pages.get(page_idx) {
            Some(Some(page)) => page.words[bit_idx / 64] & (1 << (bit_idx % 64)) != 0,
            _ => false,
        }
    }

    /// Returns the number of IDs stored in the bitmap (popcount).
    #[must_use]
    pub fn len(&self) -> u64 {
        self.pages
            .iter()
            .filter_map(|p| p.as_ref())
            .flat_map(|p| p.words.iter())
            .map(|w| w.count_ones() as u64)
            .sum()
    }

    /// Returns `true` if the bitmap contains no IDs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pages
            .iter()
            .filter_map(|p| p.as_ref())
            .all(|p| p.words.iter().all(|&w| w == 0))
    }

    /// Clears the bitmap and repopulates it from the given iterator.
    ///
    /// This reuses existing page allocations when possible. New pages are allocated as needed,
    /// and stale pages are evicted per the generation-based eviction policy.
    pub fn populate(&mut self, iter: impl Iterator<Item = u32>) {
        self.clear();
        for id in iter {
            self.insert(id);
        }
    }
}

impl Default for IdBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for IdBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let allocated_pages = self.pages.iter().filter(|p| p.is_some()).count();
        f.debug_struct("IdBitmap")
            .field("total_pages", &self.pages.len())
            .field("allocated_pages", &allocated_pages)
            .field("generation", &self.generation)
            .field("len", &self.len())
            .finish()
    }
}

impl PartialEq for IdBitmap {
    fn eq(&self, other: &Self) -> bool {
        // Compare the logical content only (not generation counters): for each page index,
        // both bitmaps must have the same bits set. Missing or None pages are treated as
        // all-zeros.
        let max_pages = self.pages.len().max(other.pages.len());
        for i in 0..max_pages {
            let self_words = self
                .pages
                .get(i)
                .and_then(|p| p.as_ref())
                .map(|p| &p.words[..]);
            let other_words = other
                .pages
                .get(i)
                .and_then(|p| p.as_ref())
                .map(|p| &p.words[..]);
            match (self_words, other_words) {
                (Some(sw), Some(ow)) => {
                    if sw != ow {
                        return false;
                    }
                }
                (Some(w), None) | (None, Some(w)) => {
                    if w.iter().any(|&v| v != 0) {
                        return false;
                    }
                }
                (None, None) => {}
            }
        }
        true
    }
}

pub fn filter_child_batch(
    ids: &IdBitmap,
    parent_ids: Option<PrimitiveArray<UInt16Type>>,
    child_batch: &RecordBatch,
) -> Option<RecordBatch> {
    let filter = build_uint16_id_filter(
        parent_ids.as_ref().unwrap_or_else(|| {
            child_batch
                .column_by_name(consts::PARENT_ID)
                .expect("has parent ids")
                .as_primitive::<UInt16Type>()
        }),
        ids,
    );

    if filter.true_count() > 0 {
        Some(filter::filter_record_batch(child_batch, &filter).unwrap())
    } else {
        None
    }
}

/// Builds a selection [`BooleanArray`] for a native (non-dictionary) [`PrimitiveArray`] by checking
/// each value against the [`IdBitmap`].
#[must_use]
fn build_uint16_id_filter(
    id_column: &PrimitiveArray<UInt16Type>,
    id_set: &IdBitmap,
) -> BooleanArray {
    let mut builder = BooleanBuilder::with_capacity(id_column.len());
    let mut seg_val = false;
    let mut seg_len = 0usize;

    for val in id_column {
        let valid = match val {
            Some(v) => id_set.contains(v as u32),
            None => false,
        };

        if valid != seg_val {
            if seg_len > 0 {
                builder.append_n(seg_len, seg_val);
            }
            seg_val = valid;
            seg_len = 0;
        }

        seg_len += 1;
    }

    if seg_len > 0 {
        builder.append_n(seg_len, seg_val);
    }

    builder.finish()
}
