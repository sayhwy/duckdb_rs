//! Arena allocator — bump-pointer memory arena.
//!
//! Mirrors `src/storage/arena_allocator.cpp` + `.hpp`.
//!
//! # Design decisions vs. C++
//!
//! | C++ | Rust (this file) | Reason |
//! |-----|-----------------|--------|
//! | `unsafe_unique_ptr<ArenaChunk> next` | `Option<Box<ArenaChunk>>` | Idiomatic ownership chain |
//! | `ArenaChunk *prev` | omitted | Cannot express non-owning back-pointer in safe Rust; `prev` was write-only in the C++ shown |
//! | `ArenaChunk *tail` | traversal / `Option<*mut>` (private) | See note below |
//! | `AllocatedData data` | `Box<[u8]>` | Stable heap address even when struct is moved |
//! | `Allocator &allocator` | `Box<dyn ChunkAllocator>` | Trait-based, allows test injection |
//! | `data_ptr_t Allocate(len)` → raw ptr | `*mut u8` return | Caller uses `unsafe` to dereference; API surface is safe |
//!
//! ## `tail` note
//! C++ keeps a `*tail` for O(1) access to the oldest chunk (used in `reset` and
//! `get_tail`).  In safe Rust a raw back-pointer into a `Box` chain is unsound
//! without `unsafe`.  We resolve this by traversing the chain for `get_tail`
//! (O(n), acceptable since it is called rarely) and recomputing it on `reset`.
//! If profiling shows this matters, a private `*mut ArenaChunk` field can be
//! added behind a thin `unsafe` block in a future iteration.

// ────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────

/// Default capacity for the first chunk (matches DuckDB: 2 KiB).
pub const ARENA_ALLOCATOR_INITIAL_CAPACITY: usize = 2048;

/// Maximum capacity for a single chunk (matches DuckDB: 16 MiB).
pub const ARENA_ALLOCATOR_MAX_CAPACITY: usize = 1 << 24;

/// Alignment requirement for `allocate_aligned` / `align_next` (8 bytes).
pub const ARENA_ALIGNMENT: usize = 8;

// ────────────────────────────────────────────────────────────
// Allocator trait
// ────────────────────────────────────────────────────────────

/// Strategy for obtaining raw byte slabs.
///
/// The default implementation (`SystemChunkAllocator`) uses the global
/// allocator via `vec![0u8; capacity]`.  Tests can inject a custom
/// implementation to track allocations or inject failures.
pub trait ChunkAllocator {
    /// Returns a zeroed, heap-allocated byte slice of exactly `capacity` bytes.
    fn allocate_chunk(&self, capacity: usize) -> Box<[u8]>;
}

/// Default implementation: allocates via the global Rust allocator.
pub struct SystemChunkAllocator;

impl ChunkAllocator for SystemChunkAllocator {
    fn allocate_chunk(&self, capacity: usize) -> Box<[u8]> {
        vec![0u8; capacity].into_boxed_slice()
    }
}

// ────────────────────────────────────────────────────────────
// ArenaChunk
// ────────────────────────────────────────────────────────────

/// One contiguous memory slab in the arena chain.
///
/// The linked list runs from **newest → oldest** via `next`, mirroring the
/// C++ `head → ... → tail` direction.
///
/// `data` is `Box<[u8]>` so the backing bytes have a **stable heap address**:
/// even if the `ArenaChunk` struct itself is moved (e.g., when `head` is
/// reassigned in `ArenaAllocator`), the pointer `data.as_ptr()` stays valid.
/// This property is required for the in-place `reallocate` optimisation.
struct ArenaChunk {
    /// Fixed-size backing buffer; `data.len()` == `maximum_size`.
    data: Box<[u8]>,

    /// Bump cursor: bytes `[0, current_position)` are committed.
    current_position: usize,

    /// Ownership link to the next (older) chunk.
    next: Option<Box<ArenaChunk>>,
    // Note: no `prev` field — see module-level doc.
}

impl ArenaChunk {
    /// Allocates a new chunk of `capacity` bytes using `alloc`.
    fn new(alloc: &dyn ChunkAllocator, capacity: usize) -> Box<Self> {
        assert!(capacity > 0, "ArenaChunk capacity must be > 0");
        Box::new(ArenaChunk {
            data: alloc.allocate_chunk(capacity),
            current_position: 0,
            next: None,
        })
    }

    /// Total capacity of this chunk in bytes.
    #[inline]
    fn maximum_size(&self) -> usize {
        self.data.len()
    }

    /// Bytes available for new allocations.
    #[inline]
    fn remaining(&self) -> usize {
        self.maximum_size() - self.current_position
    }

    /// Raw pointer to the start of the backing buffer (stable address).
    #[inline]
    fn base_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Raw mutable pointer to the write cursor position.
    #[inline]
    fn cursor_ptr_mut(&mut self) -> *mut u8 {
        // SAFETY: current_position is always <= data.len() (class invariant).
        unsafe { self.data.as_mut_ptr().add(self.current_position) }
    }
}

// ────────────────────────────────────────────────────────────
// ArenaAllocator
// ────────────────────────────────────────────────────────────

/// Bump-pointer arena allocator.
///
/// Allocations are served from the current `head` chunk.  When a chunk is
/// full a new one is prepended (the chain grows from newest to oldest).
/// Memory is never individually freed; the entire arena is reclaimed with
/// [`reset`](ArenaAllocator::reset) or [`destroy`](ArenaAllocator::destroy).
///
/// # Pointer stability
/// All `*mut u8` pointers returned by `allocate*` are valid until the next
/// call to `reset()` or `destroy()` — identical contract to the C++ original.
///
/// # Thread safety
/// `ArenaAllocator` is **not** `Send` or `Sync` (it holds raw mutable state
/// with no interior locking).  The caller is responsible for external
/// synchronisation when sharing across threads.
pub struct ArenaAllocator {
    /// Head of the chunk list (most recently created, receives new allocations).
    head: Option<Box<ArenaChunk>>,

    /// Capacity used when the very first chunk is created.
    initial_capacity: usize,

    /// Sum of `maximum_size` across all live chunks (≥ `size_in_bytes()`).
    allocated_size: usize,

    /// Strategy for allocating new chunk backing memory.
    chunk_alloc: Box<dyn ChunkAllocator>,
}

impl ArenaAllocator {
    // ── Constructors ──────────────────────────────────────────

    /// Creates a new arena with default initial capacity and the system allocator.
    pub fn new() -> Self {
        Self::with_capacity(ARENA_ALLOCATOR_INITIAL_CAPACITY)
    }

    /// Creates a new arena with a custom initial capacity.
    pub fn with_capacity(initial_capacity: usize) -> Self {
        Self::with_capacity_and_allocator(initial_capacity, Box::new(SystemChunkAllocator))
    }

    /// Creates a new arena with a custom initial capacity and a custom chunk allocator.
    /// Useful for testing.
    pub fn with_capacity_and_allocator(
        initial_capacity: usize,
        chunk_alloc: Box<dyn ChunkAllocator>,
    ) -> Self {
        ArenaAllocator {
            head: None,
            initial_capacity,
            allocated_size: 0,
            chunk_alloc,
        }
    }

    // ── Core allocation ───────────────────────────────────────

    /// Allocates `len` bytes and returns a raw pointer to the start.
    ///
    /// The returned pointer is valid until `reset()` or `destroy()`.
    /// Callers must use `unsafe` to read/write through it.
    ///
    /// Mirrors `data_ptr_t ArenaAllocator::Allocate(idx_t len)` (inline in .hpp).
    pub fn allocate(&mut self, len: usize) -> *mut u8 {
        // Ensure current head chunk has room; if not, allocate a new one.
        let needs_new_block = match &self.head {
            None => true,
            Some(h) => h.current_position + len > h.maximum_size(),
        };
        if needs_new_block {
            self.allocate_new_block(len);
        }

        let head = self.head.as_mut().expect("head must exist after allocate_new_block");
        debug_assert!(head.current_position + len <= head.maximum_size());

        let ptr = head.cursor_ptr_mut();
        head.current_position += len;
        ptr
    }

    /// Attempts to grow or shrink a previous allocation in-place; falls back
    /// to a fresh allocation + copy when in-place growth is impossible.
    ///
    /// The passed `pointer` **must** have been returned by a prior `allocate*`
    /// call on this allocator.
    ///
    /// Mirrors `data_ptr_t ArenaAllocator::Reallocate(...)`.
    pub fn reallocate(&mut self, pointer: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
        todo!(
            "reallocate: check if `pointer` is the last allocation in `head` \
             (ptr::eq(pointer, head.base_ptr() + head.current_position - old_size)); \
             if yes, adjust cursor in-place; otherwise allocate(new_size) + memcpy"
        )
    }

    /// Advance the write cursor so the next allocation starts on an
    /// `ARENA_ALIGNMENT`-byte boundary.
    ///
    /// Mirrors `void ArenaAllocator::AlignNext()`.
    pub fn align_next(&mut self) {
        todo!("round head.current_position up to the next multiple of ARENA_ALIGNMENT")
    }

    /// `align_next()` then `allocate(align_up(size))`.
    ///
    /// Mirrors `data_ptr_t ArenaAllocator::AllocateAligned(idx_t size)`.
    pub fn allocate_aligned(&mut self, size: usize) -> *mut u8 {
        self.align_next();
        self.allocate(align_up(size))
    }

    /// `align_next()` then `reallocate(pointer, old_size, align_up(size))`.
    ///
    /// Mirrors `data_ptr_t ArenaAllocator::ReallocateAligned(...)`.
    pub fn reallocate_aligned(
        &mut self,
        pointer: *mut u8,
        old_size: usize,
        new_size: usize,
    ) -> *mut u8 {
        self.align_next();
        self.reallocate(pointer, old_size, align_up(new_size))
    }

    /// Shrink the most-recent allocation by `shrink_size` bytes.
    ///
    /// # Panics
    /// Panics if the head chunk's cursor is less than `shrink_size` (would
    /// underflow), i.e., if this is called without a matching prior `allocate`.
    ///
    /// Mirrors `void ArenaAllocator::ShrinkHead(idx_t shrink_size)`.
    pub fn shrink_head(&mut self, shrink_size: usize) {
        let head = self.head.as_mut().expect("cannot shrink empty arena");
        assert!(
            head.current_position >= shrink_size,
            "shrink_head({shrink_size}) underflows cursor ({})",
            head.current_position
        );
        head.current_position -= shrink_size;
    }

    // ── Lifecycle ─────────────────────────────────────────────

    /// Keeps only the `head` chunk, resets its cursor to zero, and drops all
    /// older chunks.  `allocated_size` is reset to zero.
    ///
    /// This is cheaper than `destroy()` when the arena will be reused
    /// immediately: the head chunk's backing buffer is reused.
    ///
    /// Mirrors `void ArenaAllocator::Reset()`.
    pub fn reset(&mut self) {
        todo!(
            "drop head.next chain; reset head.current_position = 0; \
             reset head.prev = nullptr equivalent; allocated_size = 0"
        )
    }

    /// Drops all chunks.  After this call `is_empty()` returns `true`.
    ///
    /// Mirrors `void ArenaAllocator::Destroy()`.
    pub fn destroy(&mut self) {
        self.head = None;
        self.allocated_size = 0;
    }

    /// Transfers ownership of all chunks to `other` and destroys `self`.
    ///
    /// `other` must be empty.
    ///
    /// Mirrors `void ArenaAllocator::Move(ArenaAllocator &other)`.
    pub fn move_into(&mut self, other: &mut ArenaAllocator) {
        assert!(other.is_empty(), "move_into: target arena must be empty");
        other.head = self.head.take();
        other.initial_capacity = self.initial_capacity;
        other.allocated_size = self.allocated_size;
        self.destroy();
    }

    // ── Inspection ────────────────────────────────────────────

    /// Returns `true` when no chunks have been allocated yet.
    ///
    /// Mirrors `bool ArenaAllocator::IsEmpty()`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    /// Sum of **used** bytes across all chunks (not cached; traverses the list).
    ///
    /// Mirrors `idx_t ArenaAllocator::SizeInBytes()`.
    pub fn size_in_bytes(&self) -> usize {
        let mut total = 0usize;
        let mut current = self.head.as_deref();
        while let Some(chunk) = current {
            total += chunk.current_position;
            current = chunk.next.as_deref();
        }
        total
    }

    /// Cached sum of **allocated** bytes (i.e., sum of chunk capacities).
    ///
    /// Mirrors `idx_t ArenaAllocator::AllocationSize()`.
    #[inline]
    pub fn allocation_size(&self) -> usize {
        self.allocated_size
    }

    /// Returns a reference to the head (most-recently-created) chunk, or `None`.
    pub fn get_head(&self) -> Option<&ArenaChunk> {
        self.head.as_deref()
    }

    /// Returns a reference to the tail (oldest) chunk by traversing the list,
    /// or `None` if the arena is empty.
    ///
    /// O(n) — see module doc for why we avoid a raw `*tail` pointer.
    ///
    /// Mirrors `ArenaChunk *ArenaAllocator::GetTail()`.
    pub fn get_tail(&self) -> Option<&ArenaChunk> {
        let mut current = self.head.as_deref()?;
        while let Some(next) = current.next.as_deref() {
            current = next;
        }
        Some(current)
    }

    // ── Internal ──────────────────────────────────────────────

    /// Prepends a new chunk to the list.  The new chunk's capacity follows the
    /// doubling strategy of the C++ original:
    ///
    /// 1. Start from `initial_capacity` (first chunk) or previous head's capacity.
    /// 2. Cap at `ARENA_ALLOCATOR_MAX_CAPACITY`.
    /// 3. Double if still below the cap.
    /// 4. Keep doubling until `>= min_size`.
    ///
    /// Mirrors `void ArenaAllocator::AllocateNewBlock(idx_t min_size)`.
    fn allocate_new_block(&mut self, min_size: usize) {
        // Step 1: pick starting capacity
        let mut capacity = match &self.head {
            None => self.initial_capacity,
            Some(h) => h.maximum_size(),
        };

        // Step 2: bring over-sized previous chunk back down to max
        if capacity > ARENA_ALLOCATOR_MAX_CAPACITY {
            capacity = ARENA_ALLOCATOR_MAX_CAPACITY;
        }

        // Step 3: double if still under max
        if capacity < ARENA_ALLOCATOR_MAX_CAPACITY {
            capacity *= 2;
        }

        // Step 4: keep doubling until we can serve `min_size`
        while capacity < min_size {
            capacity *= 2;
        }

        let mut new_chunk = ArenaChunk::new(self.chunk_alloc.as_ref(), capacity);

        // Prepend: new_chunk.next = old head
        new_chunk.next = self.head.take();
        self.head = Some(new_chunk);
        self.allocated_size += capacity;
    }
}

impl Default for ArenaAllocator {
    fn default() -> Self {
        Self::new()
    }
}

// ────────────────────────────────────────────────────────────
// Helper
// ────────────────────────────────────────────────────────────

/// Round `n` up to the next multiple of `ARENA_ALIGNMENT` (8 bytes).
#[inline]
pub fn align_up(n: usize) -> usize {
    (n + ARENA_ALIGNMENT - 1) & !(ARENA_ALIGNMENT - 1)
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_arena_is_empty() {
        let arena = ArenaAllocator::new();
        assert!(arena.is_empty());
        assert_eq!(arena.size_in_bytes(), 0);
        assert_eq!(arena.allocation_size(), 0);
    }

    #[test]
    fn single_allocate_creates_head_chunk() {
        let mut arena = ArenaAllocator::new();
        let _ptr = arena.allocate(64);
        assert!(!arena.is_empty());
        assert_eq!(arena.size_in_bytes(), 64);
    }

    #[test]
    fn allocation_triggers_new_block_when_full() {
        // Initial capacity 128 → first chunk is doubled to 256 bytes.
        // Allocating 256 fills it exactly; the next byte triggers a new block.
        let mut arena = ArenaAllocator::with_capacity(128);
        let _p1 = arena.allocate(256); // fills first block (128 * 2 = 256)
        let _p2 = arena.allocate(1);   // must trigger a new block
        // Two chunks: head (new) and tail (original).
        assert!(arena.get_head().is_some());
        assert!(arena.get_tail().is_some());
        // They should be different chunks.
        assert!(!std::ptr::eq(
            arena.get_head().unwrap() as *const _,
            arena.get_tail().unwrap() as *const _,
        ));
    }

    #[test]
    fn capacity_doubling_strategy() {
        let mut arena = ArenaAllocator::with_capacity(64);
        arena.allocate(64); // fills initial 64-byte block
        arena.allocate(1);  // new block should be 128 bytes (64 * 2)
        let head_cap = arena.get_head().unwrap().maximum_size();
        assert_eq!(head_cap, 128);
    }

    #[test]
    fn destroy_clears_arena() {
        let mut arena = ArenaAllocator::new();
        arena.allocate(32);
        arena.destroy();
        assert!(arena.is_empty());
        assert_eq!(arena.allocation_size(), 0);
    }

    #[test]
    fn shrink_head_adjusts_cursor() {
        let mut arena = ArenaAllocator::new();
        arena.allocate(64);
        let before = arena.size_in_bytes();
        arena.shrink_head(16);
        assert_eq!(arena.size_in_bytes(), before - 16);
    }

    #[test]
    fn align_up_works() {
        assert_eq!(align_up(0), 0);
        assert_eq!(align_up(1), 8);
        assert_eq!(align_up(7), 8);
        assert_eq!(align_up(8), 8);
        assert_eq!(align_up(9), 16);
    }

    #[test]
    fn move_into_transfers_ownership() {
        let mut src = ArenaAllocator::with_capacity(64);
        src.allocate(32);
        let alloc_size = src.allocation_size();

        let mut dst = ArenaAllocator::new();
        src.move_into(&mut dst);

        assert!(src.is_empty());
        assert_eq!(dst.allocation_size(), alloc_size);
    }

    #[test]
    fn get_tail_returns_oldest_chunk() {
        let mut arena = ArenaAllocator::with_capacity(16);
        arena.allocate(16); // chunk 0 (becomes tail after next alloc)
        arena.allocate(1);  // chunk 1 (head)
        // tail == oldest chunk, its capacity is the initial block size
        // (before doubling), so it should be smaller than head's.
        let tail_cap = arena.get_tail().unwrap().maximum_size();
        let head_cap = arena.get_head().unwrap().maximum_size();
        assert!(head_cap >= tail_cap);
    }
}
