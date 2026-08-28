# mojito_async/fiber/stack_pool.mojo
#
# A1.4 (issue #52) — per-worker stack cache / allocator over mojito-sys.
#
# Spec §15 (cache policy) + §18 (one stack cache per worker): completed
# stack reservations are pooled and reused across a task lifecycle.  This is
# PURE Mojo policy over the vendored NativeStack mechanism — ms_stack_alloc
# / ms_stack_free own the guard pages, live-address immutability and
# commit-as-you-grow; the pool only decides WHICH reservation to hand out
# and when to return it to the free set (spec: "the stack object remains a
# mojito-sys mechanism").
#
# Bounds (spec §15): the cache is bounded by stack COUNT (capacity) AND
# reserved bytes AND committed bytes.  Every live stack is one uniform
# reservation (page-rounded + guard page by ms_stack_alloc), so the count
# bound implies the reserved-byte bound (capacity * (stack_bytes+guard),
# computed with the vendored ms_page_size).  The committed-byte bound is
# observed via ms_stack_total_size() and stays within the same capacity
# budget.  A warm acquire() (reuse of an equal-size released stack) performs
# NO fresh ms_stack_alloc: warm-path OS allocation is flat.
#
# Reuse gate (spec §15 "never recycle until unquestionably complete"): a
# cache entry is handed to a NEW caller only after an explicit release(),
# which the caller performs once the owning fiber/task is TERMINAL.  The
# pool never recycles a still-acquired (live) cell: a cell only re-enters
# the free set through release().  Attempting to release a cell that is not
# live raises (a correctness signal, not odr-use).
#
# Decommit of cold pages: the frozen vendored substrate exposes no separate
# decommit API, so per the issue ("decommit cold pages IF the vendored API
# supports it") decommit is gated OFF; the over-budget / drain path instead
# returns reservations to the OS via ms_stack_free (retire/drain), which is
# the decommit-equivalent eviction this substrate offers.
#
# Extern discipline (b2, modular/modular#6971): every extern call site sits
# at CONCRETE module scope (this struct is non-generic).  The extern symbols
# come from the production vendor seam (mojito_async.vendor.mojito_sys),
# never re-declared here.
#
# Ownership: the pool owns a caller-visible array of NativeStack cells
# (allocated once at construction — caller-owns-cells, TCB pattern).
# acquire() returns a pointer INTO the cell array; the caller owns that
# NativeStack handle while acquired and MUST release()/retire() it before
# the pool may reuse the cell.
#
# Link with:  mojo run -Xlinker <repo>/libmojito_spike.dylib <driver>

from std.memory import stack_allocation

from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
    ms_stack_total_size,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default reservation size for a pooled stack (page-rounded + guard by
# ms_stack_alloc).  64 KiB usable per fiber stack for A1.
comptime DEFAULT_STACK_BYTES = Int(65536)

# Cell lifecycle states.
comptime STATE_FREE = Int(0)      # slot never allocated (initial)
comptime STATE_LIVE = Int(1)      # allocated AND acquired (owned by a caller)
comptime STATE_CACHED = Int(2)    # allocated, released, in the free set

# Freelist "no next" sentinel.
comptime NO_NEXT = Int(-1)


# ---------------------------------------------------------------------------
# Outside-C (libc) raw memory for the pool's cell backing.  These live at
# CONCRETE module scope (extern discipline), not inside the struct.
# ---------------------------------------------------------------------------

@extern("calloc")
def _c_calloc(count: Int, size: Int) abi("C") -> UnsafePointer[Byte, MutAnyOrigin]:
    ...


# sizeof(NativeStack) measured as a pointer stride once, at module scope,
# from a stack-allocated element (UnsafePointer is non-nullable, so address
# 0 is not constructible).
def _native_stack_stride() -> Int:
    var one = stack_allocation[1, NativeStack]()
    return Int(one + 1) - Int(one)


# ---------------------------------------------------------------------------
# StackCache — per-worker stack allocator/cache.
# ---------------------------------------------------------------------------

struct StackCache(ImplicitlyCopyable, ImplicitlyDeletable):
    """Bounded per-worker stack cache over mojito-sys (issue #52).

    Fixed-capacity, no per-alloc heap traffic: backing cells are carved once
    at construction.  acquire() prefers a warm (exact-size) freelist hit;
    only a miss calls ms_stack_alloc.  release() returns the cell to the free
    set; retire() drops a reservation back to the OS.  drain() empties the
    cache.  All three spec §15 bounds (count / reserved / committed) derive
    from the fixed capacity and are enforced by construction + observable
    via the counters."""

    # Unified backing block: `capacity` cells of NativeStack, followed by
    # `capacity` state Ints and `capacity` freelist-next Ints.  Single malloc
    # so the pool is one pointer + counters (caller-owns-cells TCB slab).
    var _cells: UnsafePointer[NativeStack, MutAnyOrigin]
    var _capacity: Int
    var _stack_bytes: Int
    var _page_size: Int
    var _cell_bytes: Int

    # Parallel arrays (indexed by cell ordinal), packed after the cells.
    var _state: UnsafePointer[Int, MutAnyOrigin]
    var _next: UnsafePointer[Int, MutAnyOrigin]

    var _live: Int      # cells currently acquired (live)
    var _cached: Int    # cells in the free set (released, reusable)
    var _head: Int      # freelist head cell ordinal (NO_NEXT when empty)

    def __init__(out self):
        self._cells = UnsafePointer[NativeStack, MutAnyOrigin](unsafe_from_address=1)
        self._capacity = 0
        self._stack_bytes = 0
        self._page_size = 0
        self._cell_bytes = 0
        self._state = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self._next = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self._live = 0
        self._cached = 0
        self._head = NO_NEXT

    def __init__(out self, capacity: Int, stack_bytes: Int, ps: Int,
                 cell_bytes: Int, block: UnsafePointer[Byte, MutAnyOrigin]):
        """Bind a pre-allocated backing block.  `block` must be large enough
        for `capacity` cells: capacity * (cell_bytes + 2*sizeof Int).
        The caller owns the block (caller-owns-cells, TCB pattern)."""
        self._cells = block.bitcast[NativeStack]()
        self._capacity = capacity
        self._stack_bytes = stack_bytes
        self._page_size = ps
        self._cell_bytes = cell_bytes
        # state / next arrays sit immediately after the cells.
        var cells_ofs = capacity * cell_bytes
        var st = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=Int(block) + cells_ofs
        )
        self._state = st
        self._next = st + capacity
        self._live = 0
        self._cached = 0
        self._head = NO_NEXT
        # Initialise freelist.  The backing block was calloc'd (zeroed), so
        # cells read as zero NativeStacks without invoking the default ctor
        # (whose unstable_from_address=0 is non-nullable in a b2 codegen).
        var i = 0
        while i < capacity:
            self._state[i] = STATE_FREE
            self._next[i] = NO_NEXT
            i += 1

    # -- queries -----------------------------------------------------------

    def capacity(self) -> Int:
        return self._capacity

    def stack_bytes(self) -> Int:
        return self._stack_bytes

    def live(self) -> Int:
        return self._live

    def cached(self) -> Int:
        return self._cached

    def page_size(self) -> Int:
        return self._page_size

    def cell_bytes(self) -> Int:
        return self._cell_bytes

    def reserved_bytes(self) -> Int:
        """Reserved-byte budget (spec §15): every pooled stack reservation is
        exactly stack_bytes + one guard page."""
        return self._capacity * (self._stack_bytes + self._page_size)

    def total_live_bytes(self) -> Int:
        """Committed bytes currently held by this process (ms_stack_total_size)."""
        return Int(ms_stack_total_size())

    # -- acquire -----------------------------------------------------------

    # Prefer a warm freelist hit; only a miss calls ms_stack_alloc.  Returns
    # a pointer into the pool's cell array (caller owns the NativeStack while
    # acquired).
    def acquire(mut self) raises -> UnsafePointer[NativeStack, MutAnyOrigin]:
        if self._cached > 0:
            # Warm hit: pop the head cell, reuse its existing reservation.
            var idx = self._head
            self._head = self._next[idx]
            self._next[idx] = NO_NEXT
            self._state[idx] = STATE_LIVE
            self._cached -= 1
            self._live += 1
            return self._cells + idx

        # Cold: is the pool at capacity (all cells either live or cached)?
        if self._live + self._cached >= self._capacity:
            raise Error(
                "stack_pool.acquire: pool exhausted (capacity "
                + String(self._capacity) + " all in use)"
            )

        # Find a STATE_FREE (never-allocated) cell to allocate into.
        var idx = self._find_free()
        # Enforce the reserved-byte budget explicitly before mmap-ing.
        if not self._reserved_ok():
            raise Error("stack_pool.acquire: reserved-byte budget exceeded")
        var slots = stack_allocation[2, BytePtr]()
        var rc = ms_stack_alloc(self._stack_bytes, slots, slots + 1)
        if rc != 0:
            raise Error("stack_pool.acquire: ms_stack_alloc failed rc=" + String(rc))
        self._cells[idx] = NativeStack(slots[], (slots + 1)[])
        self._state[idx] = STATE_LIVE
        self._live += 1
        return self._cells + idx

    def _reserved_ok(self) -> Bool:
        """True if allocating one more stack stays within the reserved-byte
        budget (capacity * (stack_bytes+guard)).  Equal-size reservoirs make
        count-capacity the binding bound; kept as an explicit, testable gate."""
        var per_stack = self._stack_bytes + self._page_size
        var could_total = (self._live + self._cached + 1) * per_stack
        return could_total <= self.reserved_bytes()

    def _find_free(self) raises -> Int:
        var i = 0
        while i < self._capacity:
            if self._state[i] == STATE_FREE:
                return i
            i += 1
        raise Error("stack_pool.acquire: no free cell (internal)")

    # -- release -----------------------------------------------------------

    # Reuse gate: returns `cell` (must be a LIVE cell this pool handed out)
    # to the free set.  The caller attests the owning fiber/task reached
    # TERMINAL before invoking this; the pool enforces that the cell is in
    # fact live (never double-release, never release-of-unknown).
    def release(mut self, cell: UnsafePointer[NativeStack, MutAnyOrigin]) raises:
        var idx = self._index_of(cell)
        if idx < 0:
            raise Error("stack_pool.release: cell not owned by this pool")
        if self._state[idx] != STATE_LIVE:
            raise Error(
                "stack_pool.release: cell " + String(idx)
                + " not live (reuse-gate violation: recycle of a non-terminal task)"
            )
        self._next[idx] = self._head
        self._head = idx
        self._state[idx] = STATE_CACHED
        self._live -= 1
        self._cached += 1

    # Drop ONE cached reservation back to the OS (eviction / decommit-
    # equivalent).  Only valid on a CACHED cell; the cell returns to FREE.
    def retire(mut self, cell: UnsafePointer[NativeStack, MutAnyOrigin]) raises:
        var idx = self._index_of(cell)
        if idx < 0:
            raise Error("stack_pool.retire: cell not owned by this pool")
        if self._state[idx] != STATE_CACHED:
            raise Error(
                "stack_pool.retire: cell " + String(idx) + " not cached"
            )
        if self._cells[idx].alive():
            ms_stack_free(self._cells[idx].base)
        self._state[idx] = STATE_FREE
        self._cached -= 1
        self._unlink(idx)

    # Return every cached reservation to the OS; pool stays usable with
    # capacity hot cells re-acquirable on demand.
    def drain(mut self) raises:
        var idx = self._head
        while idx != NO_NEXT:
            var nxt = self._next[idx]
            if self._cells[idx].alive():
                ms_stack_free(self._cells[idx].base)
            self._state[idx] = STATE_FREE
            self._next[idx] = NO_NEXT
            self._cached -= 1
            idx = nxt
        self._head = NO_NEXT

    def _unlink(mut self, target: Int) raises:
        # Remove `target` from the freelist.  O(capacity); retiring is cold.
        if target == self._head:
            self._head = self._next[target]
            self._next[target] = NO_NEXT
            return
        var prev = self._head
        while prev != NO_NEXT:
            var cur = self._next[prev]
            if cur == target:
                self._next[prev] = self._next[cur]
                self._next[cur] = NO_NEXT
                return
            prev = cur
        raise Error("stack_pool._unlink: target not in freelist")

    def _index_of(self, cell: UnsafePointer[NativeStack, MutAnyOrigin]) -> Int:
        """Cell ordinal for a pointer into this pool's backing, or -1."""
        var addr = Int(cell)
        var base = Int(self._cells)
        if addr < base:
            return -1
        var stride = self._cell_bytes
        var diff = addr - base
        if diff % stride != 0:
            return -1
        var idx = diff // stride
        if idx < 0 or idx >= self._capacity:
            return -1
        return idx


# ---------------------------------------------------------------------------
# Module factory (b2 has no static methods).
# ---------------------------------------------------------------------------

# Build a fresh StackCache, allocating its backing cells internally (one
# calloc of the whole slab) so the returned cache is immediately usable.
def make_stack_cache(
    capacity: Int,
    stack_bytes: Int = DEFAULT_STACK_BYTES,
) raises -> StackCache:
    """Create a per-worker stack cache with `capacity` slots.

    Allocates the internal cell slab (caller-owns-cells pattern realised
    inside the pool: storage set up once up-front, never on the hot path).
    Raises when the backing block cannot be allocated.
    """
    if capacity <= 0:
        raise Error("stack_pool.make_stack_cache: capacity must be positive")
    var ps = Int(ms_page_size())
    if ps <= 0:
        ps = 4096

    var cell_bytes = _native_stack_stride()
    if cell_bytes <= 0:
        cell_bytes = 16
    var total = capacity * (cell_bytes + 2 * 8)
    var block = _c_calloc(capacity, cell_bytes + 2 * 8)
    if Int(block) == 0:
        raise Error("stack_pool.make_stack_cache: backing allocation failed")

    var c = StackCache(capacity, stack_bytes, ps, cell_bytes, block)
    return c
