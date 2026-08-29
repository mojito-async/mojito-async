# mojito_async/reactor/io_op_table.mojo
#
# A7.2 reactor lane (issue #76) — the op-table slab: generation-tagged
# slots + a freelist-bump allocator, one entry per in-flight reactor
# registration.
#
# Model (issue #76 deliverable 1): a fixed-capacity slab keyed by slot
# index.  Each entry carries the op kind, the native fd, the registered
# interests, a lifecycle `state`, and — once the caller has parked its
# task — the WAITER identity as raw, TYPE-ERASED `(tcb_addr, task_id)`
# Ints (the SAME address-erasure TaskRecord/scope.mojo already use
# throughout this codebase; see task_control_block.mojo's TCB_Prefix
# layout-contract header) plus the WAITING-epoch `waiter_gen` the waiter
# captured at its OWN generation bump, so a delivery can reject a stale
# wake at BOTH the op-slot generation AND the task's own wait-epoch layer
# (belt-and-suspenders C6 defense in depth).
#
# allocate() is a freelist bump: a released slot returns to the head of
# the freelist and is handed out again on the NEXT allocate(), with its
# generation bumped (io_token.mojo documents the exact bump rule) — a
# slot is only reused after the PREVIOUS op is fully drained (released),
# never while still REGISTERED/ARMED/READY.
#
# Extern-free, allocation-free after construction (SYS-4/§15 precedent):
# the entries List is sized to CAPACITY once at __init__ and never grown
# or shrunk afterward; allocate()/release() only flip fields and freelist
# links.
from mojito_async.reactor.io_token import IoOpKind, IoToken, invalid_token

# Per-slot lifecycle.
comptime IO_OP_FREE = Int(0)  # unused / released; on the freelist
comptime IO_OP_REGISTERED = Int(1)  # OS interest live, no waiter attached yet
comptime IO_OP_ARMED = Int(2)  # waiter attached, awaiting readiness
comptime IO_OP_READY = Int(3)  # readiness delivered (drained); pending unregister


struct IoOpEntry(ImplicitlyCopyable, ImplicitlyDeletable):
    """One op-table slot.  `waiter_tcb == 0` means "no waiter attached yet"
    (register_op() was called but attach_waiter() has not); a slot may
    legitimately reach READY with no waiter (the caller polls instead of
    parking) — Reactor.poll()/service_io() only attempts a wake when
    `waiter_tcb != 0`."""

    var generation: Int
    var op_kind: Int
    var fd: Int32
    var interests: UInt32
    var state: Int
    var waiter_tcb: Int
    var waiter_task_id: Int
    var waiter_gen: Int
    var free_next: Int

    def __init__(out self):
        self.generation = 0
        self.op_kind = IoOpKind.NONE
        self.fd = -1
        self.interests = 0
        self.state = IO_OP_FREE
        self.waiter_tcb = 0
        self.waiter_task_id = 0
        self.waiter_gen = 0
        self.free_next = -1


struct IoOpTable(Movable):
    """Fixed-capacity op-table slab (issue #76 deliverable 1).  CAPACITY
    matches the native pollers' own per-wait event batch ceiling
    (vendor/mojito_sys_io/platform/kqueue.mojo's MAX_BATCH) so a single
    `Reactor.poll()` call can never observe more ready ops than the table
    could possibly hold live at once."""

    comptime CAPACITY = Int(256)

    var _entries: List[IoOpEntry]
    var _free_head: Int
    var _live: Int

    def __init__(out self):
        self._entries = List[IoOpEntry]()
        for i in range(Self.CAPACITY):
            var e = IoOpEntry()
            e.free_next = i + 1 if i + 1 < Self.CAPACITY else -1
            self._entries.append(e)
        self._free_head = 0
        self._live = 0


    # --- allocation ----------------------------------------------------

    def allocate(
        mut self, op_kind: Int, fd: Int32, interests: UInt32
    ) raises -> IoToken:
        """Pop the freelist head, bump its generation, and stamp a fresh
        REGISTERED entry (no waiter attached yet).  Raises when the table
        is full (CAPACITY live registrations) — the caller's register_op()
        propagates this as a real backpressure signal, never a silent
        drop."""
        if self._free_head < 0:
            raise Error(
                "IoOpTable.allocate: table full (capacity "
                + String(Self.CAPACITY)
                + ")"
            )
        var slot = self._free_head
        var e = self._entries[slot]
        self._free_head = e.free_next
        e.generation += 1
        e.op_kind = op_kind
        e.fd = fd
        e.interests = interests
        e.state = IO_OP_REGISTERED
        e.waiter_tcb = 0
        e.waiter_task_id = 0
        e.waiter_gen = 0
        e.free_next = -1
        self._entries[slot] = e
        self._live += 1
        return IoToken(slot, e.generation, op_kind)

    def release(mut self, token: IoToken) -> Bool:
        """Idempotent slot release: pushes `token.slot` back onto the
        freelist IFF the token is still live (bounds-checked, state !=
        FREE, generation matches).  A stale/already-released token is a
        silent no-op — RETURNS False rather than raising — matching the
        native poller's own "already-closed handle degrades to a no-op"
        contract (spec §25); callers driving cancellation races (issue
        #76's "slot reuse after a cancelled/closed op never reuses an old
        token") never need to guard a double-release themselves."""
        if token.slot < 0 or token.slot >= Self.CAPACITY:
            return False
        var e = self._entries[token.slot]
        if e.state == IO_OP_FREE or e.generation != token.generation:
            return False
        e.state = IO_OP_FREE
        e.fd = -1
        e.op_kind = IoOpKind.NONE
        e.waiter_tcb = 0
        e.waiter_task_id = 0
        e.waiter_gen = 0
        e.free_next = self._free_head
        self._entries[token.slot] = e
        self._free_head = token.slot
        self._live -= 1
        return True

    # --- access ----------------------------------------------------------

    def is_live(self, token: IoToken) -> Bool:
        """True iff `token` still names its ORIGINAL registration: bounds-
        checked, slot not FREE, and the stamped generation still matches —
        the single predicate every staleness check in this package (and
        in reactor.mojo's delivery path) is built from."""
        if token.slot < 0 or token.slot >= Self.CAPACITY:
            return False
        var e = self._entries[token.slot]
        return e.state != IO_OP_FREE and e.generation == token.generation

    def get(self, slot: Int) -> IoOpEntry:
        """Raw slot read by INDEX (no generation check — callers that
        already validated a token via `is_live` use this to read the
        entry; a slot outside [0, CAPACITY) returns a FREE sentinel entry
        rather than raising, since this is a hot, allocation-free read
        path with no natural error to report)."""
        if slot < 0 or slot >= Self.CAPACITY:
            return IoOpEntry()
        return self._entries[slot]

    def set(mut self, slot: Int, e: IoOpEntry):
        """Raw slot write by INDEX — the counterpart to `get`, used by
        `attach_waiter`/delivery to update fields in place."""
        if slot < 0 or slot >= Self.CAPACITY:
            return
        self._entries[slot] = e

    def live_count(self) -> Int:
        """Number of currently-allocated (non-FREE) slots (observability
        for tests: e.g. asserting a released slot's count drops)."""
        return self._live
