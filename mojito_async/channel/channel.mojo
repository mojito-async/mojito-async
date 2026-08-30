# mojito_async/channel/channel.mojo
#
# A1.3 channel (issue #35) — bounded Channel[T]: ring buffer + sender/receiver
# wait queues + closed flags (spec §39-41), Sender/Receiver split, close
# semantics (spec §41), fast paths §40.1 / slow paths §40.2.
#
# Mojo 1.0.0b2 design notes (verified by the lane probes):
#   - `def`-only, module factories (no static methods), no hidden allocation
#     on the fast paths (the ring and the wait queues are caller-owned
#     Deques; parks allocate nothing).
#   - The Channel struct is PURE DATA: ring Deque[T], FIFO sender/receiver
#     wait queues (WaitRecord = tcb_addr + task_id), a deferred wake list
#     (_to_wake), the closed flags and the live slot counts.  It NEVER
#     reconstructs task handles from raw addresses — the b2 compiler
#     miscompiles `unsafe_from_address` reconstruction inside generic struct
#     methods (probes 7/10), so no transition is performed inside a method.
#     Parks use the TWO-PHASE `park_prepare`/`park_validate`/`park_commit`
#     kernel (see the cross-worker safety note below) on the PASSED-IN
#     JoinHandle of the CURRENT task; wakes are DEFERRED: a signal moves a
#     WaitRecord into `_to_wake`, and the embedding DRIVER (a plain concrete
#     function that knows the task's result type) drains `_to_wake` and
#     resumes each waiter via `unpark_current` — the canonical wake path
#     (single source, issue #39).
#   - send/recv are one-shot colorless operations: attempt now; if the
#     channel forces a wait, register the waiter and park.  On resume the
#     driver re-enters the task, which re-invokes send()/recv() with the
#     same pending item (A1.1 has no fibers — suspension happens between
#     dispatcher slices).  register_* dedupe by task_id.
#   - Algorithm (spec §40): producers buffer into the ring when it is not
#     full and, per buffered item, wake the OLDEST parked receiver (FIFO);
#     consumers pop the ring and, per item, wake the OLDEST parked sender.
#     A task parks only when the ring blocks it.  Close (spec §41): closing
#     the LAST sender marks the send side closed and moves EVERY parked
#     receiver into `_to_wake` — they drain the remaining values, then recv()
#     returns None (closed observable).  Closing the LAST receiver marks the
#     receive side closed, drops the buffered values, and moves EVERY parked
#     sender into `_to_wake` — their resumed send() raises "ChannelError:
#     send on closed channel".
#
# Cross-worker safety (issue #128, mirrors Mutex/RWLock's A4.1/A4-batch-
# review treatment): `_guard` is a SpinLock (queue.mojo's) serializing every
# read/write of `_items`/`_send_waiters`/`_recv_waiters`/`_to_wake`/the
# closed flags/the slot counts.  Channel predates the A2 M:N scheduler (this
# file landed in the very first merge batch, before cross-worker execution
# existed); a plain check-then-set on any of this state was correct only on
# the A1 single cooperative worker (no interleaving inside a slice) and
# would let two REAL M:N worker OS threads calling send()/recv()/try_send()/
# try_recv()/close() on the SAME channel concurrently race on the ring,
# waiter FIFOs, and close flags — the exact double-acquisition/lost-update
# class t38_mutex_cross_worker_aot caught for Mutex.  Every fast-path check
# is now combined with its slow-path FIFO append into ONE guarded critical
# section (mirrors Mutex.lock's exact shape: two separately-guarded calls
# would let a concurrent operation land BETWEEN them and miss a waiter or
# corrupt the ring).  send()/recv() also now park via the TWO-PHASE
# `park_prepare`/`park_validate`/`park_commit` kernel instead of the
# single-phase `park_current`, for the identical lost-wakeup reason
# documented in mutex.mojo's header (a cross-worker wake racing into the
# PARKING window between PREPARE and the WAITING commit).  Unlike Mutex's
# GRANT-marker re-entry (ownership transfer), a channel wake carries no
# ownership — it only means "retry, state may have changed" (the existing
# single-phase design's own re-entry contract).  So where Mutex's early
# unwind returns True directly, send()/recv() LOOP back to the top-of-
# function attempt instead: functionally identical to a fresh re-dispatch
# of the task by the scheduler, just resolved synchronously without ever
# actually entering WAITING.  A cancel-flavored early wake (the #57
# send_cancellable/recv_cancellable path) is caught by `raise_if_cancel_
# wake(h)` immediately after the early unwind — the exact "post-resume
# winner check ... on THIS SAME re-entry" park.mojo's with_cancel docstring
# already documents for this class of call.  The SpinLock's Atomic cannot
# be Movable/ImplicitlyDeletable, so Channel drops those conformances too —
# it now mirrors Mutex/RWLock/Runtime/Worker, which embed the identical
# guard for the identical reason.
from std.collections import Deque
from mojito_async.runtime.queue import SpinLock
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle, claim_running
from mojito_async.runtime.park import (
    park_commit,
    park_prepare,
    park_validate,
    raise_if_cancel_wake,
    wake_cancelled,
)
from mojito_async.cancellation import CancellationToken


# ---------------------------------------------------------------------------
# WaitRecord — type-erased waiter identity (DATA, not code)
# ---------------------------------------------------------------------------

struct WaitRecord(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Identity of a parked task: address of its caller-allocated TCB cell +
    scheduler id.  The embedding driver (which knows the task's concrete
    result type) reconstructs the JoinHandle from these two ints and resumes
    it via the canonical unpark_current."""

    var tcb_addr: Int
    var task_id: Int

    def __init__(out self):
        self.tcb_addr = 0
        self.task_id = 0

    def __init__(out self, tcb_addr: Int, task_id: Int):
        self.tcb_addr = tcb_addr
        self.task_id = task_id


def _remove_waiter_by_id(mut q: Deque[WaitRecord], task_id: Int) raises -> Bool:
    """Remove the WaitRecord whose task_id matches (issue #57's cancel_send_
    wait/cancel_recv_wait: a cancelled waiter may sit anywhere in the FIFO),
    preserving the relative order of every OTHER waiter.  O(n) rotate —
    Deque has no native middle-removal.  Only compares/moves the ALREADY-
    STORED task_id ints — never reconstructs a handle from them (the file
    header's ban on in-method address reconstruction stays intact).
    Returns True iff found+removed.  Caller MUST hold `_guard` (issue #128):
    this is a plain helper over caller-owned storage, not a method."""
    var n = len(q)
    var found = False
    for _ in range(n):
        var w = q.popleft()
        if w.task_id == task_id and not found:
            found = True
            continue
        q.append(w)
    return found



# ---------------------------------------------------------------------------
# RecvOutcome[T] — discriminated recv result (issue #152)
# ---------------------------------------------------------------------------

struct RecvOutcome[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable, Movable
):
    """Discriminated result of a blocking recv() call (issue #152).

    KIND   | Meaning
    -------+----------------------------------------------------------------
    VALUE  | A value was received.  Call `value()` to obtain it.
    CLOSED | The channel is closed-and-empty; no further values will arrive.
    PARKED | The task committed to WAITING (channel was empty and open).
             | The dispatcher MUST return immediately so the scheduler can
             | drive other tasks; on resume recv() is re-issued and returns
             | VALUE, CLOSED, or PARKED again (in multi-consumer scenarios a
             | competing receiver can steal the woken slot between the wake
             | and re-entry; dispatchers must loop until is_parked() is False).
    """

    comptime VALUE  = Int(0)
    comptime CLOSED = Int(1)
    comptime PARKED = Int(2)

    var _kind: Int
    var _value: Optional[Self.T]

    def __init__(out self, kind: Int):
        self._kind = kind
        self._value = Optional[Self.T]()

    def __init__(out self, kind: Int, value: Optional[Self.T]):
        self._kind = kind
        self._value = value

    def is_value(self) -> Bool:
        """True when a value was received (VALUE kind)."""
        return self._kind == Self.VALUE

    def is_closed(self) -> Bool:
        """True when the channel is closed-and-empty (CLOSED kind)."""
        return self._kind == Self.CLOSED

    def is_parked(self) -> Bool:
        """True when the task parked (PARKED kind).  The dispatcher MUST
        return immediately; do not inspect the value when is_parked()."""
        return self._kind == Self.PARKED

    def value(self) raises -> Self.T:
        """Return the received value.  Raises unless `is_value()` is True."""
        if self._kind != Self.VALUE:
            raise Error("RecvOutcome: expected VALUE, got kind "
                        + String(self._kind))
        return self._value.value()


# ---------------------------------------------------------------------------
# SendOutcome — discriminated send result (issue #152)
# ---------------------------------------------------------------------------

struct SendOutcome(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Discriminated result of a blocking send() call (issue #152).

    KIND   | Meaning
    -------+----------------------------------------------------------------
    SENT   | The item was delivered (buffered in the ring or handed off).
    PARKED | The task committed to WAITING (ring was full).
             | The dispatcher MUST return immediately; on resume send() is
             | re-issued with the SAME item and returns SENT.
    """

    comptime SENT   = Int(0)
    comptime PARKED = Int(1)

    var _kind: Int

    def __init__(out self, kind: Int):
        self._kind = kind

    def is_sent(self) -> Bool:
        """True when the item was delivered (SENT kind)."""
        return self._kind == Self.SENT

    def is_parked(self) -> Bool:
        """True when the task parked (PARKED kind).  The dispatcher MUST
        return immediately."""
        return self._kind == Self.PARKED

# ---------------------------------------------------------------------------
# Channel[T] — shared ring + wait queues + closed flags
# ---------------------------------------------------------------------------

struct Channel[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable]:
    """Bounded channel (spec §39-41).

    State:
      _guard         — SpinLock (issue #128) serializing every read/write of
                        the fields below — see the module header for why.
      _items         — ring buffer (Deque; bounded by _capacity).
      _send_waiters  — parked senders, FIFO (backpressure).
      _recv_waiters  — parked receivers, FIFO (empty-channel waits).
      _to_wake       — deferred wakes for the embedding driver to drain.
      _send_closed   — send side done (last sender closed).
      _recv_closed   — receive side done (last receiver closed).
      _senders / _receivers — live slot counts (close when the last drops).

    Every mutation of the fields above is a single guarded critical section
    (issue #128): two REAL M:N worker OS threads may call send()/recv()/
    try_send()/try_recv()/close() on the SAME channel concurrently.  A task
    parks ONLY via the two-phase `park_prepare`/`park_validate`/
    `park_commit` kernel on its own handle; every wake is a deferred
    WaitRecord the driver executes via `unpark_current` (no transition
    inside this struct).
    """

    var _guard: SpinLock
    var _items: Deque[Self.T]
    var _capacity: Int
    var _send_waiters: Deque[WaitRecord]
    var _recv_waiters: Deque[WaitRecord]
    var _to_wake: Deque[WaitRecord]
    var _send_closed: Bool
    var _recv_closed: Bool
    var _senders: Int
    var _receivers: Int

    def __init__(out self, capacity: Int) raises:
        if capacity < 1:
            raise Error("ChannelError: capacity must be >= 1, got " + String(capacity))
        self._guard = SpinLock()
        self._items = Deque[Self.T]()
        self._capacity = capacity
        self._send_waiters = Deque[WaitRecord]()
        self._recv_waiters = Deque[WaitRecord]()
        self._to_wake = Deque[WaitRecord]()
        self._send_closed = False
        self._recv_closed = False
        self._senders = 0
        self._receivers = 0

    # --- queries -----------------------------------------------------------

    def capacity(self) -> Int:
        """Immutable after construction (never mutated post-`__init__`) —
        no guard needed, unlike every query below."""
        return self._capacity

    def len(mut self) -> Int:
        self._guard.lock()
        var n = len(self._items)
        self._guard.unlock()
        return n

    def is_empty(mut self) -> Bool:
        self._guard.lock()
        var e = len(self._items) == 0
        self._guard.unlock()
        return e

    def is_full(mut self) -> Bool:
        self._guard.lock()
        var f = len(self._items) >= self._capacity
        self._guard.unlock()
        return f

    def is_send_closed(mut self) -> Bool:
        self._guard.lock()
        var v = self._send_closed
        self._guard.unlock()
        return v

    def is_recv_closed(mut self) -> Bool:
        self._guard.lock()
        var v = self._recv_closed
        self._guard.unlock()
        return v

    def is_closed(mut self) -> Bool:
        self._guard.lock()
        var v = self._send_closed or self._recv_closed
        self._guard.unlock()
        return v

    def sender_count(mut self) -> Int:
        self._guard.lock()
        var v = self._senders
        self._guard.unlock()
        return v

    def receiver_count(mut self) -> Int:
        self._guard.lock()
        var v = self._receivers
        self._guard.unlock()
        return v

    def send_waiters_len(mut self) -> Int:
        self._guard.lock()
        var v = len(self._send_waiters)
        self._guard.unlock()
        return v

    def recv_waiters_len(mut self) -> Int:
        self._guard.lock()
        var v = len(self._recv_waiters)
        self._guard.unlock()
        return v

    def to_wake_len(mut self) -> Int:
        self._guard.lock()
        var v = len(self._to_wake)
        self._guard.unlock()
        return v

    # --- handle split --------------------------------------------------------

    def sender(mut self) raises -> Sender[Self.T]:
        """Split a new Sender handle.  Refuses once the send side is
        closed.  GUARDED (issue #128): the closed check and the slot-count
        increment are ONE critical section."""
        self._guard.lock()
        if self._send_closed:
            self._guard.unlock()
            raise Error("ChannelError: send side already closed; cannot split a sender")
        self._senders += 1
        self._guard.unlock()
        return Sender[Self.T](UnsafePointer[Channel[Self.T], MutAnyOrigin](to=self))

    def receiver(mut self) raises -> Receiver[Self.T]:
        """Split a new Receiver handle.  Refuses once the receive side is
        closed.  GUARDED (issue #128), see `sender`."""
        self._guard.lock()
        if self._recv_closed:
            self._guard.unlock()
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        self._guard.unlock()
        return Receiver[Self.T](UnsafePointer[Channel[Self.T], MutAnyOrigin](to=self))

    # --- non-blocking core (spec §40.1 fast paths) ----------------------------

    def try_send(mut self, item: Self.T) raises -> Bool:
        """Non-blocking send (spec §40.1): buffers when the ring has room;
        returns False when the ring is full OR the channel is closed (the
        item is NOT consumed).  Never parks, never registers a waiter.
        GUARDED (issue #128): the closed/full check and the buffer-append +
        wake are ONE critical section — two separately-guarded calls would
        let a concurrent recv()/close() land between them and corrupt the
        ring or miss a waiter."""
        self._guard.lock()
        if self._recv_closed or self._send_closed:
            self._guard.unlock()
            return False
        if len(self._items) >= self._capacity:
            self._guard.unlock()
            return False
        self._items.append(item)
        if len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)
        self._guard.unlock()
        return True

    def try_recv(mut self) raises -> Optional[Self.T]:
        """Non-blocking receive (spec §40.1): moves the OLDEST buffered value
        out and wakes the OLDEST parked sender (deferred); returns None when
        the ring is empty.  Never parks.  On a closed channel it keeps
        draining buffered values (spec §41) — only emptiness yields None.
        GUARDED (issue #128), see `try_send`."""
        self._guard.lock()
        if len(self._items) > 0:
            var v = self._items.popleft()
            if len(self._send_waiters) > 0:
                var w = self._send_waiters[0]
                _ = self._send_waiters.popleft()
                self._to_wake.append(w)
            self._guard.unlock()
            return Optional[Self.T](v)
        self._guard.unlock()
        return Optional[Self.T]()

    # --- blocking slow paths (spec §40.2) -------------------------------------

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises -> SendOutcome:
        """One-shot send of the current task (issued by a Sender).

        Fast path: ring not full — buffer the item, and if a receiver is
        parked (it parked on an empty ring) wake the OLDEST one (deferred):
        the item is delivered through the ring and the waiter consumes it on
        re-entry.  Slow path: ring full — register this task as a sender
        waiter (FIFO, deduped by id; ONE guarded critical section with the
        fast-path re-check, issue #128 — see the module header), then park
        via the TWO-PHASE `park_prepare`/`park_validate`/`park_commit`
        kernel (issue #128), NOT the single-phase `park_current`, so a
        cross-worker recv()/close() racing into the PARKING window is never
        lost.  A wake carries no ownership transfer (unlike Mutex's GRANT
        marker) — it only means "retry"; an early wake (validate observes
        it before WAITING commits) therefore LOOPS back to the top-of-
        function attempt instead of returning, exactly as a fresh re-
        dispatch of this task would.  A cancel-flavored early wake (issue
        #57's send_cancellable) raises via `raise_if_cancel_wake` right
        there — the same "post-resume winner check on this re-entry"
        park.mojo's with_cancel docstring documents.  On a genuine WAITING
        commit the driver re-enters the task with the SAME pending item;
        if the channel closed meanwhile the re-entry raises here.
        """
        while True:
            self._guard.lock()
            if self._recv_closed or self._send_closed:
                self._guard.unlock()
                raise Error("ChannelError: send on closed channel")
            if len(self._items) < self._capacity:
                self._items.append(item)
                if len(self._recv_waiters) > 0:
                    var w = self._recv_waiters[0]
                    _ = self._recv_waiters.popleft()
                    self._to_wake.append(w)
                self._guard.unlock()
                return SendOutcome(SendOutcome.SENT)
            self._register_sender_locked(h)
            self._guard.unlock()
            park_prepare(h)
            if park_validate(h):
                _ = park_commit(h)
                claim_running(h)
                raise_if_cancel_wake(h)
                continue
            if not park_commit(h):
                claim_running(h)
                raise_if_cancel_wake(h)
                continue
            return SendOutcome(SendOutcome.PARKED)

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> RecvOutcome[Self.T]:
        """One-shot receive of the current task (issued by a Receiver).

        Fast path (§40.1): ring non-empty — move the OLDEST value out and
        wake the OLDEST parked sender (deferred).  Closed-and-empty: return
        None (close observable).  Slow path (§40.2): register this task as a
        receiver waiter (FIFO, deduped by id; ONE guarded critical section,
        issue #128) and park via the TWO-PHASE kernel (see `send`); on an
        early wake, loop back to the top-of-function attempt instead of
        returning — see `send`'s docstring for the full rationale."""
        while True:
            self._guard.lock()
            if len(self._items) > 0:
                var v = self._items.popleft()
                if len(self._send_waiters) > 0:
                    var w = self._send_waiters[0]
                    _ = self._send_waiters.popleft()
                    self._to_wake.append(w)
                self._guard.unlock()
                return RecvOutcome[Self.T](RecvOutcome.VALUE, Optional[Self.T](v))
            if self._send_closed or self._recv_closed:
                self._guard.unlock()
                return RecvOutcome[Self.T](RecvOutcome.CLOSED)
            self._register_receiver_locked(h)
            self._guard.unlock()
            park_prepare(h)
            if park_validate(h):
                _ = park_commit(h)
                claim_running(h)
                raise_if_cancel_wake(h)
                continue
            if not park_commit(h):
                claim_running(h)
                raise_if_cancel_wake(h)
                continue
            return RecvOutcome[Self.T](RecvOutcome.PARKED)

    # --- token-aware slow paths (A4.3, issue #57) ---------------------------

    def send_cancellable[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T, token: CancellationToken,
    ) raises -> SendOutcome:
        """Token-aware send.  Identical to `send()` on every readiness path
        (fast buffer, contended park, re-entry); the ONLY addition is the C6
        winner check this waiter's own resume carries: raises ONLY when
        THIS waiter's `cancel_send_wait` won the race (never when a slot
        opened up first — the ring/queue is left exactly as `send()` would
        leave it, the item NOT consumed twice)."""
        raise_if_cancel_wake(h)
        if token.is_cancellation_requested():
            raise Error("CancellationError: channel send cancelled")
        return self.send(rt, h, item)

    def recv_cancellable[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], token: CancellationToken,
    ) raises -> RecvOutcome[Self.T]:
        """Token-aware receive.  Identical to `recv()` on every readiness
        path; raises ONLY when THIS waiter's `cancel_recv_wait` won the
        race (never when a value arrived first)."""
        raise_if_cancel_wake(h)
        if token.is_cancellation_requested():
            raise Error("CancellationError: channel recv cancelled")
        return self.recv(rt, h)

    def cancel_send_wait[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Cancel a parked `send_cancellable` waiter (issue #57): removes
        `h` from `_send_waiters` and delivers a CANCEL wake.  Returns True
        iff THIS call found + removed + woke the waiter; False when the id
        was never queued or a concurrent fast-path/close already popped it
        (readiness/close won — no ghost entry, no double wake).  The FIFO
        search+removal is GUARDED (issue #128) — see the module header."""
        self._guard.lock()
        var found = _remove_waiter_by_id(self._send_waiters, h.id())
        self._guard.unlock()
        if not found:
            return False
        wake_cancelled(rt, h)
        return True

    def cancel_recv_wait[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        """Cancel a parked `recv_cancellable` waiter (issue #57): removes
        `h` from `_recv_waiters` and delivers a CANCEL wake.  Returns True
        iff THIS call found + removed + woke the waiter; False when the id
        was never queued or a concurrent fast-path/close already popped it.
        GUARDED (issue #128), see `cancel_send_wait`."""
        self._guard.lock()
        var found = _remove_waiter_by_id(self._recv_waiters, h.id())
        self._guard.unlock()
        if not found:
            return False
        wake_cancelled(rt, h)
        return True

    # --- waiter registration (FIFO, dedupe by task id) -------------------------

    def _register_sender_locked[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Caller MUST hold `_guard` (issue #128) — the `send()` slow path
        calls this from within its own already-held critical section; a
        second internal lock here would deadlock the (non-reentrant)
        SpinLock."""
        for i in range(len(self._send_waiters)):
            if self._send_waiters[i].task_id == h.id():
                return  # already parked as a sender; never double-register
        self._send_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    def register_sender[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Standalone GUARDED entry point (issue #128) for external callers
        (select.mojo registers a BLOCKED branch directly, outside any
        send()/recv() call of its own)."""
        self._guard.lock()
        self._register_sender_locked(h)
        self._guard.unlock()

    def _register_receiver_locked[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Caller MUST hold `_guard` (issue #128), see
        `_register_sender_locked`."""
        for i in range(len(self._recv_waiters)):
            if self._recv_waiters[i].task_id == h.id():
                return  # already parked as a receiver; never double-register
        self._recv_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    def register_receiver[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Standalone GUARDED entry point (issue #128), see
        `register_sender`."""
        self._guard.lock()
        self._register_receiver_locked(h)
        self._guard.unlock()

    def unregister_sender(mut self, task_id: Int) raises:
        """Logical cancellation (A5.4/A5.5 select, issues #92/#93): drop a
        parked sender waiter by task id, if present.  A `select` LOSING
        branch must vacate every OTHER branch's wait queue once a winner is
        claimed elsewhere, so a later real wake on this channel never finds
        (and double-enqueues) a stale WaitRecord for a task that already
        resumed through a different branch.  No-op when `task_id` is not
        currently registered here (idempotent, matches register_* dedupe).
        GUARDED (issue #128): the scan+rebuild is ONE critical section — a
        concurrent send()/recv() on another worker must never observe a
        partially-rebuilt `_send_waiters`."""
        self._guard.lock()
        var kept = Deque[WaitRecord]()
        while len(self._send_waiters) > 0:
            var w = self._send_waiters[0]
            _ = self._send_waiters.popleft()
            if w.task_id != task_id:
                kept.append(w)
        self._send_waiters = kept^
        self._guard.unlock()

    def unregister_receiver(mut self, task_id: Int) raises:
        """Logical cancellation (A5.4/A5.5 select, issues #92/#93): drop a
        parked receiver waiter by task id, if present.  See
        unregister_sender for the rationale (select's losing branches).
        GUARDED (issue #128), see `unregister_sender`."""
        self._guard.lock()
        var kept = Deque[WaitRecord]()
        while len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            if w.task_id != task_id:
                kept.append(w)
        self._recv_waiters = kept^
        self._guard.unlock()

    # --- deferred wakes (driver drains these) ----------------------------------

    def pop_to_wake(mut self) raises -> WaitRecord:
        """Pop the oldest deferred wake.  Returns the (0,0) sentinel record
        when the list is empty (Deque.pop on empty raises; a sentinel keeps
        the driver drain total).  GUARDED (issue #128): the embedding
        driver may run on a DIFFERENT worker than the one whose send()/
        recv() pushed the wake."""
        self._guard.lock()
        if len(self._to_wake) == 0:
            self._guard.unlock()
            return WaitRecord(0, 0)
        var w = self._to_wake[0]
        _ = self._to_wake.popleft()
        self._guard.unlock()
        return w

    # --- close slots (spec §41) ------------------------------------------------

    def close_sender_slot(mut self) raises:
        """One Sender dropped its slot.  When the LAST sender closes, the
        send side closes and every parked receiver is moved to the deferred
        wake list: they drain the remaining buffered values, then recv()
        returns None.  Idempotent.  GUARDED (issue #128): the slot-count
        decrement, the closed-flag flip, and the FIFO drain are ONE
        critical section — see the module header."""
        self._guard.lock()
        if self._senders > 0:
            self._senders -= 1
        if self._senders != 0:
            self._guard.unlock()
            return
        if self._send_closed:
            self._guard.unlock()
            return
        self._send_closed = True
        while len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)
        self._guard.unlock()

    def close_receiver_slot(mut self) raises:
        """One Receiver dropped its slot.  When the LAST receiver closes, the
        receive side closes, the buffered values are dropped, and every
        parked sender is moved to the deferred wake list: their resumed
        send() raises "ChannelError: send on closed channel".  Idempotent.
        GUARDED (issue #128), see `close_sender_slot`."""
        self._guard.lock()
        if self._receivers > 0:
            self._receivers -= 1
        if self._receivers != 0:
            self._guard.unlock()
            return
        if self._recv_closed:
            self._guard.unlock()
            return
        self._recv_closed = True
        while len(self._items) > 0:
            _ = self._items.popleft()
        while len(self._send_waiters) > 0:
            var w = self._send_waiters[0]
            _ = self._send_waiters.popleft()
            self._to_wake.append(w)
        self._guard.unlock()


# ---------------------------------------------------------------------------
# Sender / Receiver — light handles over the shared Channel cell
# ---------------------------------------------------------------------------

struct Sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Send half of a bounded channel (spec §39).  Single-owner BY CONVENTION
    (b2 handles are implicitly copyable): exactly one handle per slot calls
    close(); the channel's slot count tracks live senders so the send side
    closes when the LAST sender closes (spec §41).  `_closed` latches the
    handle-level close: after close(), this handle stops sending and its
    second close() is a no-op."""

    var _chan: UnsafePointer[Channel[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[Channel[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[Channel[Self.T], MutAnyOrigin]:
        return self._chan

    def is_closed(self) -> Bool:
        return self._closed

    def try_send(mut self, item: Self.T) raises -> Bool:
        if self._closed:
            return False
        return self._chan[].try_send(item)

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises -> SendOutcome:
        if self._closed:
            raise Error("ChannelError: send on closed channel")
        return self._chan[].send(rt, h, item)

    def send_cancellable[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T, token: CancellationToken,
    ) raises -> SendOutcome:
        if self._closed:
            raise Error("ChannelError: send on closed channel")
        return self._chan[].send_cancellable(rt, h, item, token)

    def cancel_send_wait[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        return self._chan[].cancel_send_wait(rt, h)

    def close(mut self) raises:
        """Drop this sender's slot.  When the LAST sender closes, the send
        side closes and parked receivers are woken (they drain, then observe
        close).  Idempotent per handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_sender_slot()


struct Receiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Receive half of a bounded channel (spec §39).  Receiver.close() drops
    the slot; when the LAST receiver closes, the receive side closes, parked
    senders are woken, and subsequent sends fail (spec §41)."""

    var _chan: UnsafePointer[Channel[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[Channel[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[Channel[Self.T], MutAnyOrigin]:
        return self._chan

    def is_closed(self) -> Bool:
        return self._closed

    def try_recv(mut self) raises -> Optional[Self.T]:
        return self._chan[].try_recv()

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> RecvOutcome[Self.T]:
        return self._chan[].recv(rt, h)

    def recv_cancellable[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], token: CancellationToken,
    ) raises -> RecvOutcome[Self.T]:
        return self._chan[].recv_cancellable(rt, h, token)

    def cancel_recv_wait[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Bool:
        return self._chan[].cancel_recv_wait(rt, h)

    def close(mut self) raises:
        """Drop this receiver's slot.  When the LAST receiver closes, the
        receive side closes, buffered values are dropped, and parked senders
        are woken (their resumed send raises).  Idempotent per handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_receiver_slot()


# ---------------------------------------------------------------------------
# Module factories (b2: no static methods)
# ---------------------------------------------------------------------------

def make_channel[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    capacity: Int,
) raises -> Channel[T]:
    """Fresh bounded channel with `capacity` slots (>= 1)."""
    return Channel[T](capacity)


def make_sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[Channel[T], MutAnyOrigin],
) raises -> Sender[T]:
    """Hand a Sender handle over an existing channel cell."""
    return chan[].sender()


def make_receiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[Channel[T], MutAnyOrigin],
) raises -> Receiver[T]:
    """Hand a Receiver handle over an existing channel cell."""
    return chan[].receiver()
