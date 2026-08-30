# mojito_async/channel/unbounded.mojo
#
# A5.2 unbounded channel (issue #90) — UnboundedChannel[T]: unbounded Deque
# store + receiver wait queue + closed flags (spec §39-41), Sender/Receiver
# split, close semantics (spec §41).  Mirrors the landed bounded Channel[T]
# (channel/channel.mojo, issue #35) verbatim for the WaitRecord / deferred-
# wake / close choreography, minus the capacity-based backpressure: a
# sender never blocks on buffer capacity, only receivers wait on an empty
# channel.
#
# Mojo 1.0.0b2 design notes (inherited from the bounded channel's verified
# lane probes):
#   - `def`-only, module factories (no static methods), no hidden allocation
#     on the fast paths (the store and the receiver wait queue are
#     caller-owned Deques; parks allocate nothing).
#   - The UnboundedChannel struct is PURE DATA: an unbounded Deque[T] store,
#     a FIFO receiver wait queue (WaitRecord = tcb_addr + task_id, reused
#     verbatim from channel/channel.mojo), a deferred wake list (_to_wake),
#     the closed flags and the live slot counts.  It NEVER reconstructs
#     task handles from raw addresses inside a method (b2 miscompiles
#     `unsafe_from_address` reconstruction inside generic struct methods).
#     Parks use the TWO-PHASE `park_prepare`/`park_validate`/`park_commit`
#     kernel (see the cross-worker safety note below) on the PASSED-IN
#     JoinHandle of the CURRENT task; wakes are DEFERRED: a signal moves a
#     WaitRecord into `_to_wake`, and the embedding DRIVER drains
#     `_to_wake` and resumes each waiter via `unpark_current` (the canonical
#     wake path, single source, issue #39).
#   - send/recv are one-shot colorless operations: attempt now; recv
#     registers a waiter and parks when the store is empty.  On resume the
#     driver re-enters the task, which re-invokes recv() (A1.1 has no
#     fibers).  register_receiver dedupes by task_id.
#   - Algorithm: a sender always buffers (no capacity bound) and, per
#     buffered item, wakes the OLDEST parked receiver (FIFO).  There is NO
#     `_send_waiters` queue: a sender never parks, so nothing ever
#     populates it.  Close (spec §41): closing the LAST sender marks the
#     send side closed and moves EVERY parked receiver into `_to_wake` —
#     they drain the remaining values, then recv() returns None (closed
#     observable).  Closing the LAST receiver marks the receive side
#     closed and drops the buffered values; subsequent send() raises
#     "ChannelError: send on closed channel" (there is no blocked-sender
#     wake step — no sender is ever parked).
#
# Cross-worker safety (issue #128, mirrors channel.mojo's treatment):
# `_guard` is a SpinLock (queue.mojo's) serializing every read/write of
# `_items`/`_recv_waiters`/`_to_wake`/the closed flags/the slot counts —
# see channel.mojo's module header for the full rationale (the same
# double-acquisition/lost-update class t38_mutex_cross_worker_aot caught
# for Mutex).  recv()'s slow path now parks via the TWO-PHASE kernel
# instead of the single-phase `park_current`; an early wake LOOPS back to
# the top-of-function attempt (a channel wake carries no ownership, unlike
# Mutex's GRANT marker — see channel.mojo's `send`/`recv` docstrings for
# the full rationale).  send() never parks here, so it needs no two-phase
# treatment — only its guarded critical section.  There are no cancellable
# variants on this struct (unlike channel.mojo), so no cancel-wake check is
# needed on the early-unwind path.  The SpinLock's Atomic cannot be
# Movable/ImplicitlyDeletable, so UnboundedChannel drops those
# conformances too — it now mirrors Channel/Mutex/RWLock.
from std.collections import Deque
from mojito_async.channel.channel import WaitRecord, RecvOutcome, SendOutcome
from mojito_async.runtime.queue import SpinLock
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle, claim_running
from mojito_async.runtime.park import park_commit, park_prepare, park_validate


# ---------------------------------------------------------------------------
# UnboundedChannel[T] — shared unbounded store + receiver wait queue + closed
# flags
# ---------------------------------------------------------------------------

struct UnboundedChannel[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable]:
    """Unbounded channel (spec §39-41, issue #90).

    State:
      _guard         — SpinLock (issue #128) serializing every read/write of
                        the fields below — see the module header.
      _items         — unbounded store (Deque; no capacity bound).
      _recv_waiters  — parked receivers, FIFO (empty-channel waits).
      _to_wake       — deferred wakes for the embedding driver to drain.
      _send_closed   — send side done (last sender closed).
      _recv_closed   — receive side done (last receiver closed).
      _senders / _receivers — live slot counts (close when the last drops).

    There is no `_send_waiters` queue and no `_capacity`: a sender never
    parks on backpressure, so no field ever needs to record one.  Every
    mutation of the fields above is a single guarded critical section
    (issue #128): two REAL M:N worker OS threads may call send()/recv()/
    try_send()/try_recv()/close() on the SAME channel concurrently.  A task
    parks ONLY via the two-phase park kernel on its own handle; every wake
    is a deferred WaitRecord the driver executes via `unpark_current` (no
    transition inside this struct).
    """

    var _guard: SpinLock
    var _items: Deque[Self.T]
    var _recv_waiters: Deque[WaitRecord]
    var _to_wake: Deque[WaitRecord]
    var _send_closed: Bool
    var _recv_closed: Bool
    var _senders: Int
    var _receivers: Int

    def __init__(out self):
        self._guard = SpinLock()
        self._items = Deque[Self.T]()
        self._recv_waiters = Deque[WaitRecord]()
        self._to_wake = Deque[WaitRecord]()
        self._send_closed = False
        self._recv_closed = False
        self._senders = 0
        self._receivers = 0

    # --- queries -----------------------------------------------------------

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

    def send_waiters_len(self) -> Int:
        """Always 0 — a sender never parks, so `_send_waiters` does not
        exist here.  Kept for API symmetry with the bounded Channel so
        acceptance drivers can assert the same "zero leftovers" shape.  No
        guard needed: a compile-time constant, not shared mutable state."""
        return 0

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
        return Sender[Self.T](UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin](to=self))

    def receiver(mut self) raises -> Receiver[Self.T]:
        """Split a new Receiver handle.  Refuses once the receive side is
        closed.  GUARDED (issue #128), see `sender`."""
        self._guard.lock()
        if self._recv_closed:
            self._guard.unlock()
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        self._guard.unlock()
        return Receiver[Self.T](UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin](to=self))

    # --- non-blocking core (fast paths) ---------------------------------------

    def try_send(mut self, item: Self.T) raises -> Bool:
        """Non-blocking send: always consumes the item while the channel is
        open — there is no capacity to fill.  Returns False only when the
        channel is closed (the item is NOT consumed).  Never parks, never
        registers a waiter.  GUARDED (issue #128): the closed check, the
        append, and the wake are ONE critical section."""
        self._guard.lock()
        if self._recv_closed or self._send_closed:
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
        """Non-blocking receive: moves the OLDEST buffered value out, or
        returns None when the store is empty.  Never parks.  On a closed
        channel it keeps draining buffered values (spec §41) — only
        emptiness yields None.  No sender waiters exist to wake.  GUARDED
        (issue #128)."""
        self._guard.lock()
        if len(self._items) > 0:
            var v = self._items.popleft()
            self._guard.unlock()
            return Optional[Self.T](v)
        self._guard.unlock()
        return Optional[Self.T]()

    # --- blocking slow path (recv only — send never blocks) ------------------

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises -> SendOutcome:
        """One-shot send of the current task (issued by a Sender).  Always
        buffers — an unbounded channel never applies backpressure — and, if
        a receiver is parked (it parked on an empty store), wakes the
        OLDEST one (deferred): the item is delivered through the store and
        the waiter consumes it on re-entry.  Raises on a closed channel;
        never parks.  GUARDED (issue #128): the closed check, the append,
        and the wake are ONE critical section — see `try_send`."""
        self._guard.lock()
        if self._recv_closed or self._send_closed:
            self._guard.unlock()
            raise Error("ChannelError: send on closed channel")
        self._items.append(item)
        if len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)
        self._guard.unlock()
        return SendOutcome(SendOutcome.SENT)

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> RecvOutcome[Self.T]:
        """One-shot receive of the current task (issued by a Receiver).

        Fast path: store non-empty — move the OLDEST value out (no sender
        waiters exist to wake).  Closed-and-empty: return None (close
        observable).  Slow path: register this task as a receiver waiter
        (FIFO, deduped by id; ONE guarded critical section with the fast-
        path re-check, issue #128) and park via the TWO-PHASE
        `park_prepare`/`park_validate`/`park_commit` kernel, NOT the
        single-phase `park_current`, so a cross-worker send()/close() racing
        into the PARKING window is never lost.  A wake carries no ownership
        transfer — it only means "retry"; an early wake LOOPS back to the
        top-of-function attempt instead of returning (see channel.mojo's
        `recv` docstring for the full rationale — this struct has no
        cancellable variant, so no cancel-wake check is needed here).  On a
        genuine WAITING commit the driver re-enters and the fresh call
        takes the fast path or observes close.
        """
        while True:
            self._guard.lock()
            if len(self._items) > 0:
                var v = self._items.popleft()
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
                continue
            if not park_commit(h):
                claim_running(h)
                continue
            return RecvOutcome[Self.T](RecvOutcome.PARKED)

    # --- waiter registration (FIFO, dedupe by task id) -------------------------

    def _register_receiver_locked[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Caller MUST hold `_guard` (issue #128) — `recv()`'s slow path
        calls this from within its own already-held critical section; a
        second internal lock here would deadlock the (non-reentrant)
        SpinLock."""
        for i in range(len(self._recv_waiters)):
            if self._recv_waiters[i].task_id == h.id():
                return  # already parked as a receiver; never double-register
        self._recv_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    def register_receiver[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Standalone GUARDED entry point (issue #128) for external callers
        that register a waiter outside a recv() call of their own."""
        self._guard.lock()
        self._register_receiver_locked(h)
        self._guard.unlock()

    # --- deferred wakes (driver drains these) ----------------------------------

    def pop_to_wake(mut self) raises -> WaitRecord:
        """Pop the oldest deferred wake.  Returns the (0,0) sentinel record
        when the list is empty (mirrors #35's Channel.pop_to_wake).
        GUARDED (issue #128): the embedding driver may run on a DIFFERENT
        worker than the one whose send()/recv() pushed the wake."""
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
        critical section."""
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
        """One Receiver dropped its slot.  When the LAST receiver closes,
        the receive side closes and the buffered values are dropped.
        There is no blocked-sender wake step: a sender never parks on an
        unbounded channel, so `_send_waiters` never has anything to move —
        a subsequent send() simply observes `_recv_closed` and raises
        "ChannelError: send on closed channel".  Idempotent.  GUARDED
        (issue #128)."""
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
        self._guard.unlock()


# ---------------------------------------------------------------------------
# Sender / Receiver — light handles over the shared UnboundedChannel cell
# ---------------------------------------------------------------------------

struct Sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Send half of an unbounded channel (spec §39).  Single-owner BY
    CONVENTION (b2 handles are implicitly copyable): exactly one handle per
    slot calls close(); the channel's slot count tracks live senders so the
    send side closes when the LAST sender closes (spec §41).  `_closed`
    latches the handle-level close: after close(), this handle stops
    sending and its second close() is a no-op."""

    var _chan: UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin]:
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
    """Receive half of an unbounded channel (spec §39).  Receiver.close()
    drops the slot; when the LAST receiver closes, the receive side closes,
    buffered values are dropped, and subsequent sends fail (spec §41)."""

    var _chan: UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin]:
        return self._chan

    def is_closed(self) -> Bool:
        return self._closed

    def try_recv(mut self) raises -> Optional[Self.T]:
        return self._chan[].try_recv()

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> RecvOutcome[Self.T]:
        return self._chan[].recv(rt, h)

    def close(mut self) raises:
        """Drop this receiver's slot.  When the LAST receiver closes, the
        receive side closes and buffered values are dropped.  Idempotent
        per handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_receiver_slot()


# ---------------------------------------------------------------------------
# Module factories (b2: no static methods)
# ---------------------------------------------------------------------------

def make_unbounded[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable]() raises -> UnboundedChannel[T]:
    """Fresh unbounded channel: no capacity bound."""
    return UnboundedChannel[T]()


def make_unbounded_sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[UnboundedChannel[T], MutAnyOrigin],
) raises -> Sender[T]:
    """Hand a Sender handle over an existing channel cell."""
    return chan[].sender()


def make_unbounded_receiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[UnboundedChannel[T], MutAnyOrigin],
) raises -> Receiver[T]:
    """Hand a Receiver handle over an existing channel cell."""
    return chan[].receiver()
