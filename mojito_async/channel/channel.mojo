# mojito_async/channel/channel.mojo
#
# A1.3 channel (issue #35) — bounded Channel[T]: ring buffer + sender/receiver
# wait queues + closed flags (spec §39-41).
#
# TDD RED SCAFFOLD: the full public surface is present and typed exactly as
# the implementation commit will ship it; every operation raises a precise
# "not implemented" error so the acceptance suites compile, run, and print
# RED against this scaffold.  The implementation commit replaces the bodies
# (never the signatures).
#
# Design notes (b2 — Mojo 1.0.0b2):
#   - `def`-only, module factories (no static methods).
#   - The Channel struct is PURE DATA: ring Deque[T], sender/receiver wait
#     queues, a deferred wake list, and the closed flags/slot counts.  It
#     NEVER reconstructs task handles from raw addresses — the b2 compiler
#     miscompiles `unsafe_from_address` reconstruction inside generic struct
#     methods (verified by probe).  Parks use the canonical A1.1
#     `_suspend_current(mut rt, h)` on the PASSED-IN handle; wakes are
#     DEFERRED: a signal moves a WaitRecord (tcb_addr + task_id) from a wait
#     queue into `_to_wake`, and the embedding DRIVER (a plain, concrete
#     function that knows the task's result type) drains `_to_wake` and
#     resumes each waiter via `resume_current` (the canonical wake path,
#     issue #39 single-source park/wake).
#   - send()/recv() are one-shot colorless operations: attempt now; if the
#     channel forces a wait, register the waiter and park ONCE
#     (`_suspend_current`, no OS-thread block).  On resume the embedding
#     driver re-enters the task, which re-invokes send()/recv() with the same
#     pending item (there are no fibers in A1.1 — suspension happens between
#     dispatcher slices).  register_* dedupe by task_id.
#   - Close semantics (spec §41): closing the LAST sender marks the send side
#     closed and moves EVERY parked receiver into `_to_wake` (they drain the
#     buffer, then recv() returns None); closing the LAST receiver marks the
#     receive side closed, drops buffered values, moves every parked sender
#     into `_to_wake` (their send() then raises "ChannelError: send on closed
#     channel").
from std.collections import Deque
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import _suspend_current
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle


# ---------------------------------------------------------------------------
# WaitRecord — type-erased waiter identity (DATA, not code)
# ---------------------------------------------------------------------------

struct WaitRecord(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Identity of a parked task: address of its caller-allocated TCB cell +
    scheduler id.  The embedding driver (which knows the task's concrete
    result type) reconstructs the JoinHandle from these two ints and resumes
    it via the canonical resume_current."""

    var tcb_addr: Int
    var task_id: Int

    def __init__(out self):
        self.tcb_addr = 0
        self.task_id = 0

    def __init__(out self, tcb_addr: Int, task_id: Int):
        self.tcb_addr = tcb_addr
        self.task_id = task_id


# ---------------------------------------------------------------------------
# Channel[T] — shared ring + wait queues + closed flags
# ---------------------------------------------------------------------------

struct Channel[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyDeletable, Movable
):
    """Bounded channel (spec §39-41).

    State:
      _items         — ring buffer (Deque; bounded by _capacity).
      _send_waiters  — parked senders, FIFO (backpressure).
      _recv_waiters  — parked receivers, FIFO (empty-channel waits).
      _to_wake       — deferred wakes for the embedding driver to drain.
      _send_closed   — send side done (last sender closed).
      _recv_closed   — receive side done (last receiver closed).
      _senders / _receivers — live slot counts (close when the last drops).

    Single worker, fully deterministic: no mutex, no atomics; the wait
    queues and buffer are only mutated by the one running worker slice.
    """

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
        return self._capacity

    def len(self) -> Int:
        return len(self._items)

    def is_empty(self) -> Bool:
        return len(self._items) == 0

    def is_full(self) -> Bool:
        return len(self._items) >= self._capacity

    def is_send_closed(self) -> Bool:
        return self._send_closed

    def is_recv_closed(self) -> Bool:
        return self._recv_closed

    def is_closed(self) -> Bool:
        return self._send_closed or self._recv_closed

    def sender_count(self) -> Int:
        return self._senders

    def receiver_count(self) -> Int:
        return self._receivers

    def send_waiters_len(self) -> Int:
        return len(self._send_waiters)

    def recv_waiters_len(self) -> Int:
        return len(self._recv_waiters)

    def to_wake_len(self) -> Int:
        return len(self._to_wake)

    # --- handle split --------------------------------------------------------

    def sender(mut self) raises -> Sender[Self.T]:
        if self._send_closed:
            raise Error("ChannelError: send side already closed; cannot split a sender")
        self._senders += 1
        return Sender[Self.T](UnsafePointer[Channel[Self.T], MutAnyOrigin](to=self))

    def receiver(mut self) raises -> Receiver[Self.T]:
        if self._recv_closed:
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        return Receiver[Self.T](UnsafePointer[Channel[Self.T], MutAnyOrigin](to=self))

    # --- non-blocking core (spec §40.1 fast paths) ----------------------------

    def try_send(mut self, item: Self.T) raises -> Bool:
        raise Error("ChannelError: try_send not implemented yet (A1.3 TDD red)")

    def try_recv(mut self) raises -> Optional[Self.T]:
        raise Error("ChannelError: try_recv not implemented yet (A1.3 TDD red)")

    # --- blocking slow paths (spec §40.2) -------------------------------------

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises:
        raise Error("ChannelError: send not implemented yet (A1.3 TDD red)")

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Optional[Self.T]:
        raise Error("ChannelError: recv not implemented yet (A1.3 TDD red)")

    # --- waiter registration (dedupe by task id) ------------------------------

    def register_sender[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        raise Error("ChannelError: register_sender not implemented yet (A1.3 TDD red)")

    def register_receiver[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        raise Error("ChannelError: register_receiver not implemented yet (A1.3 TDD red)")

    # --- deferred wakes (driver drains these) ----------------------------------

    def pop_to_wake(mut self) raises -> WaitRecord:
        raise Error("ChannelError: pop_to_wake not implemented yet (A1.3 TDD red)")

    # --- close slots (spec §41) ------------------------------------------------

    def close_sender_slot(mut self) raises:
        raise Error("ChannelError: close_sender_slot not implemented yet (A1.3 TDD red)")

    def close_receiver_slot(mut self) raises:
        raise Error("ChannelError: close_receiver_slot not implemented yet (A1.3 TDD red)")


# ---------------------------------------------------------------------------
# Sender / Receiver — light handles over the shared Channel cell
# ---------------------------------------------------------------------------

struct Sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Send half of a bounded channel (spec §39).  Single-owner BY CONVENTION
    (b2 handles are implicitly copyable): exactly one handle per slot calls
    close(); the channel's slot count tracks live senders so the send side
    closes when the LAST sender closes (spec §41)."""

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
    ) raises:
        if self._closed:
            raise Error("ChannelError: send on closed channel")
        self._chan[].send(rt, h, item)

    def close(mut self) raises:
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
    ) raises -> Optional[Self.T]:
        return self._chan[].recv(rt, h)

    def close(mut self) raises:
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