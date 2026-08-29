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
#     Parks use the canonical A1.1 `park_current(mut rt, h)` on the
#     PASSED-IN JoinHandle of the CURRENT task; wakes are DEFERRED: a signal
#     moves a WaitRecord into `_to_wake`, and the embedding DRIVER drains
#     `_to_wake` and resumes each waiter via `unpark_current` (the canonical
#     wake path, single source, issue #39).
#   - send/recv are one-shot colorless operations: attempt now; recv
#     registers a waiter and parks ONCE when the store is empty.  On resume
#     the driver re-enters the task, which re-invokes recv() (A1.1 has no
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
from std.collections import Deque
from mojito_async.channel.channel import WaitRecord
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle
from mojito_async.runtime.park import park_current


# ---------------------------------------------------------------------------
# UnboundedChannel[T] — shared unbounded store + receiver wait queue + closed
# flags
# ---------------------------------------------------------------------------

struct UnboundedChannel[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyDeletable, Movable
):
    """Unbounded channel (spec §39-41, issue #90).

    State:
      _items         — unbounded store (Deque; no capacity bound).
      _recv_waiters  — parked receivers, FIFO (empty-channel waits).
      _to_wake       — deferred wakes for the embedding driver to drain.
      _send_closed   — send side done (last sender closed).
      _recv_closed   — receive side done (last receiver closed).
      _senders / _receivers — live slot counts (close when the last drops).

    There is no `_send_waiters` queue and no `_capacity`: a sender never
    parks on backpressure, so no field ever needs to record one.  Single
    worker, fully deterministic: no mutex, no atomics; the wait queue and
    store are only mutated by the one running worker slice.  A task parks
    ONLY via the canonical `park_current` on its own handle; every wake is
    a deferred WaitRecord the driver executes via `unpark_current` (no
    transition inside this struct).
    """

    var _items: Deque[Self.T]
    var _recv_waiters: Deque[WaitRecord]
    var _to_wake: Deque[WaitRecord]
    var _send_closed: Bool
    var _recv_closed: Bool
    var _senders: Int
    var _receivers: Int

    def __init__(out self):
        self._items = Deque[Self.T]()
        self._recv_waiters = Deque[WaitRecord]()
        self._to_wake = Deque[WaitRecord]()
        self._send_closed = False
        self._recv_closed = False
        self._senders = 0
        self._receivers = 0

    # --- queries -----------------------------------------------------------

    def len(self) -> Int:
        return len(self._items)

    def is_empty(self) -> Bool:
        return len(self._items) == 0

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
        """Always 0 — a sender never parks, so `_send_waiters` does not
        exist here.  Kept for API symmetry with the bounded Channel so
        acceptance drivers can assert the same "zero leftovers" shape."""
        return 0

    def recv_waiters_len(self) -> Int:
        return len(self._recv_waiters)

    def to_wake_len(self) -> Int:
        return len(self._to_wake)

    # --- handle split --------------------------------------------------------

    def sender(mut self) raises -> Sender[Self.T]:
        """Split a new Sender handle.  Refuses once the send side is closed."""
        if self._send_closed:
            raise Error("ChannelError: send side already closed; cannot split a sender")
        self._senders += 1
        return Sender[Self.T](UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin](to=self))

    def receiver(mut self) raises -> Receiver[Self.T]:
        """Split a new Receiver handle.  Refuses once the receive side is
        closed."""
        if self._recv_closed:
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        return Receiver[Self.T](UnsafePointer[UnboundedChannel[Self.T], MutAnyOrigin](to=self))

    # --- non-blocking core (fast paths) ---------------------------------------

    def try_send(mut self, item: Self.T) raises -> Bool:
        """Non-blocking send: always consumes the item while the channel is
        open — there is no capacity to fill.  Returns False only when the
        channel is closed (the item is NOT consumed).  Never parks, never
        registers a waiter."""
        if self._recv_closed or self._send_closed:
            return False
        self._items.append(item)
        if len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)
        return True

    def try_recv(mut self) raises -> Optional[Self.T]:
        """Non-blocking receive: moves the OLDEST buffered value out, or
        returns None when the store is empty.  Never parks.  On a closed
        channel it keeps draining buffered values (spec §41) — only
        emptiness yields None.  No sender waiters exist to wake."""
        if len(self._items) > 0:
            var v = self._items.popleft()
            return Optional[Self.T](v)
        return Optional[Self.T]()

    # --- blocking slow path (recv only — send never blocks) ------------------

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises:
        """One-shot send of the current task (issued by a Sender).  Always
        buffers — an unbounded channel never applies backpressure — and, if
        a receiver is parked (it parked on an empty store), wakes the
        OLDEST one (deferred): the item is delivered through the store and
        the waiter consumes it on re-entry.  Raises on a closed channel;
        never parks."""
        if self._recv_closed or self._send_closed:
            raise Error("ChannelError: send on closed channel")
        self._items.append(item)
        if len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Optional[Self.T]:
        """One-shot receive of the current task (issued by a Receiver).

        Fast path: store non-empty — move the OLDEST value out (no sender
        waiters exist to wake).  Closed-and-empty: return None (close
        observable).  Slow path: register this task as a receiver waiter
        (FIFO, deduped by id) and park via the canonical `park_current`; on
        resume the driver re-enters and the fresh call takes the fast path
        or observes close.
        """
        if len(self._items) > 0:
            var v = self._items.popleft()
            return Optional[Self.T](v)
        if self._send_closed or self._recv_closed:
            return Optional[Self.T]()
        self.register_receiver(h)
        park_current(rt, h)
        return Optional[Self.T]()

    # --- waiter registration (FIFO, dedupe by task id) -------------------------

    def register_receiver[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        for i in range(len(self._recv_waiters)):
            if self._recv_waiters[i].task_id == h.id():
                return  # already parked as a receiver; never double-register
        self._recv_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    # --- deferred wakes (driver drains these) ----------------------------------

    def pop_to_wake(mut self) raises -> WaitRecord:
        """Pop the oldest deferred wake.  Returns the (0,0) sentinel record
        when the list is empty (Deque.pop on empty raises; a sentinel keeps
        the driver drain total)."""
        if len(self._to_wake) == 0:
            return WaitRecord(0, 0)
        var w = self._to_wake[0]
        _ = self._to_wake.popleft()
        return w

    # --- close slots (spec §41) ------------------------------------------------

    def close_sender_slot(mut self) raises:
        """One Sender dropped its slot.  When the LAST sender closes, the
        send side closes and every parked receiver is moved to the deferred
        wake list: they drain the remaining buffered values, then recv()
        returns None.  Idempotent."""
        if self._senders > 0:
            self._senders -= 1
        if self._senders != 0:
            return
        if self._send_closed:
            return
        self._send_closed = True
        while len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)

    def close_receiver_slot(mut self) raises:
        """One Receiver dropped its slot.  When the LAST receiver closes,
        the receive side closes and the buffered values are dropped.
        There is no blocked-sender wake step: a sender never parks on an
        unbounded channel, so `_send_waiters` never has anything to move —
        a subsequent send() simply observes `_recv_closed` and raises
        "ChannelError: send on closed channel".  Idempotent."""
        if self._receivers > 0:
            self._receivers -= 1
        if self._receivers != 0:
            return
        if self._recv_closed:
            return
        self._recv_closed = True
        while len(self._items) > 0:
            _ = self._items.popleft()


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
    ) raises:
        if self._closed:
            raise Error("ChannelError: send on closed channel")
        self._chan[].send(rt, h, item)

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
    ) raises -> Optional[Self.T]:
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
