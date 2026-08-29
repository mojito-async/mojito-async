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
#     Parks use the canonical A1.1 `park_current(mut rt, h)` on the
#     PASSED-IN JoinHandle of the CURRENT task; wakes are DEFERRED: a signal
#     moves a WaitRecord into `_to_wake`, and the embedding DRIVER (a plain
#     concrete function that knows the task's result type) drains `_to_wake`
#     and resumes each waiter via `unpark_current` — the canonical wake path
#     (single source, issue #39).
#   - send/recv are one-shot colorless operations: attempt now; if the
#     channel forces a wait, register the waiter and park ONCE.  On resume
#     the driver re-enters the task, which re-invokes send()/recv() with the
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
from std.collections import Deque
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle
from mojito_async.runtime.park import park_current


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
    queues and buffer are only mutated by the one running worker slice.  A
    task parks ONLY via the canonical `park_current` on its own handle;
    every wake is a deferred WaitRecord the driver executes via
    `unpark_current` (no transition inside this struct).
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
        """Split a new Sender handle.  Refuses once the send side is closed."""
        if self._send_closed:
            raise Error("ChannelError: send side already closed; cannot split a sender")
        self._senders += 1
        return Sender[Self.T](UnsafePointer[Channel[Self.T], MutAnyOrigin](to=self))

    def receiver(mut self) raises -> Receiver[Self.T]:
        """Split a new Receiver handle.  Refuses once the receive side is
        closed."""
        if self._recv_closed:
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        return Receiver[Self.T](UnsafePointer[Channel[Self.T], MutAnyOrigin](to=self))

    # --- non-blocking core (spec §40.1 fast paths) ----------------------------

    def try_send(mut self, item: Self.T) raises -> Bool:
        """Non-blocking send (spec §40.1): buffers when the ring has room;
        returns False when the ring is full OR the channel is closed (the
        item is NOT consumed).  Never parks, never registers a waiter."""
        if self._recv_closed or self._send_closed:
            return False
        if self.is_full():
            return False
        self._items.append(item)
        if len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)
        return True

    def try_recv(mut self) raises -> Optional[Self.T]:
        """Non-blocking receive (spec §40.1): moves the OLDEST buffered value
        out and wakes the OLDEST parked sender (deferred); returns None when
        the ring is empty.  Never parks.  On a closed channel it keeps
        draining buffered values (spec §41) — only emptiness yields None."""
        if len(self._items) > 0:
            var v = self._items.popleft()
            if len(self._send_waiters) > 0:
                var w = self._send_waiters[0]
                _ = self._send_waiters.popleft()
                self._to_wake.append(w)
            return Optional[Self.T](v)
        return Optional[Self.T]()

    # --- blocking slow paths (spec §40.2) -------------------------------------

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises:
        """One-shot send of the current task (issued by a Sender).

        Fast path: ring not full — buffer the item, and if a receiver is
        parked (it parked on an empty ring) wake the OLDEST one (deferred):
        the item is delivered through the ring and the waiter consumes it on
        re-entry.  Slow path: ring full — register this task as a sender
        waiter (FIFO, deduped by id), then park via the canonical
        `park_current`.  On resume the driver re-enters the task with the
        SAME pending item; if the channel closed meanwhile the re-entry
        raises here.
        """
        if self._recv_closed or self._send_closed:
            raise Error("ChannelError: send on closed channel")
        if not self.is_full():
            self._items.append(item)
            if len(self._recv_waiters) > 0:
                var w = self._recv_waiters[0]
                _ = self._recv_waiters.popleft()
                self._to_wake.append(w)
            return
        self.register_sender(h)
        park_current(rt, h)

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Optional[Self.T]:
        """One-shot receive of the current task (issued by a Receiver).

        Fast path (§40.1): ring non-empty — move the OLDEST value out and
        wake the OLDEST parked sender (deferred).  Closed-and-empty: return
        None (close observable).  Slow path (§40.2): register this task as a
        receiver waiter (FIFO, deduped by id) and park via the canonical
        `park_current`; on resume the driver re-enters and the fresh call
        takes the fast path or observes close.
        """
        if len(self._items) > 0:
            var v = self._items.popleft()
            if len(self._send_waiters) > 0:
                var w = self._send_waiters[0]
                _ = self._send_waiters.popleft()
                self._to_wake.append(w)
            return Optional[Self.T](v)
        if self._send_closed or self._recv_closed:
            return Optional[Self.T]()
        self.register_receiver(h)
        park_current(rt, h)
        return Optional[Self.T]()

    # --- waiter registration (FIFO, dedupe by task id) -------------------------

    def register_sender[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        for i in range(len(self._send_waiters)):
            if self._send_waiters[i].task_id == h.id():
                return  # already parked as a sender; never double-register
        self._send_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    def register_receiver[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        for i in range(len(self._recv_waiters)):
            if self._recv_waiters[i].task_id == h.id():
                return  # already parked as a receiver; never double-register
        self._recv_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    def unregister_sender(mut self, task_id: Int) raises:
        """Logical cancellation (A5.4/A5.5 select, issues #92/#93): drop a
        parked sender waiter by task id, if present.  A `select` LOSING
        branch must vacate every OTHER branch's wait queue once a winner is
        claimed elsewhere, so a later real wake on this channel never finds
        (and double-enqueues) a stale WaitRecord for a task that already
        resumed through a different branch.  No-op when `task_id` is not
        currently registered here (idempotent, matches register_* dedupe)."""
        var kept = Deque[WaitRecord]()
        while len(self._send_waiters) > 0:
            var w = self._send_waiters[0]
            _ = self._send_waiters.popleft()
            if w.task_id != task_id:
                kept.append(w)
        self._send_waiters = kept^

    def unregister_receiver(mut self, task_id: Int) raises:
        """Logical cancellation (A5.4/A5.5 select, issues #92/#93): drop a
        parked receiver waiter by task id, if present.  See
        unregister_sender for the rationale (select's losing branches)."""
        var kept = Deque[WaitRecord]()
        while len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            if w.task_id != task_id:
                kept.append(w)
        self._recv_waiters = kept^

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
        """One Receiver dropped its slot.  When the LAST receiver closes, the
        receive side closes, the buffered values are dropped, and every
        parked sender is moved to the deferred wake list: their resumed
        send() raises "ChannelError: send on closed channel".  Idempotent."""
        if self._receivers > 0:
            self._receivers -= 1
        if self._receivers != 0:
            return
        if self._recv_closed:
            return
        self._recv_closed = True
        while len(self._items) > 0:
            _ = self._items.popleft()
        while len(self._send_waiters) > 0:
            var w = self._send_waiters[0]
            _ = self._send_waiters.popleft()
            self._to_wake.append(w)


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