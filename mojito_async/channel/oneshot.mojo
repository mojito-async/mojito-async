# mojito_async/channel/oneshot.mojo
#
# A5.1 oneshot channel (issue #89) — single-value, single-delivery channel:
# `Oneshot[T]` holds exactly one value and delivers it exactly once.
#
# Unlike RendezvousChannel (rendezvous.mojo, same issue), a oneshot's
# send() NEVER parks: there is no backpressure concept for a single slot
# from the sender's side — it either deposits the one-and-only value now
# (fresh slot, not yet closed) or raises immediately ("already sent" /
# "receiver dropped").  Only recv() can park (waiting for the value to
# arrive), and — exactly like RendezvousChannel.recv() — that resumed call
# is safely re-invokable: `_filled`/`_send_closed`/`_recv_closed` are
# state-derived, idempotent checks, so no consume-once outcome marker is
# needed here (see rendezvous.mojo's module docstring for why the SENDER
# side of a rendezvous needs one and this file's sender does not: a
# oneshot's send() never parks in the first place, so there is no resumed
# send() call to disambiguate).
from std.collections import Deque
from mojito_async.channel.channel import WaitRecord
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle
from mojito_async.runtime.park import park_current


# ---------------------------------------------------------------------------
# Oneshot[T] — single-value slot + closed flag
# ---------------------------------------------------------------------------

struct Oneshot[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyDeletable, Movable
):
    """Single-value, single-delivery channel (issue #89).

    State:
      _slot          — the one-and-only value, once sent and before it is
                        collected.
      _filled        — True from a successful send() until try_recv()/
                        recv() consumes the value.
      _recv_waiters  — parked receivers (FIFO; only one will ever collect
                        the real value — later ones observe close).
      _to_wake       — deferred wakes for the embedding driver to drain
                        (same discipline as #35/#89's RendezvousChannel).
      _send_closed   — the send side is done: either a value was
                        successfully sent (one-shot, so no more sends are
                        possible) or the last OneshotSender dropped
                        without ever sending.
      _recv_closed   — the receive side is done: either the value was
                        collected or the last OneshotReceiver dropped.
      _senders / _receivers — live slot counts (close when the last
                        handle drops), mirroring the bounded Channel.
    """

    var _slot: Optional[Self.T]
    var _filled: Bool
    var _recv_waiters: Deque[WaitRecord]
    var _to_wake: Deque[WaitRecord]
    var _send_closed: Bool
    var _recv_closed: Bool
    var _senders: Int
    var _receivers: Int

    def __init__(out self):
        self._slot = Optional[Self.T]()
        self._filled = False
        self._recv_waiters = Deque[WaitRecord]()
        self._to_wake = Deque[WaitRecord]()
        self._send_closed = False
        self._recv_closed = False
        self._senders = 0
        self._receivers = 0

    # --- queries -------------------------------------------------------

    def is_filled(self) -> Bool:
        return self._filled

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

    def recv_waiters_len(self) -> Int:
        return len(self._recv_waiters)

    def to_wake_len(self) -> Int:
        return len(self._to_wake)

    # --- handle split ----------------------------------------------------

    def sender(mut self) raises -> OneshotSender[Self.T]:
        """Split a new OneshotSender handle.  Refuses once the send side
        is closed."""
        if self._send_closed:
            raise Error("ChannelError: send side already closed; cannot split a sender")
        self._senders += 1
        return OneshotSender[Self.T](UnsafePointer[Oneshot[Self.T], MutAnyOrigin](to=self))

    def receiver(mut self) raises -> OneshotReceiver[Self.T]:
        """Split a new OneshotReceiver handle.  Refuses once the receive
        side is closed."""
        if self._recv_closed:
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        return OneshotReceiver[Self.T](UnsafePointer[Oneshot[Self.T], MutAnyOrigin](to=self))

    # --- send (never parks: one slot, no backpressure to wait on) ---------

    def try_send(mut self, item: Self.T) raises -> Bool:
        """Deliver the one-and-only value.  False when a value was already
        sent, the send side is closed, or the receiver dropped — `item` is
        NOT consumed in that case."""
        if self._filled or self._send_closed or self._recv_closed:
            return False
        self._slot = Optional[Self.T](item)
        self._filled = True
        self._send_closed = True  # one-shot: this WAS the only send, ever
        while len(self._recv_waiters) > 0:
            var w = self._recv_waiters[0]
            _ = self._recv_waiters.popleft()
            self._to_wake.append(w)
        return True

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises:
        """Deliver the one-and-only value; raises instead of returning
        False.  Two distinct, documented failure prefixes (both under the
        "ChannelError:" taxonomy, spec-shared-context convention):
          - "ChannelError: oneshot already sent" — a second send.
          - "ChannelError: send on closed channel (receiver dropped)" —
            the receiver side is gone; checked FIRST so a racing close
            always wins over a stale "already sent" reading.
        Never parks: unlike RendezvousChannel.send(), there is no
        receiver-readiness gate to wait on for a single slot."""
        if self._recv_closed:
            raise Error("ChannelError: send on closed channel (receiver dropped)")
        if self._filled or self._send_closed:
            raise Error("ChannelError: oneshot already sent")
        _ = self.try_send(item)

    # --- recv (may park; safely re-invokable on resume) --------------------

    def try_recv(mut self) raises -> Optional[Self.T]:
        """Take the value if it has been sent; consumes it (further calls
        return None).  None if nothing has been sent yet OR it was already
        taken — never distinguishes those two (use is_closed()/is_filled()
        for that)."""
        if self._filled:
            var v = self._slot.value()
            self._slot = Optional[Self.T]()
            self._filled = False
            self._recv_closed = True  # one-shot: value taken, channel done
            return Optional[Self.T](v)
        return Optional[Self.T]()

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> Optional[Self.T]:
        """One-shot receive (issued by a OneshotReceiver).  Safe to
        re-invoke on resume: `_filled`/closed are state-derived, idempotent
        checks (see module docstring — the sender side never parks, so
        there is no resumed-sender ambiguity to guard against here)."""
        if self._filled:
            return self.try_recv()
        if self._send_closed or self._recv_closed:
            return Optional[Self.T]()  # closed-and-unset: observe close
        self.register_receiver(h)
        park_current(rt, h)
        return Optional[Self.T]()

    # --- waiter registration (FIFO, dedupe by task id) ----------------------

    def register_receiver[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        for i in range(len(self._recv_waiters)):
            if self._recv_waiters[i].task_id == h.id():
                return  # already parked as a receiver; never double-register
        self._recv_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    # --- deferred wakes (driver drains these) -------------------------------

    def pop_to_wake(mut self) raises -> WaitRecord:
        """Pop the oldest deferred wake.  Returns the (0,0) sentinel record
        when the list is empty (mirrors #35's Channel.pop_to_wake)."""
        if len(self._to_wake) == 0:
            return WaitRecord(0, 0)
        var w = self._to_wake[0]
        _ = self._to_wake.popleft()
        return w

    # --- close slots ---------------------------------------------------------

    def close_sender_slot(mut self) raises:
        """One OneshotSender dropped its slot.  When the LAST sender
        closes WITHOUT a value ever having been sent, the send side closes
        and every parked receiver is moved to the deferred wake list: its
        resumed recv() observes close (None).  A sender closing AFTER a
        successful send is a pure no-op (the send side already closed
        itself in try_send()).  Idempotent."""
        if self._senders > 0:
            self._senders -= 1
        if self._senders != 0:
            return
        if self._send_closed:
            return
        self._send_closed = True
        if not self._filled:
            while len(self._recv_waiters) > 0:
                var w = self._recv_waiters[0]
                _ = self._recv_waiters.popleft()
                self._to_wake.append(w)

    def close_receiver_slot(mut self) raises:
        """One OneshotReceiver dropped its slot.  When the LAST receiver
        closes, the receive side closes and any unsent-or-uncollected
        value is dropped: a subsequent/pending send() observes "receiver
        dropped" and raises.  Idempotent."""
        if self._receivers > 0:
            self._receivers -= 1
        if self._receivers != 0:
            return
        if self._recv_closed:
            return
        self._recv_closed = True
        self._slot = Optional[Self.T]()
        self._filled = False


# ---------------------------------------------------------------------------
# OneshotSender / OneshotReceiver — light handles over the shared cell
# ---------------------------------------------------------------------------

struct OneshotSender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Send half of a oneshot (issue #89).  Single-owner BY CONVENTION (b2
    handles are implicitly copyable); `_closed` latches the handle-level
    close (idempotent second close)."""

    var _chan: UnsafePointer[Oneshot[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[Oneshot[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[Oneshot[Self.T], MutAnyOrigin]:
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
            raise Error("ChannelError: oneshot already sent")
        self._chan[].send(rt, h, item)

    def close(mut self) raises:
        """Drop this sender's slot.  When the LAST sender closes without
        ever sending, the send side closes and parked receivers observe
        close.  Idempotent per handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_sender_slot()


struct OneshotReceiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Receive half of a oneshot (issue #89)."""

    var _chan: UnsafePointer[Oneshot[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[Oneshot[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[Oneshot[Self.T], MutAnyOrigin]:
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
        receive side closes and any unsent/uncollected value is dropped
        (a pending send() then raises "receiver dropped").  Idempotent per
        handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_receiver_slot()


# ---------------------------------------------------------------------------
# Module factories (b2: no static methods)
# ---------------------------------------------------------------------------

def make_oneshot[
    T: Movable & ImplicitlyCopyable & ImplicitlyDeletable
]() -> Oneshot[T]:
    """Fresh, empty, unsent oneshot slot."""
    return Oneshot[T]()


def make_oneshot_sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[Oneshot[T], MutAnyOrigin],
) raises -> OneshotSender[T]:
    """Hand a OneshotSender handle over an existing oneshot cell."""
    return chan[].sender()


def make_oneshot_receiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[Oneshot[T], MutAnyOrigin],
) raises -> OneshotReceiver[T]:
    """Hand a OneshotReceiver handle over an existing oneshot cell."""
    return chan[].receiver()
