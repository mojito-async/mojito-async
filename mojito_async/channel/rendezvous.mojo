# mojito_async/channel/rendezvous.mojo
#
# A5.1 rendezvous + oneshot channels (issue #89) — capacity-0 handoff
# channel: sender and receiver exchange a value directly, with no ring and
# no buffered slot (close discipline carried over from the bounded Channel,
# #35).  See also oneshot.mojo, the single-value sibling built the same way.
#
# Design notes (read before touching send()/recv() — this is the one place
# this lane diverges from a literal reading of the issue, and why):
#   - `_slot: Optional[T]` is the ONE handoff cell the issue specifies.  It
#     is used ONLY for the "sender finds an already-parked receiver"
#     direction (Case A below): the receiver has no payload of its own, so
#     the depositing sender needs a shared cell for the woken receiver to
#     read on resume.
#   - The REVERSE direction ("receiver finds an already-parked sender",
#     Case B) does NOT round-trip the item through `_slot`.  A newcomer
#     sender that finds no receiver ready carries its item INLINE in its
#     own wait record (`SendWait[T]`, not the bare `WaitRecord` from #35)
#     and parks; a later receiver pops that record directly and returns
#     `.item` synchronously — the record is gone from `_send_waiters` the
#     moment it is matched.
#   - Why not "sender always deposits into `_slot`, receiver always pops
#     `_slot`" (the naive literal reading)?  send()/recv() are ONE-SHOT
#     colorless operations (channel.mojo: "the driver re-enters the task,
#     which re-invokes send()/recv() with the same pending item") and a
#     resumed SENDER has no channel-owned signal to distinguish "still
#     queued" from "already matched" if its only footprint were a shared,
#     generic `_slot` — a blind re-attempt would re-deposit an
#     already-delivered item and re-park forever (a real lost-wakeup /
#     double-delivery bug, hand-traced during this lane's design pass).
#     `SendWait[T]` sidesteps the ambiguity: the record's presence in
#     `_send_waiters` IS the "not yet matched" signal, and a resumed
#     sender never needs to touch `_slot` at all.
#   - A resumed sender still needs ONE bit of channel memory: once matched
#     (or failed by close), its outcome is stashed by task id in
#     `_send_done` / `_send_failed` (consume-once, mirroring the TCB
#     result slot's `_has_result` guard) so its one-shot re-entry into
#     send() resolves to "already delivered" / "raise: closed" instead of
#     re-running the match/park logic a second time.
#   - `_try_advance` closes a narrower gap: at most one handoff may be "in
#     flight" through `_slot` at a time (single cell), so a second sender
#     that finds `_recv_waiters` non-empty but `_slot` still occupied must
#     ALSO queue (never clobber the earlier handoff).  When the earlier
#     handoff is finally collected, `_try_advance` immediately re-checks
#     both queues so two already-parked opposite parties that could
#     satisfy each other are never left as a mutual, unwakeable deadlock.
#
# Mojo 1.0.0b2: `def`-only, module factories (no static methods), no
# hidden allocation beyond the caller-owned Deque/List backing stores.
#
# Cross-worker safety (issue #128, mirrors channel.mojo's treatment):
# `_guard` is a SpinLock (queue.mojo's) serializing every read/write of
# `_slot`/`_send_waiters`/`_recv_waiters`/`_to_wake`/`_send_done`/
# `_send_failed`/the closed flags/the slot counts — see channel.mojo's
# module header for the full rationale (the same double-acquisition/
# lost-update class t38_mutex_cross_worker_aot caught for Mutex).  All the
# matching helpers below (`_match_waiting_receiver`, `_match_waiting_
# sender`, `_try_advance`, `_take_send_done`, `_take_send_failed`,
# `register_send_wait`, `register_receiver`) are renamed with a `_locked`
# suffix and now ASSUME the caller already holds `_guard` — `send()`/
# `recv()` combine the outcome-marker check, the match attempt, and the
# slow-path register into ONE guarded critical section (mirrors Mutex.
# lock's exact shape); `try_send`/`try_recv` wrap the SAME `_locked`
# helpers in their own single-call guard.  send()/recv() also now park via
# the TWO-PHASE `park_prepare`/`park_validate`/`park_commit` kernel instead
# of the single-phase `park_current`, for the identical lost-wakeup reason
# documented in mutex.mojo's header.  A channel wake carries no ownership
# transfer (unlike Mutex's GRANT marker) — an early wake therefore LOOPS
# back to the top-of-function attempt instead of returning; for a sender
# this naturally re-checks the `_send_done`/`_send_failed` consume-once
# markers first, which is exactly the same check a fresh re-dispatch would
# perform (see channel.mojo's `send`/`recv` docstrings for the full
# rationale).  There are no cancellable variants on this struct, so no
# cancel-wake check is needed on the early-unwind path.  The SpinLock's
# Atomic cannot be Movable/ImplicitlyDeletable, so RendezvousChannel drops
# those conformances too — it now mirrors Channel/Mutex/RWLock.
from std.collections import Deque, List
from mojito_async.channel.channel import WaitRecord, RecvOutcome, SendOutcome
from mojito_async.runtime.queue import SpinLock
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.task import JoinHandle, claim_running
from mojito_async.runtime.park import park_commit, park_prepare, park_validate


# ---------------------------------------------------------------------------
# SendWait[T] — a parked sender's identity + its NOT-YET-DELIVERED item
# ---------------------------------------------------------------------------

struct SendWait[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable, Movable
):
    """A parked sender (issue #89): unlike the bare `WaitRecord` (#35), a
    rendezvous sender carries its pending item WITH its wait identity — the
    item lives nowhere else once send() parks (returns control to the
    scheduler), so a later-arriving receiver that matches this record can
    hand the value straight to its own caller."""

    var tcb_addr: Int
    var task_id: Int
    var item: Self.T

    def __init__(out self, tcb_addr: Int, task_id: Int, item: Self.T):
        self.tcb_addr = tcb_addr
        self.task_id = task_id
        self.item = item


# ---------------------------------------------------------------------------
# RendezvousChannel[T] — capacity-0 handoff: no ring, no buffered slot
# ---------------------------------------------------------------------------

struct RendezvousChannel[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable]:
    """Capacity-0 channel (issue #89): sender and receiver hand a value off
    directly; try_send/try_recv NEVER buffer — they succeed only when the
    opposite party is already parked waiting.

    State:
      _guard         — SpinLock (issue #128) serializing every read/write of
                        the fields below — see the module header.
      _slot          — the single handoff cell; holds a value ONLY while a
                        matched receiver is between being woken and its
                        resumed recv() call collecting it (Case A).
      _send_waiters  — parked senders WITH their item inline (SendWait),
                        FIFO; each entry is a complete, independently
                        deliverable handoff (Case B).
      _recv_waiters  — parked receivers, FIFO (bare WaitRecord; receivers
                        carry no payload).
      _to_wake       — deferred wakes for the embedding driver to drain
                        (same discipline as #35).
      _send_done     — consume-once success markers (sender task ids) a
                        resumed send() reads to short-circuit to "already
                        delivered" instead of re-matching/re-parking.
      _send_failed   — consume-once failure markers (sender task ids) a
                        resumed send() reads to raise "closed" instead of
                        re-matching/re-parking.
      _send_closed / _recv_closed / _senders / _receivers — same close
                        discipline as the bounded Channel (#35).
    """

    var _guard: SpinLock
    var _slot: Optional[Self.T]
    var _send_waiters: Deque[SendWait[Self.T]]
    var _recv_waiters: Deque[WaitRecord]
    var _to_wake: Deque[WaitRecord]
    var _send_done: List[Int]
    var _send_failed: List[Int]
    var _send_closed: Bool
    var _recv_closed: Bool
    var _senders: Int
    var _receivers: Int

    def __init__(out self):
        self._guard = SpinLock()
        self._slot = Optional[Self.T]()
        self._send_waiters = Deque[SendWait[Self.T]]()
        self._recv_waiters = Deque[WaitRecord]()
        self._to_wake = Deque[WaitRecord]()
        self._send_done = List[Int]()
        self._send_failed = List[Int]()
        self._send_closed = False
        self._recv_closed = False
        self._senders = 0
        self._receivers = 0

    # --- queries -------------------------------------------------------

    def is_slot_filled(mut self) -> Bool:
        self._guard.lock()
        var f = Bool(self._slot)
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

    def send_done_len(mut self) -> Int:
        self._guard.lock()
        var v = len(self._send_done)
        self._guard.unlock()
        return v

    def send_failed_len(mut self) -> Int:
        self._guard.lock()
        var v = len(self._send_failed)
        self._guard.unlock()
        return v

    # --- handle split ----------------------------------------------------

    def sender(mut self) raises -> RendezvousSender[Self.T]:
        """Split a new RendezvousSender handle.  Refuses once the send
        side is closed.  GUARDED (issue #128): the closed check and the
        slot-count increment are ONE critical section."""
        self._guard.lock()
        if self._send_closed:
            self._guard.unlock()
            raise Error("ChannelError: send side already closed; cannot split a sender")
        self._senders += 1
        self._guard.unlock()
        return RendezvousSender[Self.T](
            UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin](to=self)
        )

    def receiver(mut self) raises -> RendezvousReceiver[Self.T]:
        """Split a new RendezvousReceiver handle.  Refuses once the
        receive side is closed.  GUARDED (issue #128), see `sender`."""
        self._guard.lock()
        if self._recv_closed:
            self._guard.unlock()
            raise Error("ChannelError: receive side already closed; cannot split a receiver")
        self._receivers += 1
        self._guard.unlock()
        return RendezvousReceiver[Self.T](
            UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin](to=self)
        )

    # --- matching helpers (shared by try_*/blocking paths) -----------------
    # Every helper below is `_locked`: the caller MUST already hold `_guard`
    # (issue #128) — send()/recv() combine these with their own marker/
    # closed checks into ONE critical section; a second internal lock here
    # would deadlock the (non-reentrant) SpinLock.

    def _match_waiting_receiver_locked(mut self, item: Self.T) raises -> Bool:
        """Case A: deposit `item` into `_slot` for the OLDEST parked
        receiver — only when the single handoff cell is free (an occupied
        cell means an earlier handoff has not been collected yet; a second
        sender must queue rather than clobber it)."""
        if self._slot:
            return False
        if len(self._recv_waiters) == 0:
            return False
        var w = self._recv_waiters[0]
        _ = self._recv_waiters.popleft()
        self._slot = Optional[Self.T](item)
        self._to_wake.append(w)
        return True

    def _match_waiting_sender_locked(mut self) raises -> Optional[SendWait[Self.T]]:
        """Case B: pop the OLDEST parked sender's inline item directly (no
        `_slot` round-trip); marks it delivered so its own resumed send()
        short-circuits instead of re-matching."""
        if len(self._send_waiters) == 0:
            return Optional[SendWait[Self.T]]()
        var sw = self._send_waiters[0]
        _ = self._send_waiters.popleft()
        self._send_done.append(sw.task_id)
        self._to_wake.append(WaitRecord(sw.tcb_addr, sw.task_id))
        return Optional[SendWait[Self.T]](sw)

    def _try_advance_locked(mut self) raises:
        """Once `_slot` frees up, immediately re-match the oldest queued
        pair if BOTH sides still have waiters: two already-parked parties
        that could satisfy each other must never sit as a mutual,
        unwakeable deadlock just because an earlier handoff was still in
        flight when they queued."""
        if self._slot:
            return
        if len(self._send_waiters) == 0 or len(self._recv_waiters) == 0:
            return
        var sw = self._send_waiters[0]
        _ = self._send_waiters.popleft()
        var rw = self._recv_waiters[0]
        _ = self._recv_waiters.popleft()
        self._slot = Optional[Self.T](sw.item)
        self._send_done.append(sw.task_id)
        self._to_wake.append(WaitRecord(sw.tcb_addr, sw.task_id))
        self._to_wake.append(rw)

    # --- consume-once outcome markers (resumed-sender re-entry) ------------

    def _take_send_done_locked(mut self, task_id: Int) -> Bool:
        for i in range(len(self._send_done)):
            if self._send_done[i] == task_id:
                var rest = List[Int]()
                for j in range(len(self._send_done)):
                    if j != i:
                        rest.append(self._send_done[j])
                self._send_done = rest^
                return True
        return False

    def _take_send_failed_locked(mut self, task_id: Int) -> Bool:
        for i in range(len(self._send_failed)):
            if self._send_failed[i] == task_id:
                var rest = List[Int]()
                for j in range(len(self._send_failed)):
                    if j != i:
                        rest.append(self._send_failed[j])
                self._send_failed = rest^
                return True
        return False

    # --- non-blocking core (never buffers, never registers) ----------------

    def try_send(mut self, item: Self.T) raises -> Bool:
        """Non-blocking send: succeeds ONLY when a receiver is already
        parked (direct match); otherwise returns False WITHOUT consuming
        `item` and WITHOUT registering a waiter — a rendezvous cannot
        buffer.  GUARDED (issue #128): the closed check and the match
        attempt are ONE critical section."""
        self._guard.lock()
        if self._send_closed or self._recv_closed:
            self._guard.unlock()
            return False
        var ok = self._match_waiting_receiver_locked(item)
        self._guard.unlock()
        return ok

    def try_recv(mut self) raises -> Optional[Self.T]:
        """Non-blocking receive: succeeds ONLY when a sender is already
        parked (direct match, `.item` returned straight from its
        SendWait); otherwise returns None.  Never touches `_slot` — a
        value sitting there is already earmarked for a specific woken
        receiver (see module docstring), not fair game for an unrelated
        try_recv().  GUARDED (issue #128)."""
        self._guard.lock()
        var matched = self._match_waiting_sender_locked()
        self._guard.unlock()
        if matched:
            return Optional[Self.T](matched.value().item)
        return Optional[Self.T]()

    # --- blocking slow paths -------------------------------------------------

    def send[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R], item: Self.T
    ) raises -> SendOutcome:
        """One-shot send (issued by a RendezvousSender).  A resumed call
        (task id already carries an outcome) short-circuits to success or
        raises "closed" WITHOUT touching the match/park logic again — see
        the module docstring for why a blind re-match would double-deliver.
        A genuinely fresh call matches an already-parked receiver directly
        (Case A) or queues (with `item` inline) and parks (Case B).

        The marker checks, the closed check, the match attempt, and the
        slow-path register are ONE guarded critical section (issue #128).
        Parks via the TWO-PHASE `park_prepare`/`park_validate`/
        `park_commit` kernel, NOT the single-phase `park_current`, so a
        cross-worker recv()/close() racing into the PARKING window is
        never lost.  A wake carries no ownership transfer — an early wake
        LOOPS back to the top-of-function attempt instead of returning,
        which naturally re-checks `_send_done`/`_send_failed` first (the
        exact check a fresh re-dispatch would perform) — see channel.
        mojo's `send` docstring for the full rationale."""
        while True:
            self._guard.lock()
            if self._take_send_done_locked(h.id()):
                self._guard.unlock()
                return SendOutcome(SendOutcome.SENT)
            if self._take_send_failed_locked(h.id()):
                self._guard.unlock()
                raise Error("ChannelError: send on closed channel")
            if self._send_closed or self._recv_closed:
                self._guard.unlock()
                raise Error("ChannelError: send on closed channel")
            if self._match_waiting_receiver_locked(item):
                self._guard.unlock()
                return SendOutcome(SendOutcome.SENT)
            self._register_send_wait_locked(h, item)
            self._guard.unlock()
            park_prepare(h)
            if park_validate(h):
                _ = park_commit(h)
                claim_running(h)
                continue
            if not park_commit(h):
                claim_running(h)
                continue
            return SendOutcome(SendOutcome.PARKED)

    def recv[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises -> RecvOutcome[Self.T]:
        """One-shot receive (issued by a RendezvousReceiver).  Safe to
        re-invoke on resume: both a filled `_slot` (Case A) and a queued
        sender (Case B) are state-derived, idempotent checks — no outcome
        marker is needed on this side (see module docstring).

        GUARDED (issue #128): the slot check, the match attempt, the
        closed check, and the slow-path register are ONE critical section.
        Parks via the TWO-PHASE kernel (see `send`); on an early wake, loop
        back to the top-of-function attempt instead of returning."""
        while True:
            self._guard.lock()
            if self._slot:
                var v = self._slot.value()
                self._slot = Optional[Self.T]()
                self._try_advance_locked()
                self._guard.unlock()
                return RecvOutcome[Self.T](RecvOutcome.VALUE, Optional[Self.T](v))
            var matched = self._match_waiting_sender_locked()
            if matched:
                self._guard.unlock()
                return RecvOutcome[Self.T](RecvOutcome.VALUE, Optional[Self.T](matched.value().item))
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

    # --- waiter registration (FIFO, dedupe by task id) ----------------------

    def _register_send_wait_locked[R: ResultValue](
        mut self, h: JoinHandle[R], item: Self.T
    ) raises:
        """Caller MUST hold `_guard` (issue #128), see the matching-helpers
        note above."""
        for i in range(len(self._send_waiters)):
            if self._send_waiters[i].task_id == h.id():
                return  # already parked as a sender; never double-register
        self._send_waiters.append(SendWait[Self.T](Int(h.tcb()), h.id(), item))

    def _register_receiver_locked[R: ResultValue](mut self, h: JoinHandle[R]) raises:
        """Caller MUST hold `_guard` (issue #128), see the matching-helpers
        note above."""
        for i in range(len(self._recv_waiters)):
            if self._recv_waiters[i].task_id == h.id():
                return  # already parked as a receiver; never double-register
        self._recv_waiters.append(WaitRecord(Int(h.tcb()), h.id()))

    # --- deferred wakes (driver drains these) -------------------------------

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

    # --- close slots ---------------------------------------------------------

    def close_sender_slot(mut self) raises:
        """One RendezvousSender dropped its slot.  When the LAST sender
        closes, the send side closes and every parked receiver is moved to
        the deferred wake list: a receiver still finds a genuinely queued
        sender's item if one exists (send-side closing does not cancel an
        already-queued handoff), otherwise it observes close (None).
        Idempotent.  GUARDED (issue #128): the slot-count decrement, the
        closed-flag flip, and the FIFO drain are ONE critical section."""
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
        """One RendezvousReceiver dropped its slot.  When the LAST receiver
        closes, the receive side closes, any in-flight `_slot` value is
        dropped, and every parked sender is moved to the deferred wake list
        with a FAILURE outcome: its resumed send() raises "ChannelError:
        send on closed channel".  Idempotent.  GUARDED (issue #128), see
        `close_sender_slot`."""
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
        self._slot = Optional[Self.T]()
        while len(self._send_waiters) > 0:
            var sw = self._send_waiters[0]
            _ = self._send_waiters.popleft()
            self._send_failed.append(sw.task_id)
            self._to_wake.append(WaitRecord(sw.tcb_addr, sw.task_id))
        self._guard.unlock()


# ---------------------------------------------------------------------------
# RendezvousSender / RendezvousReceiver — light handles (named distinctly
# from #35's Sender/Receiver so both are re-exportable from channel/
# __init__.mojo without a name clash)
# ---------------------------------------------------------------------------

struct RendezvousSender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Send half of a rendezvous channel (issue #89).  Single-owner BY
    CONVENTION (b2 handles are implicitly copyable); `_closed` latches the
    handle-level close (idempotent second close)."""

    var _chan: UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin]:
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
        side closes and parked receivers are woken.  Idempotent per
        handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_sender_slot()


struct RendezvousReceiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    ImplicitlyCopyable, ImplicitlyDeletable
):
    """Receive half of a rendezvous channel (issue #89)."""

    var _chan: UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin]
    var _closed: Bool

    def __init__(out self, chan: UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin]):
        self._chan = chan
        self._closed = False

    def channel(mut self) -> UnsafePointer[RendezvousChannel[Self.T], MutAnyOrigin]:
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
        receive side closes, any in-flight handoff is dropped, and parked
        senders are woken with a failure outcome (their resumed send()
        raises).  Idempotent per handle."""
        if self._closed:
            return
        self._closed = True
        self._chan[].close_receiver_slot()


# ---------------------------------------------------------------------------
# Module factories (b2: no static methods)
# ---------------------------------------------------------------------------

def make_rendezvous[
    T: Movable & ImplicitlyCopyable & ImplicitlyDeletable
]() -> RendezvousChannel[T]:
    """Fresh capacity-0 rendezvous channel (no ring, no buffered slot)."""
    return RendezvousChannel[T]()


def make_rendezvous_sender[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[RendezvousChannel[T], MutAnyOrigin],
) raises -> RendezvousSender[T]:
    """Hand a RendezvousSender handle over an existing channel cell."""
    return chan[].sender()


def make_rendezvous_receiver[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[RendezvousChannel[T], MutAnyOrigin],
) raises -> RendezvousReceiver[T]:
    """Hand a RendezvousReceiver handle over an existing channel cell."""
    return chan[].receiver()
