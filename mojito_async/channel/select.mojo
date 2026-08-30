# mojito_async/channel/select.mojo
#
# A5.4 (issue #92) multi-wait select primitive + A5.5 (issue #93) close/
# select behavior and try-scan fairness + A5.3 (issue #91) deadline/timeout
# integration (spec §42, Phase A5).
#
# select() waits on several channel recv/send operations and a real
# timer-integrated deadline branch (issue #91 — `deadline_branch(heap,
# clock, deadline)`/`timeout_branch(heap, clock, duration)`, layered on
# the #92 bare ticks-only descriptor) and resumes on exactly ONE winner;
# the losing branches are logically cancelled and the whole scheme
# composes with the C6 generation-claim / park kernel unchanged (EPIC #4,
# #56/#58): select never invents a second park/wake mechanism, it only
# fans ONE task's WaitRecord out across several channels' existing FIFO
# wait queues (plus, for #91, the A1.4 timer heap's own per-id generation
# slot) and collapses back to one via unregister_*/cancel_token once a
# winner is claimed.
#
# ---------------------------------------------------------------------------
# Erasure strategy decision (issue #92 deliverable; mirrors #42's ADR-015)
# ---------------------------------------------------------------------------
#
# ADR-015 (scope's mixed-type child registry, #42) settled on ENTRY-AXIS
# erasure only: a registry entry is address-erased DATA (tcb_addr + task_id
# — the house TaskRecord/WaitRecord shape already used everywhere in this
# codebase), typed access happens ONLY at statically-typed call sites, and
# RESULTS stay value-static (never erased through an untyped pointer).  This
# module adopts the SAME split:
#
#   - SelectBranch (entry axis) is address-erased DATA: a `kind`
#     discriminant (RECV/SEND/DEADLINE) plus plain Int addresses (the target
#     Channel[T] cell, and for SEND the caller-owned item slot).  No T
#     parameter lives on the struct — b2 has no vtables/function-typed
#     fields, so a branch cannot carry a typed pointer AND be storable in a
#     single `List[SelectBranch]` unless the struct itself is monomorphic.
#     `recv_branch[T]`/`send_branch[T]`/`deadline_branch` are generic
#     CONVENIENCE FACTORIES that extract the Int address from a typed
#     pointer at the callsite (mirrors how channel.mojo's WaitRecord erases
#     tcb_addr, and how the *_aot drivers re-form a JoinHandle from raw
#     ints) — the erasure is address-only, never a stored callback.
#   - select[T]/select_fast[T]/rescan[T]/classify_branch[T] are GENERIC over
#     ONE element type T per call: every branch in a single select() call
#     re-forms its Channel[T] pointer with the SAME T the call is
#     monomorphized with.  This is the buildable interpretation of
#     "heterogeneous branches" in a def-only, no-vtable dialect: true
#     cross-T heterogeneity would need dynamic dispatch through a stored
#     function value, which b2 cannot lower (see the repo-wide "dispatch is
#     always a def switch on an enum/int kind" rule).  Within one T, the
#     KINDS are still heterogeneous (RECV vs SEND vs DEADLINE), which is
#     where the real erasure need lives.
#   - SelectOutcome[T] (results axis) stays VALUE-STATIC per ADR-015 point
#     7: it is generic over T and carries a typed `Optional[T]` payload
#     directly — no second untyped-pointer indirection is needed since T is
#     already known inside select[T].  The outcome's "erasure" is just the
#     `kind` discriminant selecting which typed accessor is valid (mirrors
#     the "erased-registry decision" of picking a discriminant + per-kind
#     accessor over a stored callback) — the SAME shape #42 chose for mixed
#     scope children, applied here to mixed BRANCH KINDS instead of mixed T.
#
# ---------------------------------------------------------------------------
# Algorithm (issue #92 detailed plan)
# ---------------------------------------------------------------------------
#
#   1. rescan classifies every branch (READY_DATA / READY_CLOSED / BLOCKED /
#      SKIP) via the channel's own non-parking try_send/try_recv semantics
#      (spec §40.1) — a RECV on a closed-and-empty channel is READY_CLOSED
#      (never blocks select, #93); a SEND on a closed channel is SKIP (never
#      selectable, #93; its send would raise).
#   2. Precedence (issue #93): the first READY_DATA branch in scan order
#      wins; only when NONE is READY_DATA does the first READY_CLOSED win
#      (data-before-close, deterministic).  `SelectState._rescan_cursor`
#      rotates the scan START point across repeated calls (fairness, #93)
#      while the FIFO registration-order tiebreak among several READY_DATA
#      branches at cursor==0 stays the natural fallback.
#   3. select_fast never registers a waiter — it only runs the same rescan
#      and, on a hit, performs the claim (try_recv/try_send) and returns.
#   4. select performs the SAME fast attempt first (never parks when
#      something is already claimable, mirroring Channel.send/recv's own
#      "attempt once" discipline).  Only when nothing is claimable does it
#      register the current task's WaitRecord into every genuinely BLOCKED
#      branch's channel wait queue (register_receiver/register_sender —
#      idempotent, dedup by task id, unchanged from #35) and park ONCE via
#      the canonical `park_current` (#39).  The embedding driver re-enters
#      select() on resume — the fresh rescan takes the fast path (the wake
#      came from exactly one branch becoming ready) and, on the claim,
#      unregisters the task from every OTHER branch (logical cancellation)
#      so a later real wake on a losing branch can never double-enqueue a
#      stale WaitRecord for this task (composes with the C6 generation-claim
#      discipline in park.mojo/#56/#58: the loser is gone from the wait
#      queue entirely, not merely generation-stale).
#
# ---------------------------------------------------------------------------
# #91 timer wiring — deadline/timeout as a REAL select branch
# ---------------------------------------------------------------------------
#
#   `deadline_branch(heap, clock, deadline)` / `timeout_branch(heap, clock,
#   duration)` layer a caller-owned `time.timer_heap.TimerHeap` address onto
#   the #92 DEADLINE descriptor (`SelectBranch.heap_addr`) so `select()` can
#   arm/cancel a REAL wake instead of only comparing against a caller-
#   supplied `now_ticks` (the #92 fast-path-only behavior, unchanged for the
#   bare `deadline_branch(deadline_ticks)` form).
#     - ARM-BEFORE-PARK: when a DEADLINE branch is genuinely BLOCKED (its
#       `now_ticks` hasn't reached `deadline_ticks` yet), `select()` arms
#       the heap for THIS task (`heap.arm(h.id(), Int(h.tcb()),
#       deadline_ticks)` — the exact call `time/sleep.mojo#sleep_current`
#       uses) BEFORE registering the other channel branches and parking, so
#       an already-due deadline is never missed (mirrors the sleep lane's
#       proven arm-before-park order, #36).  An already-due deadline is
#       caught by the ordinary `classify_branch` fast path and never touches
#       the heap at all — `select_fast` never arms or cancels a timer.
#     - CANCEL SYMMETRY: `SelectState` carries the ONE armed timer's
#       generation (`_timer_armed`/`_timer_gen`) across the park/resume
#       boundary.  A channel-branch winner calls `_cancel_armed_timer` to
#       drop the still-pending heap entry (`heap.cancel_token`) so the heap
#       ends empty; a DEADLINE winner needs no extra cancel step — the
#       timer service (`time/timer_service.service_timers`) already popped
#       its own entry before waking this task — and `_unregister_others`
#       (unchanged) drops every losing channel registration exactly as it
#       already does for any other winner kind.
#     - ONE timer per task: `heap.arm` is keyed by `h.id()`, matching every
#       other timer/sleep caller in this codebase; a select cycle carries
#       at most one live DEADLINE branch's arm at a time (the natural
#       reading of "a select call's timeout").
#     - Exactly-at ties resolve exactly like any other #93 rescan tie: the
#       first READY_DATA branch in scan order wins (registration-order
#       determinism), so a channel branch and an elapsed deadline becoming
#       ready in the SAME rescan still produce exactly one winner.
#
#
# Mojo 1.0.0b2 workarounds: def-only, module factories (no static methods);
# SelectBranch/SelectState/SelectOutcome are pure DATA, no function-typed
# fields; dispatch on `kind` is a plain Int switch throughout (the repo-wide
# convention); no externs (pure-Mojo package, no C-ABI firewall needed).
from std.collections import List
from mojito_async.channel.channel import Channel
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.runtime.park import park_commit, park_prepare, park_validate
from mojito_async.task import JoinHandle, SuspendReason, claim_running
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline, Duration
from mojito_async.time.timer_heap import TimerHeap


# ---------------------------------------------------------------------------
# SelectBranch — type-erased DATA descriptor (entry axis, see header)
# ---------------------------------------------------------------------------

struct SelectBranch(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """One branch of a select call: a kind discriminant plus plain Int
    addresses.  Build with `recv_branch`/`send_branch`/`deadline_branch`
    (module factories, #92) rather than the raw constructor."""

    comptime RECV = Int(0)
    comptime SEND = Int(1)
    comptime DEADLINE = Int(2)

    # RECV/SEND: address of the Channel[T] cell (0 for DEADLINE).
    var chan_addr: Int
    # SEND only: address of the caller-owned T slot holding the item to
    # send (0 otherwise; DEADLINE never touches it).
    var item_addr: Int
    # DEADLINE only: the deadline tick value compared against a caller-
    # supplied `now_ticks` (0 otherwise; RECV/SEND never touch it).  A
    # bare `deadline_branch(deadline_ticks)` (#92 descriptor) never
    # classifies READY on its own until `now_ticks` says so; the real
    # timer-integrated factories below (#91) additionally arm a heap.
    var deadline_ticks: Int
    # DEADLINE only (issue #91): address of the caller-owned TimerHeap used
    # to ARM a real wake when this branch is genuinely BLOCKED at park time
    # and to CANCEL it when a different branch wins (0 for RECV/SEND, and
    # for the bare #92 ticks-only descriptor which has no heap to arm).
    var heap_addr: Int
    var kind: Int

    def __init__(
        out self,
        kind: Int,
        chan_addr: Int,
        item_addr: Int,
        deadline_ticks: Int,
        heap_addr: Int = 0,
    ):
        self.kind = kind
        self.chan_addr = chan_addr
        self.item_addr = item_addr
        self.deadline_ticks = deadline_ticks
        self.heap_addr = heap_addr


def recv_branch[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[Channel[T], MutAnyOrigin],
) -> SelectBranch:
    """RECV branch over `chan` (issue #92)."""
    return SelectBranch(SelectBranch.RECV, Int(chan), 0, 0)


def send_branch[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    chan: UnsafePointer[Channel[T], MutAnyOrigin],
    item_slot: UnsafePointer[T, MutAnyOrigin],
) -> SelectBranch:
    """SEND branch over `chan`; `item_slot` is a caller-owned cell holding
    the value to send if this branch wins (issue #92)."""
    return SelectBranch(SelectBranch.SEND, Int(chan), Int(item_slot), 0)


def deadline_branch(deadline_ticks: Int) -> SelectBranch:
    """DEADLINE branch (issue #92 descriptor; real timer wiring is #91).
    Selectable once a caller-supplied `now_ticks >= deadline_ticks`."""
    return SelectBranch(SelectBranch.DEADLINE, 0, 0, deadline_ticks)


def deadline_branch(
    heap: UnsafePointer[TimerHeap, MutAnyOrigin],
    clock: MonotonicClock,
    deadline: Deadline,
) -> SelectBranch:
    """Real timer-integrated DEADLINE branch (issue #91): the #92 bare
    descriptor carries a tick value nobody arms; this overload additionally
    stores `heap`'s address so `select()` can ARM a genuine timer wake when
    the branch is BLOCKED at park time (see the #91 section above) and
    CANCEL it if a different branch wins.  `clock` is accepted for call-
    site symmetry with `timeout_branch` and with `time/sleep.mojo`'s
    `sleep_until_current(rt, h, heap, clock, deadline)` — an ABSOLUTE
    deadline needs no `now` reading to resolve into ticks, so it is
    otherwise unused here."""
    var ticks = Int(UInt64(deadline.at_ms()) * 1000000)
    return SelectBranch(SelectBranch.DEADLINE, 0, 0, ticks, Int(heap))


def timeout_branch(
    heap: UnsafePointer[TimerHeap, MutAnyOrigin],
    clock: MonotonicClock,
    duration: Duration,
) -> SelectBranch:
    """Real timer-integrated DEADLINE branch from a RELATIVE duration
    (issue #91): resolves `clock.now() + duration.ticks()` into an
    absolute deadline at construction time — the exact convention
    `time/sleep.mojo#sleep_current` uses for `sleep(duration)`."""
    var ticks = Int(clock.now() + duration.ticks())
    return SelectBranch(SelectBranch.DEADLINE, 0, 0, ticks, Int(heap))


# Sentinel tick value a `now_ticks` reading can never reach (issue #85 gap
# fill): the largest representable Int, mirroring timer_heap.mojo's
# NO_DEADLINE (UInt64 max) one field-width down since SelectBranch.
# deadline_ticks is a plain (signed) Int.
comptime NEVER_TICKS = Int(0x7FFF_FFFF_FFFF_FFFF)


def never() -> SelectBranch:
    """A DEADLINE branch that can NEVER resolve (issue #85 deliverable: "a
    `never()` branch never fires and only ever loses"; used for
    channel-only selects where no deadline applies, e.g. a socket-only
    wait with an optional timeout the caller sometimes omits).

    `heap_addr` stays 0 (never armed — mirrors the bare #92 ticks-only
    `deadline_branch(deadline_ticks)` descriptor exactly, so `select()`
    never touches the heap for this branch: the `b.heap_addr != 0` guard
    in both the arm site and `_cancel_armed_timer` already skips it) and
    `deadline_ticks` is NEVER_TICKS, so `classify_branch`'s `now_ticks >=
    branch.deadline_ticks` test can never be satisfied by any real or
    virtual clock reading in this codebase's lifetime.  The branch
    therefore always classifies BLOCKED: `select()` registers it as a
    genuinely blockable branch (satisfying the "at least one
    blockable/claimable branch" well-formedness guard even when a
    `never()` branch sits alongside a single live channel branch) but it
    can never itself become the winner — the select still completes via
    another branch, exactly as issue #85 requires."""
    return SelectBranch(SelectBranch.DEADLINE, 0, 0, NEVER_TICKS)


# ---------------------------------------------------------------------------
# Readiness classification (issue #93 precedence table)
# ---------------------------------------------------------------------------

comptime READY_DATA = Int(0)
"""Branch would complete without blocking (data to recv / room to send /
deadline elapsed) — the branch select() and select_fast() prefer."""
comptime READY_CLOSED = Int(1)
"""RECV on a closed-and-empty channel: selectable, yields a close outcome,
never blocks select (issue #93) — only claimed when no branch is
READY_DATA (data-before-close precedence)."""
comptime BLOCKED = Int(2)
"""Genuinely not ready yet: the only classification select() may park on."""
comptime SKIP = Int(3)
"""SEND on a closed channel: never selectable, contributes nothing to
either the claim scan or the park registration (issue #93)."""


def classify_branch[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    branch: SelectBranch, now_ticks: Int = -1
) raises -> Int:
    """Non-mutating readiness probe (issue #93 rescan step 1): reuses the
    channel's own non-parking try_send/try_recv semantics (spec §40.1) so
    select_fast never parks and select only registers on a genuinely
    BLOCKED branch."""
    if branch.kind == SelectBranch.RECV:
        var chan = UnsafePointer[Channel[T], MutAnyOrigin](
            unsafe_from_address=branch.chan_addr
        )
        if chan[].len() > 0:
            return READY_DATA
        if chan[].is_closed():
            return READY_CLOSED
        return BLOCKED
    if branch.kind == SelectBranch.SEND:
        var chan = UnsafePointer[Channel[T], MutAnyOrigin](
            unsafe_from_address=branch.chan_addr
        )
        if chan[].is_closed():
            return SKIP
        if not chan[].is_full():
            return READY_DATA
        return BLOCKED
    if branch.kind == SelectBranch.DEADLINE:
        if now_ticks >= 0 and now_ticks >= branch.deadline_ticks:
            return READY_DATA
        return BLOCKED
    raise Error("SelectError: unknown branch kind " + String(branch.kind))


def branch_ready[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    branches: List[SelectBranch], idx: Int, now_ticks: Int = -1
) raises -> Bool:
    """Per-branch fairness/diagnostic probe exposed for tests (issue #93):
    True when `branches[idx]` would be claimable (READY_DATA or
    READY_CLOSED) by the next rescan, without performing the claim."""
    var cls = classify_branch[T](branches[idx], now_ticks)
    return cls == READY_DATA or cls == READY_CLOSED


# ---------------------------------------------------------------------------
# SelectState — caller-owned winner cell + fairness cursor
# ---------------------------------------------------------------------------

struct SelectState(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Caller-owned select bookkeeping (issue #92/#93).

    `winner` is a plain Int index (the single-claim touchstone the A2
    multi-worker seam would make atomic — the model is carried here exactly
    as park.mojo's PARKING-LOT-ADAPTER note carries the two-phase protocol
    as a MODEL for today's race-free single worker); -1 means "no winner
    yet" (still pending / parked).  `_rescan_cursor` rotates the try-scan
    start index across repeated select_fast/select calls so a perpetually-
    ready low-index branch cannot starve higher-index branches (issue #93
    fairness rule); it advances to just past the last claimed index."""

    var winner: Int
    var _rescan_cursor: Int
    # Timer slot (issue #91): tracks whether THIS select cycle armed a real
    # heap timer for a DEADLINE branch that was genuinely BLOCKED at park
    # time, and the generation token needed to cancel it (`heap.cancel_
    # token(task_id, gen)`) if a different branch wins first.  Persists
    # across the park -> resume boundary on the SAME caller-owned
    # SelectState the whole select cycle threads through (mirrors how
    # `winner`/`_rescan_cursor` already persist that way).
    var _timer_armed: Bool
    var _timer_gen: Int
    var _timer_deadline: UInt64

    def __init__(out self):
        self.winner = -1
        self._rescan_cursor = 0
        self._timer_armed = False
        self._timer_gen = 0
        self._timer_deadline = 0

    def cursor(self) -> Int:
        return self._rescan_cursor

    def timer_armed(self) -> Bool:
        """True while a real heap timer is still pending for this select
        cycle (issue #91 diagnostic/test accessor)."""
        return self._timer_armed

    def reset(mut self):
        """Clear the winner cell for a fresh select cycle (the cursor is
        NOT reset — fairness rotation is meant to persist across cycles).
        A still-armed timer is left untouched — the code paths that resolve
        a select cycle (`select`/`select_fast`'s claim step, issue #91)
        always consume `_timer_armed` themselves before a caller could
        reasonably call reset(); reset() is not a substitute for a real
        winner claim."""
        self.winner = -1


# ---------------------------------------------------------------------------
# rescan — the shared two-pass claim scan (issue #93 precedence + fairness)
# ---------------------------------------------------------------------------

def rescan[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    branches: List[SelectBranch], mut state: SelectState, now_ticks: Int = -1
) raises -> Int:
    """Two-pass claim scan shared by select() and select_fast().

    Pass 1 looks for the first READY_DATA branch starting at the rotating
    cursor (wrapping); pass 2 (only reached when pass 1 finds nothing) looks
    for the first READY_CLOSED branch the same way — data-before-close is
    therefore deterministic regardless of cursor position (issue #93).  On
    a hit, the cursor advances to just past the claimed index (round-robin
    fairness); on a miss (-1) the cursor is left untouched so the NEXT call
    resumes the same rotation.  Never mutates a channel — pure probe."""
    var n = len(branches)
    if n == 0:
        return -1
    var cursor = state._rescan_cursor % n
    for k in range(n):
        var i = (cursor + k) % n
        if classify_branch[T](branches[i], now_ticks) == READY_DATA:
            state._rescan_cursor = (i + 1) % n
            return i
    for k in range(n):
        var i = (cursor + k) % n
        if classify_branch[T](branches[i], now_ticks) == READY_CLOSED:
            state._rescan_cursor = (i + 1) % n
            return i
    return -1


# ---------------------------------------------------------------------------
# SelectOutcome — value-static per-kind result (see erasure decision above)
# ---------------------------------------------------------------------------

struct SelectOutcome[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    Movable, ImplicitlyDeletable
):
    """Result of a select claim.  `index`/`kind` place the winner; the
    typed accessors below are the "per-branch accessor helpers" the #92
    deliverable calls for — each is only valid for its matching `kind`
    (mirrors #42's discriminant + per-kind accessor decision, applied to
    branch KINDS rather than element types since T is already static
    here — see the module header)."""

    var index: Int
    var kind: Int
    var closed: Bool
    var sent: Bool
    var value: Optional[Self.T]

    def __init__(out self):
        self.index = -1
        self.kind = -1
        self.closed = False
        self.sent = False
        self.value = Optional[Self.T]()

    def is_pending(self) -> Bool:
        """True when select()/select_fast() found nothing claimable: for
        select_fast this IS the final answer; for select it means the
        caller parked and must check `h.state()` (exactly the same caller-
        side discipline Channel.recv()'s empty-Optional sentinel already
        requires, spec §40.2) before re-driving."""
        return self.index == -1

    def is_timeout(self) -> Bool:
        """True when a DEADLINE branch won (issue #92; real elapsed-clock
        wiring is #91)."""
        return self.index != -1 and self.kind == SelectBranch.DEADLINE

    def is_closed(self) -> Bool:
        """True when a RECV branch won by observing close-and-empty
        (issue #93; never true for SEND/DEADLINE)."""
        return self.index != -1 and self.kind == SelectBranch.RECV and self.closed

    def is_sent(self) -> Bool:
        """True when a SEND branch won and the handoff succeeded."""
        return self.index != -1 and self.kind == SelectBranch.SEND and self.sent

    def has_value(self) -> Bool:
        return Bool(self.value)

    def recv_value(self) raises -> Self.T:
        """The delivered value for a winning RECV branch with data.
        Raises for any other outcome shape (timeout/closed/send/pending) —
        callers check `is_pending()`/`is_closed()`/`is_timeout()` first."""
        if self.index == -1 or self.kind != SelectBranch.RECV or self.closed:
            raise Error("SelectError: outcome carries no recv value")
        if not self.value:
            raise Error("SelectError: outcome carries no recv value")
        return self.value.value()


# ---------------------------------------------------------------------------
# Winner claim — perform the concrete channel operation for branches[idx]
# ---------------------------------------------------------------------------

def _claim_at[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    branches: List[SelectBranch], idx: Int
) raises -> SelectOutcome[T]:
    var b = branches[idx]
    var out = SelectOutcome[T]()
    out.index = idx
    out.kind = b.kind
    if b.kind == SelectBranch.RECV:
        var chan = UnsafePointer[Channel[T], MutAnyOrigin](
            unsafe_from_address=b.chan_addr
        )
        var v = chan[].try_recv()
        if v:
            out.value = v
        else:
            out.closed = True
        return out^
    if b.kind == SelectBranch.SEND:
        var chan = UnsafePointer[Channel[T], MutAnyOrigin](
            unsafe_from_address=b.chan_addr
        )
        var item = UnsafePointer[T, MutAnyOrigin](unsafe_from_address=b.item_addr)
        out.sent = chan[].try_send(item[])
        return out^
    # DEADLINE: no channel operation — the winning index + kind alone is the
    # timeout signal (is_timeout()).
    return out^


def _unregister_others[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable](
    branches: List[SelectBranch], task_id: Int, winner_idx: Int
) raises:
    """Logical cancellation (issue #92 step 3): remove this task's WaitRecord
    from every branch except the winner.  A no-op per branch when this task
    was never registered there (select_fast/select's own fast attempt never
    registers, so this is cheap and idempotent either way)."""
    for i in range(len(branches)):
        if i == winner_idx:
            continue
        var b = branches[i]
        if b.kind == SelectBranch.RECV:
            UnsafePointer[Channel[T], MutAnyOrigin](
                unsafe_from_address=b.chan_addr
            )[].unregister_receiver(task_id)
        elif b.kind == SelectBranch.SEND:
            UnsafePointer[Channel[T], MutAnyOrigin](
                unsafe_from_address=b.chan_addr
            )[].unregister_sender(task_id)


def _cancel_armed_timer(
    branches: List[SelectBranch],
    mut state: SelectState,
    task_id: Int,
    closed_recv_win: Bool = False,
) raises:
    """Timer/data cancellation symmetry (issue #91), WITH the issue #85
    close exception: once a branch delivering a REAL operation (data
    recv'd, data sent, or the DEADLINE branch itself) is claimed, drop a
    still-armed deadline timer from the heap so a channel winner never
    leaves a stale entry behind — the reverse direction (a deadline winner
    leaving channel registrations behind) is already covered by
    `_unregister_others`.

    EXCEPTION (issue #85 "close + timer semantics"): a RECV branch winning
    by observing CLOSE-AND-EMPTY (`closed_recv_win=True`) is NOT a real
    operation completing — it must "fail only the channel branch" while
    "the timer keeps its own lifetime" (no implicit cancellation just
    because the channel closed, spec §38/39).  Callers pass
    `closed_recv_win=out.kind == SelectBranch.RECV and out.closed`; every
    other outcome shape cancels exactly as issue #91 specifies.  A timer
    left armed here fires no duplicate wake later: `service_timers` only
    resumes a task still WAITING, and this task already completed via the
    close winner (`time/timer_service.mojo`).

    No-op when no timer was armed this cycle, or when the DEADLINE branch
    itself won and the timer service already popped its own entry
    (`cancel_token` on a missing entry is a documented no-op,
    `time/timer_heap.mojo`)."""
    if closed_recv_win:
        return
    if not state._timer_armed:
        return
    for i in range(len(branches)):
        var b = branches[i]
        if b.kind == SelectBranch.DEADLINE and b.heap_addr != 0:
            var heap = UnsafePointer[TimerHeap, MutAnyOrigin](
                unsafe_from_address=b.heap_addr
            )
            _ = heap[].cancel_token(task_id, state._timer_gen)
            state._timer_armed = False
            return


# ---------------------------------------------------------------------------
# select_fast — non-parking try-scan (issue #92/#93)
# ---------------------------------------------------------------------------

def select_fast[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable, R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    branches: List[SelectBranch],
    mut state: SelectState,
    now_ticks: Int = -1,
) raises -> SelectOutcome[T]:
    """Claim the first already-ready branch; NEVER parks (issue #92/#93).
    `rt` is accepted for API symmetry with `select()` and is otherwise
    unused (no park path exists here).  On a claim, defensively unregisters
    this task from every other branch — a no-op unless a PRIOR `select()`
    call on the same task/branches parked and is now being polled instead
    of re-driven (issue #93 "ZERO leftovers")."""
    var idx = rescan[T](branches, state, now_ticks)
    if idx == -1:
        state.winner = -1
        return SelectOutcome[T]()
    var out = _claim_at[T](branches, idx)
    _unregister_others[T](branches, h.id(), idx)
    _cancel_armed_timer(
        branches, state, h.id(), out.kind == SelectBranch.RECV and out.closed
    )
    state.winner = idx
    return out^


# ---------------------------------------------------------------------------
# select — one-shot select, may register + park once (issue #92)
# ---------------------------------------------------------------------------

def select[T: Movable & ImplicitlyCopyable & ImplicitlyDeletable, R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    branches: List[SelectBranch],
    mut state: SelectState,
    now_ticks: Int = -1,
) raises -> SelectOutcome[T]:
    """One-shot colorless select (issue #92), mirroring Channel.send/recv's
    own "attempt once, maybe park" discipline exactly:

    Fast attempt first (same rescan select_fast uses) — never parks when a
    branch is already claimable.  Otherwise registers the CURRENT task as a
    single waiter across every genuinely BLOCKED branch (register_receiver/
    register_sender, idempotent + FIFO-deduped, #35 unchanged) and parks
    ONCE via the canonical `park_current` (#39).  Raises if NO branch is
    either claimable or blockable (e.g. every branch SKIP/empty) — parking
    there would hang forever with nothing able to wake this task, which is
    a caller programming error, not a runtime state to represent silently.

    On resume the embedding driver re-enters select() with the SAME
    branches; the fresh rescan takes the fast path (the wake came from
    exactly one branch) and this call's claim step unregisters the task
    from every OTHER branch (logical cancellation, #92 step 3) so a losing
    branch's later real wake can never double-enqueue this task.

    Caller-side discipline mirrors recv()'s Optional[T]() sentinel: when
    this call parks, the returned SelectOutcome is a meaningless pending
    placeholder — check `h.state()` (WAITING) BEFORE trusting an outcome
    with `is_pending() == True`."""
    var idx = rescan[T](branches, state, now_ticks)
    if idx != -1:
        var out = _claim_at[T](branches, idx)
        _unregister_others[T](branches, h.id(), idx)
        _cancel_armed_timer(
            branches, state, h.id(), out.kind == SelectBranch.RECV and out.closed
        )
        state.winner = idx
        return out^
    var any_blocked = False
    for i in range(len(branches)):
        var b = branches[i]
        var cls = classify_branch[T](b, now_ticks)
        if cls == BLOCKED:
            any_blocked = True
            if b.kind == SelectBranch.RECV:
                UnsafePointer[Channel[T], MutAnyOrigin](
                    unsafe_from_address=b.chan_addr
                )[].register_receiver(h)
            elif b.kind == SelectBranch.SEND:
                UnsafePointer[Channel[T], MutAnyOrigin](
                    unsafe_from_address=b.chan_addr
                )[].register_sender(h)
            elif b.kind == SelectBranch.DEADLINE and b.heap_addr != 0:
                # Real timer-integrated DEADLINE branch (issue #91): arm
                # the caller's heap so the timer service (time/
                # timer_service.service_timers) can wake THIS task when no
                # channel branch becomes ready first — the exact arm call
                # time/sleep.mojo#sleep_current uses.  A bare #92 ticks-
                # only descriptor (heap_addr == 0) still cannot wake on its
                # own; unchanged from #92.
                var heap = UnsafePointer[TimerHeap, MutAnyOrigin](
                    unsafe_from_address=b.heap_addr
                )
                var gen = heap[].arm(
                    h.id(), Int(h.tcb()), UInt64(b.deadline_ticks)
                )
                state._timer_armed = True
                state._timer_gen = gen
                state._timer_deadline = UInt64(b.deadline_ticks)
    if not any_blocked:
        raise Error(
            "SelectError: no branch is ready, closed, or blockable "
            "(select would park forever)"
        )
    state.winner = -1
    # Two-phase park (issue #148): NOT the single-phase park_current.
    # A channel sender/receiver that arrives after the FIFO registrations
    # above but before WAITING commits calls unpark_current on a RUNNING or
    # PARKING task; park_current never consulted the early-wake latch, so
    # that wake was silently dropped.  park_validate re-checks the latch and
    # park_commit unwinds to RUNNABLE so the re-scan below claims the branch
    # the wake signalled was ready (A0-T11 / issue #148).
    var park_reason = SuspendReason.TIMER if state._timer_armed else SuspendReason.PARK
    park_prepare(h)
    if park_validate(h):
        _ = park_commit(h)
        claim_running(h)
        var idx2 = rescan[T](branches, state, now_ticks)
        if idx2 != -1:
            var out2 = _claim_at[T](branches, idx2)
            _unregister_others[T](branches, h.id(), idx2)
            _cancel_armed_timer(
                branches, state, h.id(), out2.kind == SelectBranch.RECV and out2.closed
            )
            state.winner = idx2
            return out2^
        return SelectOutcome[T]()
    if not park_commit(h, park_reason):
        claim_running(h)
        var idx2 = rescan[T](branches, state, now_ticks)
        if idx2 != -1:
            var out2 = _claim_at[T](branches, idx2)
            _unregister_others[T](branches, h.id(), idx2)
            _cancel_armed_timer(
                branches, state, h.id(), out2.kind == SelectBranch.RECV and out2.closed
            )
            state.winner = idx2
            return out2^
        return SelectOutcome[T]()
    return SelectOutcome[T]()
