# mojito_async/channel/select.mojo
#
# A5.4 (issue #92) multi-wait select primitive + A5.5 (issue #93) close/
# select behavior and try-scan fairness (spec §42, Phase A5).
#
# select() waits on several channel recv/send operations (and a forward-
# compatible deadline branch — the actual timer wake is A6/#91, out of
# scope here) and resumes on exactly ONE winner; the losing branches are
# logically cancelled and the whole scheme composes with the C6 generation-
# claim / park kernel unchanged (EPIC #4, #56/#58): select never invents a
# second park/wake mechanism, it only fans ONE task's WaitRecord out across
# several channels' existing FIFO wait queues and collapses back to one via
# unregister_* once a winner is claimed.
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
# Mojo 1.0.0b2 workarounds: def-only, module factories (no static methods);
# SelectBranch/SelectState/SelectOutcome are pure DATA, no function-typed
# fields; dispatch on `kind` is a plain Int switch throughout (the repo-wide
# convention); no externs (pure-Mojo package, no C-ABI firewall needed).
from std.collections import List
from mojito_async.channel.channel import Channel
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.runtime.park import park_current
from mojito_async.task import JoinHandle


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
    # supplied `now_ticks` (0 otherwise; RECV/SEND never touch it).  No
    # timer/reactor exists yet (A6/#84-88, A7/#75-83 — not this wave), so a
    # DEADLINE branch with no `now_ticks` context simply never classifies
    # READY; it exists so #91's follow-on can wire a real clock without
    # changing this descriptor shape.
    var deadline_ticks: Int
    var kind: Int

    def __init__(out self, kind: Int, chan_addr: Int, item_addr: Int, deadline_ticks: Int):
        self.kind = kind
        self.chan_addr = chan_addr
        self.item_addr = item_addr
        self.deadline_ticks = deadline_ticks


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

    def __init__(out self):
        self.winner = -1
        self._rescan_cursor = 0

    def cursor(self) -> Int:
        return self._rescan_cursor

    def reset(mut self):
        """Clear the winner cell for a fresh select cycle (the cursor is
        NOT reset — fairness rotation is meant to persist across cycles)."""
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
            # DEADLINE BLOCKED: no wait queue to register into yet (#91/A6
            # timer wiring is out of scope here); the branch simply cannot
            # wake this task on its own until that lands.
    if not any_blocked:
        raise Error(
            "SelectError: no branch is ready, closed, or blockable "
            "(select would park forever)"
        )
    state.winner = -1
    park_current(rt, h)
    return SelectOutcome[T]()
