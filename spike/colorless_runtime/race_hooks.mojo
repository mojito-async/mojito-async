# spike/colorless_runtime/race_hooks.mojo
#
# A0.8 (issue #17) — deterministic race hooks over the park pipeline.
#
# Models the two-phase park pipeline (spec §24/§25, A0-T10..T12) so races
# can be forced deterministically in pure Mojo, without fibers or the
# dylib (integration with the real Event/park path lands in the A0.7 fold;
# the interfaces here are per docs/A0_PLAN.md §3):
#
#     PREPARE -> VALIDATE -> COMMIT(sleep) -> WAKE
#         `-- settle() after every boundary: cancellation first, then
#             readiness; a decided race ends the pipeline early.
#
#   - wake-before-park (T11): readiness delivered at VALIDATE time ->
#     settle() returns before COMMIT; the task never sleeps.
#   - double-wake (T12): readiness delivery is generation-guarded — one
#     accepted edge per park epoch (`_woke_epoch` vs `_epoch`); a second
#     WAKE in the same epoch is a counted but rejected no-op.
#   - cancel-vs-ready (T10): both sides fire inside one schedule; settle()
#     gives cancellation precedence and WinnerRecord guarantees exactly
#     ONE winner regardless of how many losing attempts were made.
#
# INJECTABLE-HOOK MECHANISM (b2 constraint, documented per lane brief):
# The natural design is an injectable callback invoked at each boundary,
# stored in a registry.  Mojo 1.0.0b2 forbids both halves of that design:
# function-typed struct fields are rejected ("trait types"), module-level
# mutable globals are forbidden by the spike rules, and S0 already proved
# the workaround: pass explicit state through values/pointers (the userdata
# pattern) instead of ambient registries.  This module therefore injects
# *data*, not code: a HookScript binds a bounded action list to each
# HookPoint boundary, and run_park_pipeline() interprets it.  A test forces
# a schedule by scripting actions (REQUEST_CANCEL / SET_READY / WAKE) into
# boundaries — deterministic by construction, no hidden state.

from cancel import CancelFlag


# ---------------------------------------------------------------------------
# HookPoint + actions (b2 removed `enum`; comptime-Int namespace struct)
# ---------------------------------------------------------------------------

struct HookPoint:
    """Park-pipeline boundaries. Enum stand-in: b2 has no `enum`, so the
    members are comptime Int constants on a namespace struct."""

    comptime PREPARE = Int(0)
    comptime VALIDATE = Int(1)
    comptime COMMIT = Int(2)
    comptime WAKE = Int(3)
    comptime COUNT = Int(4)


struct HookAction:
    """Scripted actions fired at a boundary. SET_READY and WAKE share the
    generation-guarded delivery path: readiness from Event.set() and a
    scheduler resume are the same edge for this harness."""

    comptime NOOP = Int(0)
    comptime REQUEST_CANCEL = Int(1)
    comptime SET_READY = Int(2)
    comptime WAKE = Int(3)


# ---------------------------------------------------------------------------
# ActionList + HookScript (bounded, allocation-free)
# ---------------------------------------------------------------------------

struct ActionList(ImplicitlyCopyable, ImplicitlyDeletable):
    """Fixed-capacity action list for one boundary (spike schedules are
    tiny; b2's InlineArray fill-init is unusable here)."""

    comptime CAPACITY = Int(4)

    var _n: Int
    var _a0: Int
    var _a1: Int
    var _a2: Int
    var _a3: Int

    def __init__(out self):
        self._n = 0
        self._a0 = HookAction.NOOP
        self._a1 = HookAction.NOOP
        self._a2 = HookAction.NOOP
        self._a3 = HookAction.NOOP

    def push(mut self, action: Int) raises:
        if action == HookAction.NOOP:
            raise Error("HookScript: explicit NOOP push is not allowed")
        if self._n >= Self.CAPACITY:
            raise Error("HookScript: action capacity exceeded")
        if self._n == 0:
            self._a0 = action
        elif self._n == 1:
            self._a1 = action
        elif self._n == 2:
            self._a2 = action
        else:
            self._a3 = action
        self._n += 1

    def size(self) -> Int:
        return self._n

    def at(self, i: Int) -> Int:
        if i == 0:
            return self._a0
        if i == 1:
            return self._a1
        if i == 2:
            return self._a2
        if i == 3:
            return self._a3
        return HookAction.NOOP


struct HookScript(ImplicitlyCopyable, ImplicitlyDeletable):
    """The injected schedule: one ActionList per HookPoint boundary."""

    var prepare: ActionList
    var validate: ActionList
    var commit: ActionList
    var wake: ActionList

    def __init__(out self):
        self.prepare = ActionList()
        self.validate = ActionList()
        self.commit = ActionList()
        self.wake = ActionList()


def make_hook_script() -> HookScript:
    """Empty script: the pipeline runs all four boundaries undisturbed."""
    return HookScript()


def script_add(mut s: HookScript, point: Int, action: Int) raises:
    """Append `action` to the list fired at boundary `point`."""
    if point == HookPoint.PREPARE:
        s.prepare.push(action)
    elif point == HookPoint.VALIDATE:
        s.validate.push(action)
    elif point == HookPoint.COMMIT:
        s.commit.push(action)
    elif point == HookPoint.WAKE:
        s.wake.push(action)
    else:
        raise Error("script_add: unknown HookPoint")


def _list_at(s: HookScript, point: Int) -> ActionList:
    """Read view of the boundary's action list (copy; read-only use)."""
    if point == HookPoint.PREPARE:
        return s.prepare
    if point == HookPoint.VALIDATE:
        return s.validate
    if point == HookPoint.COMMIT:
        return s.commit
    return s.wake


# ---------------------------------------------------------------------------
# WinnerRecord (exactly-one-winner invariant)
# ---------------------------------------------------------------------------

struct WinnerRecord(ImplicitlyCopyable, ImplicitlyDeletable):
    """Race outcome ledger for the cancel-vs-ready pipeline.

    record(kind) counts the attempt AND claims the single winner slot only
    when undecided; later (losing) attempts are recorded but never move the
    winner.  exactly_one_winner(): decided AND exactly one winning claim —
    the invariant A0-T10 asserts.
    """

    comptime NONE = Int(0)
    comptime CANCEL = Int(1)
    comptime READY = Int(2)

    var _winner: Int
    var _wins: Int
    var _att_cancel: Int
    var _att_ready: Int

    def __init__(out self):
        self._winner = Self.NONE
        self._wins = 0
        self._att_cancel = 0
        self._att_ready = 0

    def decided(self) -> Bool:
        return self._winner != Self.NONE

    def winner(self) -> Int:
        return self._winner

    def attempts(self, kind: Int) -> Int:
        if kind == Self.CANCEL:
            return self._att_cancel
        if kind == Self.READY:
            return self._att_ready
        return 0

    def record_attempt(mut self, kind: Int) raises:
        """Count an attempt without winner eligibility (losing deliveries)."""
        self._count(kind)
        # An attempt on a side whose delivery was generation-rejected still
        # counts as an attempt; nothing else to do.

    def record(mut self, kind: Int) raises:
        """Count the attempt and claim the winner slot if still open."""
        self._count(kind)
        if not self.decided():
            self._winner = kind
            self._wins += 1

    def exactly_one_winner(self) -> Bool:
        return self.decided() and self._wins == 1

    def _count(mut self, kind: Int) raises:
        if kind == Self.CANCEL:
            self._att_cancel += 1
        elif kind == Self.READY:
            self._att_ready += 1
        else:
            raise Error("WinnerRecord: unknown winner kind")


# ---------------------------------------------------------------------------
# RaceContext + park-pipeline driver
# ---------------------------------------------------------------------------

struct RaceContext(ImplicitlyCopyable, ImplicitlyDeletable):
    """State of one modeled park epoch (single worker; plain Bool/Int).

    Generation guard: `_epoch` is claimed when the pipeline commits to
    sleep; `_woke_epoch` records the last epoch with an ACCEPTED readiness
    edge.  Delivery while `_woke_epoch == _epoch` is a duplicate wake —
    counted, never re-delivered (spec §25 idempotent-wake rule).
    """

    var winners: WinnerRecord
    var _flag: CancelFlag
    var _ready_pending: Bool
    var _epoch: Int
    var _woke_epoch: Int
    var _sleeping: Bool
    var _slept: Bool
    var _wake_attempts: Int
    var _wakes_accepted: Int
    var _fired_prepare: Int
    var _fired_validate: Int
    var _fired_commit: Int
    var _fired_wake: Int

    def __init__(out self, flag: CancelFlag):
        self.winners = WinnerRecord()
        self._flag = flag
        self._ready_pending = False
        self._epoch = 0
        self._woke_epoch = -1
        self._sleeping = False
        self._slept = False
        self._wake_attempts = 0
        self._wakes_accepted = 0
        self._fired_prepare = 0
        self._fired_validate = 0
        self._fired_commit = 0
        self._fired_wake = 0

    # --- queries -----------------------------------------------------------

    def cancelled(self) -> Bool:
        return self._flag.is_requested()

    def slept(self) -> Bool:
        return self._slept

    def sleeping_now(self) -> Bool:
        return self._sleeping

    def wake_attempts(self) -> Int:
        return self._wake_attempts

    def wakes_accepted(self) -> Int:
        return self._wakes_accepted

    def hooks_fired(self, point: Int) -> Int:
        if point == HookPoint.PREPARE:
            return self._fired_prepare
        if point == HookPoint.VALIDATE:
            return self._fired_validate
        if point == HookPoint.COMMIT:
            return self._fired_commit
        if point == HookPoint.WAKE:
            return self._fired_wake
        return 0

    # --- internals ---------------------------------------------------------

    def _fire(mut self, point: Int):
        if point == HookPoint.PREPARE:
            self._fired_prepare += 1
        elif point == HookPoint.VALIDATE:
            self._fired_validate += 1
        elif point == HookPoint.COMMIT:
            self._fired_commit += 1
        elif point == HookPoint.WAKE:
            self._fired_wake += 1


def _deliver_ready(mut ctx: RaceContext) raises:
    """Generation-guarded readiness delivery (SET_READY and WAKE alike).

    Counts the attempt, then accepts at most one edge per park epoch.
    """
    ctx._wake_attempts += 1
    if ctx._woke_epoch == ctx._epoch:
        return  # duplicate wake for this generation: rejected no-op
    ctx._woke_epoch = ctx._epoch
    ctx._wakes_accepted += 1
    ctx._ready_pending = True
    ctx._sleeping = False
    ctx.winners.record_attempt(WinnerRecord.READY)


def _apply_action(mut ctx: RaceContext, action: Int) raises:
    if action == HookAction.NOOP:
        pass
    elif action == HookAction.REQUEST_CANCEL:
        ctx._flag.request()
    elif action == HookAction.SET_READY:
        _deliver_ready(ctx)
    elif action == HookAction.WAKE:
        _deliver_ready(ctx)
    else:
        raise Error("race_hooks: unknown scripted action")


def _run_script(mut ctx: RaceContext, actions: ActionList) raises:
    var i = 0
    while i < actions.size():
        _apply_action(ctx, actions.at(i))
        i += 1


def _settle(mut ctx: RaceContext) raises -> Bool:
    """Decide the race after a boundary, deterministically.

    Cancellation takes precedence: a cooperative cancel must never be lost
    behind a ready result (the join path re-checks cancellation after
    resume).  Readiness is consumed when it wins.
    """
    if ctx._flag.is_requested():
        ctx.winners.record(WinnerRecord.CANCEL)
        return True
    if ctx._ready_pending:
        ctx._ready_pending = False
        ctx.winners.record(WinnerRecord.READY)
        return True
    return False


def run_park_pipeline(mut ctx: RaceContext, hooks: HookScript) raises:
    """Drive one modeled park epoch under the injected schedule.

    PREPARE -> VALIDATE -> COMMIT -> WAKE, settling after each boundary;
    the pipeline stops at the first decided outcome.  With no cancellation
    and no delivered readiness the pipeline parks through all four
    boundaries (undecided; the real Event/park fold completes this path).
    """
    # Phase 1: PREPARE (stamp wait reason, arm the node).
    ctx._fire(HookPoint.PREPARE)
    _run_script(ctx, _list_at(hooks, HookPoint.PREPARE))
    if _settle(ctx):
        return

    # Phase 2: VALIDATE — the lost-wakeup window.  Readiness delivered HERE
    # is honored before the sleep, so the task never parks (T11).
    ctx._fire(HookPoint.VALIDATE)
    _run_script(ctx, _list_at(hooks, HookPoint.VALIDATE))
    if _settle(ctx):
        return

    # Phase 3: COMMIT — claim the park epoch and "sleep".
    ctx._epoch += 1
    ctx._sleeping = True
    ctx._slept = True
    ctx._fire(HookPoint.COMMIT)
    _run_script(ctx, _list_at(hooks, HookPoint.COMMIT))
    if _settle(ctx):
        return

    # Phase 4: WAKE boundary (scheduler resume path).
    ctx._fire(HookPoint.WAKE)
    _run_script(ctx, _list_at(hooks, HookPoint.WAKE))
    _ = _settle(ctx)
