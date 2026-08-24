# spike/colorless_runtime/event.mojo
#
# A0.7 (issue #16) — Event-based park/wake with two-phase parking.
#
# Pure Mojo (no dylib, no fibers, no externs): an Event is the readiness
# cell a task parks on; the park pipeline drives the REAL TaskControlBlock
# state machine from task.mojo through the four spec §23 phases:
#
#     PREPARE  publish the waiter as DATA: the task id plus a pointer to
#              the TCB's EMBEDDED WaitNode (allocation-free park); settle
#     VALIDATE recheck readiness — the lost-wakeup window; readiness here
#              returns from wait() WITHOUT sleeping and WITHOUT a
#              generation bump (the task never left RUNNING)   [A0-T11]
#     COMMIT   RUNNING -> PARKING opens the early-wake window; the COMMIT
#              scripts fire INSIDE it, then settle (cancellation first):
#              readiness/cancel unwinds via PARKING -> RUNNABLE and
#              WAITING is never entered; otherwise PARKING -> WAITING
#              bumps the generation and stamps the embedded node
#     WAKE     Event.set() claims the waiter's generation EXACTLY ONCE,
#              records ONE enqueue, and resumes the waiter via
#              try_transition(WAITING -> RUNNABLE)                [A0-T12]
#
# Protocol outcomes x causes (asserted by tests/t10_event_protocol.mojo):
#
#   phase    | readiness (set)            | cancel (CancelFlag)
#   ---------+----------------------------+----------------------------------
#   COMMIT   | EARLY_WAKE @COMMIT via the | CANCELLED @COMMIT: unwound to
#            | PARKING->RUNNABLE early-   | RUNNABLE without sleeping;
#            | wake edge (never WAITING,  | a losing READY is counted as
#            | no gen bump, no enqueue)   | an attempt, never the winner
#   WAKE     | PARKED_AND_WOKE @WAKE: one | (cancel alone leaves the task
#            | enqueue at the fresh gen,  | blocked in WAITING; the cancel
#            | WAITING->RUNNABLE          | resume edge lands with A0.8)
#   neither  | UNDECIDED — parked in WAITING (a real scheduler blocks here;
#            | the spike documents the state instead of blocking)
#
# Settle policy (mirrors race_hooks.settle()): after every boundary,
# cancellation is checked FIRST, then readiness; a decided race ends the
# pipeline early and WinnerRecord guarantees exactly ONE winner (A0-T10).
#
# INJECTABLE-TARGET MECHANISM (b2 constraint, documented per lane brief):
# The natural design stores a wake/enqueue CALLBACK in the Event.  Mojo
# 1.0.0b2 forbids function-typed struct fields ("trait types") and
# module-level mutable globals, so — exactly like race_hooks.mojo's
# HookScript — this module injects DATA, not code:
#
#   - PREPARE publishes the waiter identity INTO the Event: the task id
#   - set() appends each accepted delivery to a bounded enqueue log owned
#     by the Event (the "callback" is a data record + counter); tests
#     assert on log LENGTH/contents for the exactly-once-per-generation
#     guarantee (A0-T12).
#   - The TCB resume itself is a generic module-level function
#     (deliver_wake[T]) over TaskControlBlock — compile-time injection of
#     the transition target, not a stored fn value.
#
# Cancellation integration seam: park_pipeline consumes CancelFlag queries
# (is_requested) under the race_hooks settle policy; the cooperative
# checkpoint surface (CancellationError raise on resume) is wired by the
# A0.8 fold — this module only guarantees the WINNER precedence.
#
# Mojo 1.0.0b2 dialect notes (same conventions as task.mojo / cancel.mojo):
#   - `def` only; mutable free-function params take `mut`; ctors `out self`.
#   - `comptime NAME = Int(...)` namespace structs instead of enums.
#   - Only builtin `Error` can be raised.

from cancel import CancelFlag
from race_hooks import ActionList, HookAction, HookPoint, HookScript, WinnerRecord
from task import ResultValue, TaskControlBlock, WaitNode


struct IntResult(ResultValue):
    """Concrete result slot for spike drivers (unit-style results).

    TOOLCHAIN NOTE (b2): the generic pipeline below is exported for real
    callers, but every generic CALLSITE in a driver multiplies compiler
    specialization work (observed: superlinear-to-exponential `mojo`
    compile times when a test main() calls the generic pipeline a dozen
    times).  The concrete aliases/wrappers below pin ONE instantiation;
    tests/driver code should use `SpikeTcb` + `park_pipeline_spike` +
    `deliver_wake_spike`.
    """

    var v: Int

    def __init__(out self):
        self.v = 0


comptime SpikeTcb = TaskControlBlock[IntResult]


def park_pipeline_spike(
    mut ev: Event,
    tp: UnsafePointer[SpikeTcb, MutAnyOrigin],
    fp: UnsafePointer[CancelFlag, MutAnyOrigin],
    hooks: HookScript,
    mut report: ParkReport,
) raises:
    """Concrete (single-instantiation) park pipeline for spike drivers."""
    park_pipeline(ev, tp, fp, hooks, report)


def deliver_wake_spike(
    mut ev: Event,
    tp: UnsafePointer[SpikeTcb, MutAnyOrigin],
) raises -> Bool:
    """Concrete (single-instantiation) WAKE leg for spike drivers."""
    return deliver_wake(ev, tp)


# ---------------------------------------------------------------------------
struct WaitReason:
    """Why the task waits — stamped on the embedded WaitNode at PREPARE.
    Open set (task.mojo leaves it to scheduler lanes); enum stand-in."""

    comptime EVENT = Int(0)
    comptime CANCEL = Int(1)


# ---------------------------------------------------------------------------
# ParkOutcome + ParkReport (pipeline verdict ledger)
# ---------------------------------------------------------------------------

struct ParkOutcome:
    """Pipeline verdicts. Enum stand-in (b2 has no `enum`)."""

    comptime PARKED_AND_WOKE = Int(0)
    comptime EARLY_WAKE = Int(1)
    comptime CANCELLED = Int(2)
    comptime UNDECIDED = Int(3)


struct ParkReport(ImplicitlyCopyable, ImplicitlyDeletable):
    """What one park_pipeline run did (read-only for callers).

    outcome     ParkOutcome verdict.
    decided_at  HookPoint boundary that decided the race (-1 if undecided).
    slept       True once COMMIT claimed a wait epoch (WAITING entry).
    winners     race_hooks WinnerRecord: exactly-one-winner ledger.
    """

    var outcome: Int
    var decided_at: Int
    var slept: Bool
    var winners: WinnerRecord

    def __init__(out self):
        self.outcome = ParkOutcome.UNDECIDED
        self.decided_at = -1
        self.slept = False
        self.winners = WinnerRecord()


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------

struct Event(ImplicitlyCopyable, ImplicitlyDeletable):
    """Readiness cell + per-generation wake guard (spec §23/§25).

    State:
      _latched      sticky readiness: set() latches it, the consuming wait
                    (early-wake settle) clears it.
      _published    a waiter was published (PREPARE ran): node pointer valid.
      _parked       the published waiter COMMITTED to WAITING (node carries
                    the fresh epoch generation); deliveries may claim/enqueue.
      _waiter_id    published task identity (data, not code).
      _node         pointer at the TCB's EMBEDDED WaitNode — generation is
                    read AT DELIVERY TIME, so post-publish WAITING entry is
                    observed without re-registration.
      _claimed_gen  last generation an accepted set() claim; duplicate
                    defense for the same epoch (A0-T12).

    Generation discipline: every WAITING entry bumps the TCB generation and
    re-stamps the embedded node (task.mojo._apply), so each park cycle has
    its own epoch; set_at(g) accepts ONLY g == current node generation AND
    g not yet claimed — stale-gen wakes and same-gen duplicates are counted
    attempts but rejected no-ops (nothing enqueued).
    """

    comptime LOG_CAPACITY = Int(8)

    var _latched: Bool
    var _published: Bool
    var _parked: Bool
    var _waiter_id: Int
    var _node: Optional[UnsafePointer[WaitNode, MutAnyOrigin]]
    var _claimed_gen: Int
    var _set_attempts: Int
    var _wakes_accepted: Int
    # bounded enqueue log (generations accepted, oldest first)
    var _log_n: Int
    var _log_dropped: Int
    var _log_0: Int
    var _log_1: Int
    var _log_2: Int
    var _log_3: Int
    var _log_4: Int
    var _log_5: Int
    var _log_6: Int
    var _log_7: Int

    def __init__(out self):
        self._latched = False
        self._published = False
        self._parked = False
        self._waiter_id = -1
        self._node = Optional[UnsafePointer[WaitNode, MutAnyOrigin]]()
        self._claimed_gen = -1
        self._set_attempts = 0
        self._wakes_accepted = 0
        self._log_n = 0
        self._log_dropped = 0
        self._log_0 = -1
        self._log_1 = -1
        self._log_2 = -1
        self._log_3 = -1
        self._log_4 = -1
        self._log_5 = -1
        self._log_6 = -1
        self._log_7 = -1

    # --- waiter publication (PREPARE, data injection) -----------------------

    def publish(mut self, waiter_id: Int, node: UnsafePointer[WaitNode, MutAnyOrigin]):
        """Publish the waiter as DATA (task id + embedded-node pointer).

        Opens a new park epoch: the previous epoch's claim is forgotten so
        a fresh generation can be claimed again.
        """
        self._waiter_id = waiter_id
        self._node = Optional[UnsafePointer[WaitNode, MutAnyOrigin]](node)
        self._published = True
        self._parked = False
        self._claimed_gen = -1

    def arm_parked(mut self):
        """COMMIT side: the published waiter entered WAITING — the node now
        carries the fresh epoch generation; deliveries may claim it."""
        self._parked = True

    # --- readiness ----------------------------------------------------------

    def set(mut self) raises -> Bool:
        """Latch readiness and attempt a delivery at the waiter's CURRENT
        generation.  Returns True iff a wake was ACCEPTED."""
        return self.set_at(self.current_generation())

    def set_at(mut self, generation: Int) raises -> Bool:
        """Delivery targeting `generation` (spec §25 wake rule):

        accept iff a waiter is PARKED, `generation` equals the waiter's
        CURRENT (fresh) generation, and that generation was not already
        claimed.  Every call counts as an attempt; rejects are silent
        no-ops that enqueue NOTHING (A0-T12)."""
        self._set_attempts += 1
        self._latched = True
        if not self._parked or not self._published:
            return False  # nobody committed to sleep: pure latch
        var cur = self.current_generation()
        if generation != cur:
            return False  # stale-generation wake: ignored
        if self._claimed_gen == cur:
            return False  # duplicate wake for this generation
        # Enqueue FIRST (lossy ring cannot fail), THEN claim the generation:
        # the WAKE path can never abort between claiming and resuming
        # (modular review fold, PR #29).
        self._log_enqueue(cur)
        self._claimed_gen = cur
        self._wakes_accepted += 1
        return True

    def clear_latch(mut self):
        """Wait-side consume: the pending readiness was observed."""
        self._latched = False

    def is_set(self) -> Bool:
        return self._latched

    # --- queries ------------------------------------------------------------

    def waiter_id(self) -> Int:
        return self._waiter_id

    def is_parked(self) -> Bool:
        return self._parked

    def claimed_gen(self) -> Int:
        return self._claimed_gen

    def set_attempts(self) -> Int:
        return self._set_attempts

    def wakes_accepted(self) -> Int:
        return self._wakes_accepted

    def current_generation(self) -> Int:
        """The published waiter's live generation (-1 if none published)."""
        if self._published:
            if self._node:
                return self._node.value()[].generation()
        return -1

    def enqueue_len(self) -> Int:
        return self._log_n

    def enqueued_gen(self, i: Int) raises -> Int:
        if i < 0 or i >= self._log_n:
            raise Error("Event.enqueued_gen: index out of range")
        if i == 0:
            return self._log_0
        if i == 1:
            return self._log_1
        if i == 2:
            return self._log_2
        if i == 3:
            return self._log_3
        if i == 4:
            return self._log_4
        if i == 5:
            return self._log_5
        if i == 6:
            return self._log_6
        return self._log_7

    # --- internals ----------------------------------------------------------

    def _log_enqueue(mut self, gen: Int):
        """Lossy diagnostic ring: at capacity the OLDEST entry is dropped
        and _log_dropped counts the loss.  NEVER raises -- a diagnostics
        buffer must not fail (or wedge) the WAKE path after the generation
        has been claimed (modular review fold, PR #29)."""
        if self._log_n >= Self.LOG_CAPACITY:
            # shift down, drop oldest
            self._log_0 = self._log_1
            self._log_1 = self._log_2
            self._log_2 = self._log_3
            self._log_3 = self._log_4
            self._log_4 = self._log_5
            self._log_5 = self._log_6
            self._log_6 = self._log_7
            self._log_n = Self.LOG_CAPACITY - 1
            self._log_dropped += 1
        if self._log_n == 0:
            self._log_0 = gen
        elif self._log_n == 1:
            self._log_1 = gen
        elif self._log_n == 2:
            self._log_2 = gen
        elif self._log_n == 3:
            self._log_3 = gen
        elif self._log_n == 4:
            self._log_4 = gen
        elif self._log_n == 5:
            self._log_5 = gen
        elif self._log_n == 6:
            self._log_6 = gen
        else:
            self._log_7 = gen
        self._log_n += 1


# ---------------------------------------------------------------------------
# WAKE leg: generic resume over the real TCB (compile-time injection)
# ---------------------------------------------------------------------------

def deliver_wake[T: ResultValue](
    mut ev: Event,
    tp: UnsafePointer[TaskControlBlock[T], MutAnyOrigin],
) raises -> Bool:
    """Claim the waiter's generation exactly once, record the enqueue, and
    resume the waiter: try_transition(WAITING -> RUNNABLE).

    Rejected deliveries (stale/duplicate generation) do NOT touch the TCB.

    Return contract: True means "delivery ACCEPTED by the Event" (generation
    claimed + enqueued); the caller owns the TCB resume transition.  A True
    return does NOT by itself prove the waiter resumed -- callers that need
    that guarantee check tp[].state() after the resume transition.
    """
    var accepted = ev.set()
    if accepted:
        _ = tp[].try_transition(
            TaskControlBlock.WAITING, TaskControlBlock.RUNNABLE
        )
    return accepted


# ---------------------------------------------------------------------------
# Two-phase park pipeline (scripted, deterministic)
# ---------------------------------------------------------------------------

def _apply_action[T: ResultValue](
    mut ev: Event,
    tp: UnsafePointer[TaskControlBlock[T], MutAnyOrigin],
    fp: UnsafePointer[CancelFlag, MutAnyOrigin],
    action: Int,
    parked: Bool,
) raises:
    """Fire one scripted hook action against the LIVE objects."""
    if action == HookAction.REQUEST_CANCEL:
        fp[].request()
    elif action == HookAction.SET_READY:
        if parked:
            _ = deliver_wake(ev, tp)
        else:
            _ = ev.set()  # pre-COMMIT: latch only (no waiter armed yet)
    elif action == HookAction.WAKE:
        if parked:
            _ = deliver_wake(ev, tp)
        else:
            _ = ev.set()
    else:
        raise Error("event._apply_action: unknown scripted action")


def _run_script[T: ResultValue](
    mut ev: Event,
    tp: UnsafePointer[TaskControlBlock[T], MutAnyOrigin],
    fp: UnsafePointer[CancelFlag, MutAnyOrigin],
    actions: ActionList,
    parked: Bool,
) raises:
    var i = 0
    while i < actions.size():
        _apply_action(ev, tp, fp, actions.at(i), parked)
        i += 1


def _settle(
    mut ev: Event,
    fp: UnsafePointer[CancelFlag, MutAnyOrigin],
    mut report: ParkReport,
    at: Int,
    parked: Bool,
) raises -> Bool:
    """Decide the race after a boundary: cancellation FIRST, then
    readiness (race_hooks settle policy).  Records exactly one winner."""
    if fp[].is_requested():
        report.winners.record(WinnerRecord.CANCEL)
        if ev.is_set():
            # A0-T10: the losing readiness WAS delivered — counted as an
            # attempt, never the winner (cancellation precedence).
            report.winners.record_attempt(WinnerRecord.READY)
        report.outcome = ParkOutcome.CANCELLED
        report.decided_at = at
        return True
    if ev.is_set():
        report.winners.record(WinnerRecord.READY)
        if parked:
            report.outcome = ParkOutcome.PARKED_AND_WOKE
        else:
            report.outcome = ParkOutcome.EARLY_WAKE
        report.decided_at = at
        ev.clear_latch()
        return True
    return False


def park_pipeline[T: ResultValue](
    mut ev: Event,
    tp: UnsafePointer[TaskControlBlock[T], MutAnyOrigin],
    fp: UnsafePointer[CancelFlag, MutAnyOrigin],
    hooks: HookScript,
    mut report: ParkReport,
) raises:
    """Drive one park epoch over the REAL TCB under the injected schedule.

    Precondition: the task is in RUNNING.  Phases per the module header;
    the pipeline stops at the first decided outcome.  With no cancellation
    and no delivered readiness the task rests in WAITING (UNDECIDED) — a
    real scheduler would block here.
    """
    if tp[].state() != TaskControlBlock.RUNNING:
        raise Error("event.park_pipeline: task must be RUNNING to wait")

    # --- Phase 1: PREPARE — publish the waiter as DATA ----------------------
    # Waiter identity: the spike has no scheduler task ids yet, so the
    # TCB's parent handle stands in (0 = none); the NODE pointer is the
    # authoritative waiter reference either way.
    var nodep = tp[].wait_node()
    nodep[].set_reason(WaitReason.EVENT)
    ev.publish(tp[].parent_id(), nodep)
    _run_script(ev, tp, fp, hooks.prepare, False)
    if _settle(ev, fp, report, HookPoint.PREPARE, False):
        return

    # --- Phase 2: VALIDATE — lost-wakeup window -----------------------------
    _run_script(ev, tp, fp, hooks.validate, False)
    if _settle(ev, fp, report, HookPoint.VALIDATE, False):
        return  # early wake: never slept, never left RUNNING, no gen bump

    # --- Phase 3: COMMIT — the PARKING window -------------------------------
    # Spec §23/§24: RUNNING -> PARKING opens the early-wake window.  COMMIT
    # scripts fire INSIDE the window (the deterministic stand-in for
    # "readiness arrived between the VALIDATE recheck and the sleep"):
    # deliveries latch only — the waiter is not armed yet — and the race is
    # settled (cancellation first) BEFORE the WAITING entry.
    if not tp[].try_transition(
        TaskControlBlock.RUNNING, TaskControlBlock.PARKING
    ):
        raise Error("event.park_pipeline: RUNNING -> PARKING refused")
    _run_script(ev, tp, fp, hooks.commit, False)
    if fp[].is_requested():
        # Cancelled in the window: unwind to RUNNABLE — never sleep behind
        # a decided cancellation.  The cooperative checkpoint observes it
        # at the next opportunity (A0.8 fold wires the raise).
        if not tp[].try_transition(
            TaskControlBlock.PARKING, TaskControlBlock.RUNNABLE
        ):
            raise Error("event.park_pipeline: PARKING -> RUNNABLE refused")
        if ev.is_set():
            # A0-T10: losing readiness counted, never the winner.
            report.winners.record_attempt(WinnerRecord.READY)
        report.winners.record(WinnerRecord.CANCEL)
        report.outcome = ParkOutcome.CANCELLED
        report.decided_at = HookPoint.COMMIT
        return
    if ev.is_set():
        # Early wake (spec §24): never enter WAITING, never bump.
        if not tp[].try_transition(
            TaskControlBlock.PARKING, TaskControlBlock.RUNNABLE
        ):
            raise Error("event.park_pipeline: PARKING -> RUNNABLE refused")
        report.winners.record(WinnerRecord.READY)
        report.outcome = ParkOutcome.EARLY_WAKE
        report.decided_at = HookPoint.COMMIT
        ev.clear_latch()
        return
    if not tp[].try_transition(
        TaskControlBlock.PARKING, TaskControlBlock.WAITING
    ):
        raise Error("event.park_pipeline: PARKING -> WAITING refused")
    ev.arm_parked()  # node now carries the fresh epoch generation
    report.slept = True

    # --- Phase 4: WAKE — scheduler resume path ------------------------------
    _run_script(ev, tp, fp, hooks.wake, True)
    _ = _settle(ev, fp, report, HookPoint.WAKE, True)
    # Otherwise: UNDECIDED — parked in WAITING (documented, not blocked).
