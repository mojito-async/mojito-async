# spike/colorless_runtime/event_scenarios.mojo
#
# A0.7 (issue #16) — deterministic park/wake scenario harness for t10.
#
# TOOLCHAIN WORKAROUND (b2, documented per lane brief): compile time of an
# ENTRY module (the `mojo run` driver) grows superlinearly with the number
# of runtime statements it contains — a dozen park cycles inline in the
# driver made `mojo build` diverge (>200 s and climbing, sampled deep in
# the semantic phase; parsing is fast).  Imported modules, by contrast, are
# type-checked once and cached.  This module therefore owns the SCENARIO
# CHOREOGRAPHY (rig setup, scripted pipelines, manual wakes), and the
# driver only calls these concrete functions and asserts on the returned
# Snapshot scalars.  Same coverage, flat entry-module cost.
#
# Every scenario builds a FRESH rig (TCB + CancelFlag + Event), drives it
# to RUNNING, runs park_pipeline under the named schedule, applies any
# post-pipeline deliveries, and snapshots EVERYTHING the driver wants to
# assert: outcome ledger, TCB state/generation/node stamping, event latch,
# claim guard, enqueue log, and the exactly-one-winner bookkeeping.

from cancel import CancelFlag, make_cancel_flag
from event import (
    Event,
    ParkOutcome,
    ParkReport,
    SpikeTcb,
    WaitReason,
    deliver_wake_spike,
    park_pipeline_spike,
)
from race_hooks import (
    HookAction,
    HookPoint,
    HookScript,
    WinnerRecord,
    make_hook_script,
    script_add,
)


# ---------------------------------------------------------------------------
# Snapshot: scalar projection of one scenario run
# ---------------------------------------------------------------------------

struct Snapshot(ImplicitlyCopyable, ImplicitlyDeletable):
    """Everything a driver may assert after one scenario."""

    var outcome: Int
    var decided_at: Int
    var slept: Bool
    # task control block
    var state: Int
    var generation: Int
    var node_generation: Int
    var reason: Int
    # event
    var latched: Bool
    var claimed_gen: Int
    var set_attempts: Int
    var wakes_accepted: Int
    var enqueue_len: Int
    var enq0: Int
    var enq1: Int
    # race ledger
    var winner: Int
    var single_winner: Bool
    var attempts_cancel: Int
    var attempts_ready: Int
    # scenario-specific delivery outcomes
    var stale_rejected: Bool
    var dup_rejected: Bool

    def __init__(out self):
        self.outcome = -1
        self.decided_at = -1
        self.slept = False
        self.state = -1
        self.generation = -1
        self.node_generation = -1
        self.reason = -1
        self.latched = False
        self.claimed_gen = -1
        self.set_attempts = 0
        self.wakes_accepted = 0
        self.enqueue_len = 0
        self.enq0 = -1
        self.enq1 = -1
        self.winner = -1
        self.single_winner = False
        self.attempts_cancel = 0
        self.attempts_ready = 0
        self.stale_rejected = False
        self.dup_rejected = False


# ---------------------------------------------------------------------------
# Rig helpers (concrete, non-generic)
# ---------------------------------------------------------------------------

def _prime_running(tp: UnsafePointer[SpikeTcb, MutAnyOrigin]) raises:
    tp[].transition(SpikeTcb.RUNNABLE)
    tp[].transition(SpikeTcb.RUNNING)


def _snapshot(
    ev: Event,
    report: ParkReport,
    tp: UnsafePointer[SpikeTcb, MutAnyOrigin],
) raises -> Snapshot:
    """Project the whole rig into scalars for the driver to assert."""
    var s = Snapshot()
    s.outcome = report.outcome
    s.decided_at = report.decided_at
    s.slept = report.slept
    s.state = tp[].state()
    s.generation = tp[].generation()
    s.node_generation = tp[].wait_node()[].generation()
    s.reason = tp[].wait_node()[].reason()
    s.latched = ev.is_set()
    s.claimed_gen = ev.claimed_gen()
    s.set_attempts = ev.set_attempts()
    s.wakes_accepted = ev.wakes_accepted()
    s.enqueue_len = ev.enqueue_len()
    if ev.enqueue_len() > 0:
        s.enq0 = ev.enqueued_gen(0)
    if ev.enqueue_len() > 1:
        s.enq1 = ev.enqueued_gen(1)
    s.winner = report.winners.winner()
    s.single_winner = report.winners.exactly_one_winner()
    s.attempts_cancel = report.winners.attempts(WinnerRecord.CANCEL)
    s.attempts_ready = report.winners.attempts(WinnerRecord.READY)
    return s


def _script_one(point: Int, action: Int) raises -> HookScript:
    var h = make_hook_script()
    script_add(h, point, action)
    return h


# ---------------------------------------------------------------------------
# Scenarios (A0-T11 / A0-T12 / A0-T10 + generation discipline)
# ---------------------------------------------------------------------------

def scn_latch_only() raises -> Snapshot:
    """set() before any waiter publishes: pure latch, nothing enqueued."""
    var ev = Event()
    var accepted = ev.set()
    var s = Snapshot()
    s.latched = ev.is_set()
    s.set_attempts = ev.set_attempts()
    s.wakes_accepted = ev.wakes_accepted()
    s.enqueue_len = ev.enqueue_len()
    # encode acceptance in stale_rejected (unused here): True iff rejected
    s.stale_rejected = not accepted
    _ = s
    return s


def scn_park_blocked() raises -> Snapshot:
    """Undisturbed schedule: parks through COMMIT, rests in WAITING."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var rep = ParkReport()
    park_pipeline_spike(ev, tp, fp, make_hook_script(), rep)
    return _snapshot(ev, rep, tp)


def scn_park_then_double_wake() raises -> Snapshot:
    """Park (undecided), wake by hand, then a DUPLICATE delivery for the
    same generation (A0-T12): second is a counted, rejected no-op."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var rep = ParkReport()
    park_pipeline_spike(ev, tp, fp, make_hook_script(), rep)
    var first = deliver_wake_spike(ev, tp)
    var dup = deliver_wake_spike(ev, tp)
    var s = _snapshot(ev, rep, tp)
    s.dup_rejected = not dup
    s.stale_rejected = first  # reuse: True iff FIRST was accepted
    return s


def scn_early_wake(at: Int) raises -> Snapshot:
    """Readiness delivered BEFORE the park commits (PREPARE or VALIDATE):
    wait returns without sleeping (A0-T11 lost-wakeup defense)."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var rep = ParkReport()
    park_pipeline_spike(ev, tp, fp, _script_one(at, HookAction.SET_READY), rep)
    return _snapshot(ev, rep, tp)


def scn_commit_window_early_wake() raises -> Snapshot:
    """Readiness at COMMIT: the PARKING->RUNNABLE early-wake edge fires;
    WAITING (and its generation bump) is never reached."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var rep = ParkReport()
    park_pipeline_spike(
        ev, tp, fp, _script_one(HookPoint.COMMIT, HookAction.SET_READY), rep
    )
    return _snapshot(ev, rep, tp)


def scn_scripted_wake() raises -> Snapshot:
    """WAKE fired at the WAKE boundary while parked: full legal cycle."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var rep = ParkReport()
    park_pipeline_spike(
        ev, tp, fp, _script_one(HookPoint.WAKE, HookAction.WAKE), rep
    )
    return _snapshot(ev, rep, tp)


def scn_duplicate_wake_in_epoch() raises -> Snapshot:
    """Three deliveries fired INSIDE one parked epoch (A0-T12): exactly one
    accepted edge, enqueue-log length 1."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var hooks = make_hook_script()
    # Three deliveries fired at the WAKE boundary while PARKED (post
    # WAITING entry): exactly one accepted edge per epoch (A0-T12).
    script_add(hooks, HookPoint.WAKE, HookAction.WAKE)
    script_add(hooks, HookPoint.WAKE, HookAction.SET_READY)
    script_add(hooks, HookPoint.WAKE, HookAction.WAKE)
    var rep = ParkReport()
    park_pipeline_spike(ev, tp, fp, hooks, rep)
    return _snapshot(ev, rep, tp)


def scn_two_cycles_stale_wake() raises -> Snapshot:
    """Generation discipline: each park epoch bumps the generation; a wake
    targeting the previous epoch is IGNORED; the current epoch wakes once
    and a same-generation repeat is rejected."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()

    # Cycle 1: park undecided, wake at the fresh generation.
    _prime_running(tp)
    var rep1 = ParkReport()
    park_pipeline_spike(ev, tp, fp, make_hook_script(), rep1)
    var woke1 = deliver_wake_spike(ev, tp)

    # Wait-side consume: the resumed task OBSERVED the readiness before
    # parking again (otherwise cycle 2 would early-wake on the old latch).
    ev.clear_latch()
    # Cycle 2: resume -> park again (generation bumps); stale wake ignored.
    tp[].transition(SpikeTcb.RUNNING)
    var rep2 = ParkReport()
    park_pipeline_spike(ev, tp, fp, make_hook_script(), rep2)
    var stale = ev.set_at(tp[].generation() - 1)
    var woke2 = deliver_wake_spike(ev, tp)
    var dup2 = ev.set_at(tp[].generation())

    var s = _snapshot(ev, rep2, tp)
    s.stale_rejected = not stale
    s.dup_rejected = not dup2
    # first-cycle evidence folded into spare scalars:
    if not woke1:
        s.winner = -2  # sentinel: cycle-1 wake unexpectedly rejected
    if not woke2:
        s.single_winner = False  # repurpose as "all wakes healthy" flag
    else:
        s.single_winner = True
    _ = rep1
    return s


def scn_race(at: Int) raises -> Snapshot:
    """Cancel-vs-readiness BOTH fire at one boundary (A0-T10): settlement
    gives cancellation precedence; exactly one winner."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var hooks = make_hook_script()
    script_add(hooks, at, HookAction.REQUEST_CANCEL)
    script_add(hooks, at, HookAction.SET_READY)
    var rep = ParkReport()
    park_pipeline_spike(ev, tp, fp, hooks, rep)
    return _snapshot(ev, rep, tp)


def scn_cancel_only(at: Int) raises -> Snapshot:
    """Cancellation alone during the pipeline: precedence without a race."""
    var t = SpikeTcb()
    var tp = UnsafePointer[SpikeTcb, MutAnyOrigin](to=t)
    var f = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=f)
    var ev = Event()
    _prime_running(tp)
    var rep = ParkReport()
    park_pipeline_spike(
        ev, tp, fp, _script_one(at, HookAction.REQUEST_CANCEL), rep
    )
    return _snapshot(ev, rep, tp)
