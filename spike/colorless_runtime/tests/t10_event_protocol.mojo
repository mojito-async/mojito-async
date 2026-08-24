# spike/colorless_runtime/tests/t10_event_protocol.mojo
#
# A0.7 (issue #16) — Event-based park/wake: two-phase parking against the
# REAL TaskControlBlock state machine (TDD: written RED against event.mojo,
# goes GREEN once the implementation lands in this lane's branch).
#
# This is the integration counterpart of t9's modeled pipeline: the same
# HookScript schedules force races deterministically, but the four phases
# now drive a genuine TaskControlBlock (states, transitions, embedded
# WaitNode generation) plus a genuine Event (spec §23):
#
#     PREPARE  publish waiter as DATA (task id + pointer to the TCB's
#              embedded WaitNode); settle(cancel, readiness)
#     VALIDATE recheck readiness (the lost-wakeup window); readiness HERE
#              means wait() returns without sleeping and WITHOUT a
#              generation bump — the task never left RUNNING   [A0-T11]
#     COMMIT   RUNNING->PARKING; readiness already visible unwinds via the
#              early-wake edge PARKING->RUNNABLE (never WAITING);
#              otherwise PARKING->WAITING bumps the wait generation and
#              stamps the embedded node; settle
#     WAKE     Event.set() claims the waiter's generation EXACTLY ONCE,
#              records ONE enqueue, resumes via WAITING->RUNNABLE [A0-T12]
#
# Covered here (spec A0-T10/T11/T12 + generation discipline):
#   - latch-only set() before any waiter publishes
#   - baseline blocked park: UNDECIDED in WAITING at a bumped generation,
#     node re-stamped; manual wake + duplicate rejection (A0-T12)
#   - wake-before-park at PREPARE and VALIDATE: no sleep, no gen bump,
#     nothing enqueued (no lost wakeup)
#   - COMMIT-window early wake via PARKING->RUNNABLE (never WAITING)
#   - scripted WAKE-boundary delivery: full legal park cycle
#   - duplicate set(): three deliveries in one epoch -> ONE accepted edge,
#     enqueue-log length 1
#   - two park cycles: generation bumps per epoch; STALE-generation wakes
#     ignored; current-generation wake accepted exactly once
#   - cancel-vs-readiness races at PREPARE and COMMIT: cancellation takes
#     precedence (settle policy), EXACTLY ONE winner, deterministic across
#     repeated runs of the identical schedule
#
# STRUCTURE NOTE (b2 toolchain workaround): the scenario choreography lives
# in event_scenarios.mojo — imported modules are type-checked once and
# cached, whereas an entry module with this much inline choreography makes
# `mojo` compile times diverge (sampled deep in the semantic phase).  Each
# scn_* call returns a Snapshot whose scalars are asserted below.
#
# Pure Mojo: `mojo run -I spike/colorless_runtime` with no dylib.

from event import ParkOutcome, SpikeTcb
from event_scenarios import (
    scn_cancel_only,
    scn_commit_window_early_wake,
    scn_duplicate_wake_in_epoch,
    scn_early_wake,
    scn_latch_only,
    scn_park_blocked,
    scn_park_then_double_wake,
    scn_race,
    scn_scripted_wake,
    scn_two_cycles_stale_wake,
)
from race_hooks import HookPoint, WinnerRecord

# --- helpers ---------------------------------------------------------------

def expect(cond: Bool, what: String) raises:
    if not cond:
        raise Error("check failed: " + what)


def main() raises:
    # ------------------------------------------------------------------
    # 1. Latch-only set(): readiness before any waiter publishes is a pure
    #    latch — recorded attempt, no accepted wake, nothing enqueued.
    # ------------------------------------------------------------------
    var s1 = scn_latch_only()
    expect(not s1.stale_rejected == False, "shape")
    expect(s1.stale_rejected, "set() with no parked waiter accepts nothing")
    expect(s1.latched, "set() latches readiness")
    expect(s1.set_attempts == 1, "attempt counted")
    expect(s1.wakes_accepted == 0, "no accepted wake")
    expect(s1.enqueue_len == 0, "nothing enqueued (no waiter armed)")

    # ------------------------------------------------------------------
    # 2. Baseline blocked park: undisturbed schedule rests in WAITING.
    # ------------------------------------------------------------------
    var sB = scn_park_blocked()
    expect(sB.outcome == ParkOutcome.UNDECIDED, "baseline undecided")
    expect(sB.slept, "baseline slept (committed to WAITING)")
    expect(sB.state == SpikeTcb.WAITING, "blocked in WAITING")
    expect(sB.generation == 2, "WAITING entry bumped generation 1 -> 2")
    expect(sB.node_generation == 2, "embedded node carries fresh generation")
    expect(sB.reason == 0, "PREPARE stamped wait reason EVENT")

    # ------------------------------------------------------------------
    # 3. Park, manual wake, DUPLICATE delivery for the same generation
    #    (A0-T12): second delivery is a counted, rejected no-op.
    # ------------------------------------------------------------------
    var sW = scn_park_then_double_wake()
    expect(sW.stale_rejected, "first manual wake accepted")
    expect(sW.state == SpikeTcb.RUNNABLE, "WAKE -> RUNNABLE")
    expect(sW.latched, "readiness latched by the wake")
    expect(sW.enqueue_len == 1, "one enqueue logged")
    expect(sW.enq0 == 2, "enqueue recorded at the fresh generation")
    expect(sW.claimed_gen == 2, "generation claimed exactly once")
    expect(sW.dup_rejected, "duplicate wake for same generation rejected")
    expect(sW.enqueue_len == 1, "duplicate enqueued NOTHING (log length)")
    expect(sW.set_attempts == 2, "both deliveries counted as attempts")
    expect(sW.wakes_accepted == 1, "only one accepted edge")

    # ------------------------------------------------------------------
    # 4. A0-T11 wake-before-park at VALIDATE: never sleeps, no gen bump,
    #    nothing enqueued (no lost wakeup).
    # ------------------------------------------------------------------
    var sE = scn_early_wake(HookPoint.VALIDATE)
    expect(sE.outcome == ParkOutcome.EARLY_WAKE, "early wake outcome")
    expect(sE.decided_at == HookPoint.VALIDATE, "decided at VALIDATE")
    expect(not sE.slept, "never slept (wake-before-park)")
    expect(sE.single_winner, "exactly one winner (READY)")
    expect(sE.winner == WinnerRecord.READY, "READY won")
    expect(sE.state == SpikeTcb.RUNNING, "still RUNNING")
    expect(sE.generation == 1, "no generation bump (never WAITING)")
    expect(sE.enqueue_len == 0, "no enqueue (never parked)")
    expect(sE.wakes_accepted == 0, "no accepted wake needed")

    # Same defense when readiness arrives even earlier, at PREPARE.
    var sP = scn_early_wake(HookPoint.PREPARE)
    expect(sP.outcome == ParkOutcome.EARLY_WAKE, "PREPARE-time early wake")
    expect(sP.decided_at == HookPoint.PREPARE, "decided at PREPARE")
    expect(not sP.slept, "PREPARE-time readiness never sleeps")
    expect(sP.state == SpikeTcb.RUNNING, "still RUNNING")

    # ------------------------------------------------------------------
    # 5. Early wake in the COMMIT window: PARKING -> RUNNABLE directly;
    #    WAITING (and its generation bump) is never reached.
    # ------------------------------------------------------------------
    var sC = scn_commit_window_early_wake()
    expect(sC.outcome == ParkOutcome.EARLY_WAKE, "COMMIT-window early wake")
    expect(sC.decided_at == HookPoint.COMMIT, "decided at COMMIT")
    expect(not sC.slept, "did not stay parked")
    expect(sC.state == SpikeTcb.RUNNABLE, "PARKING -> RUNNABLE")
    expect(sC.generation == 1, "never entered WAITING: no bump")

    # ------------------------------------------------------------------
    # 6. Scripted full cycle: WAKE fired at the WAKE boundary while parked.
    # ------------------------------------------------------------------
    var sF = scn_scripted_wake()
    expect(sF.outcome == ParkOutcome.PARKED_AND_WOKE, "parked then woken")
    expect(sF.decided_at == HookPoint.WAKE, "decided at WAKE")
    expect(sF.slept, "slept before the wake")
    expect(sF.single_winner, "single READY winner")
    expect(sF.state == SpikeTcb.RUNNABLE, "resumed RUNNABLE")
    expect(sF.generation == 2, "one park epoch: generation 1 -> 2")
    expect(sF.enqueue_len == 1, "exactly one enqueue")
    expect(sF.enq0 == 2, "enqueue at the fresh generation")

    # ------------------------------------------------------------------
    # 7. A0-T12 duplicate set() INSIDE one park epoch: three deliveries at
    #    COMMIT -> one accepted edge, enqueue-log length 1.
    # ------------------------------------------------------------------
    var sD = scn_duplicate_wake_in_epoch()
    expect(sD.set_attempts == 3, "all three deliveries counted")
    expect(sD.wakes_accepted == 1, "guard accepted ONLY the first edge")
    expect(sD.enqueue_len == 1, "enqueue-log length 1 (A0-T12)")
    expect(sD.single_winner, "exactly one winner")
    expect(sD.state == SpikeTcb.RUNNABLE, "resumed once")

    # ------------------------------------------------------------------
    # 8. Generation discipline across cycles: each park epoch bumps the
    #    generation; STALE-generation wakes are ignored.
    # ------------------------------------------------------------------
    var sG = scn_two_cycles_stale_wake()
    expect(sG.generation == 3, "cycle 2 rests at generation 3")
    expect(sG.node_generation == 3, "node re-stamped with fresh generation")
    expect(sG.single_winner, "both epoch wakes healthy (sentinel unset)")
    expect(sG.stale_rejected, "stale-generation wake ignored")
    expect(sG.dup_rejected, "same-generation re-set rejected")
    expect(sG.set_attempts >= 2, "stale/dup deliveries counted as attempts")
    expect(sG.wakes_accepted == 2, "one accepted wake per epoch only")
    expect(sG.enqueue_len == 2, "second enqueue logged (new epoch)")
    expect(sG.enq0 == 2 and sG.enq1 == 3, "logged per epoch generations")
    expect(sG.state == SpikeTcb.RUNNABLE, "resumed at gen 3")

    # ------------------------------------------------------------------
    # 9. A0-T10 cancel-vs-readiness at PREPARE: BOTH fire -> cancellation
    #    precedence, EXACTLY ONE winner, deterministic across repeats.
    # ------------------------------------------------------------------
    var rA1 = scn_race(HookPoint.PREPARE)
    var rA2 = scn_race(HookPoint.PREPARE)
    expect(rA1.outcome == ParkOutcome.CANCELLED, "race at PREPARE: CANCEL")
    expect(rA1.decided_at == HookPoint.PREPARE, "decided at PREPARE")
    expect(rA1.winner == WinnerRecord.CANCEL, "CANCEL wins")
    expect(rA1.single_winner, "exactly one winner")
    expect(rA1.attempts_cancel == 1, "cancel fired once")
    expect(rA1.attempts_ready == 1, "ready fired too...")
    expect(rA2.winner == WinnerRecord.CANCEL, "deterministic repeat")
    expect(rA2.single_winner, "deterministic single winner")
    expect(not rA1.slept, "decided before any sleep")

    # Variant at COMMIT: both fire inside the PARKING window — settled
    # before the WAITING entry, so nothing is ever enqueued.
    var rC = scn_race(HookPoint.COMMIT)
    expect(rC.outcome == ParkOutcome.CANCELLED, "race at COMMIT: CANCEL")
    expect(rC.decided_at == HookPoint.COMMIT, "decided at COMMIT")
    expect(rC.winner == WinnerRecord.CANCEL, "CANCEL wins in the window")
    expect(rC.single_winner, "exactly one winner in the window")
    expect(not rC.slept, "never slept behind a decided race")
    expect(rC.state == SpikeTcb.RUNNABLE, "unwound PARKING -> RUNNABLE")
    expect(rC.generation == 1, "never entered WAITING")
    expect(rC.enqueue_len == 0, "nothing enqueued (never parked)")
    expect(
        rC.attempts_cancel == 1 and rC.attempts_ready == 1,
        "both deliveries counted; only cancel won",
    )
    # Integration seam (documented in event.mojo): cancellation precedence
    # is enforced on the WINNER ledger by settle(); the cooperative-cancel
    # fold (A0.8) owns the CancellationError surface on resume.

    # ------------------------------------------------------------------
    # 10. Cancel alone during the pipeline: precedence without a race.
    # ------------------------------------------------------------------
    var sK = scn_cancel_only(HookPoint.VALIDATE)
    expect(sK.outcome == ParkOutcome.CANCELLED, "cancel-only: CANCELLED")
    expect(sK.decided_at == HookPoint.VALIDATE, "decided at VALIDATE")
    expect(not sK.slept, "cancelled before sleeping")
    expect(sK.state == SpikeTcb.RUNNING, "still RUNNING")
    expect(sK.enqueue_len == 0, "no enqueue on the cancel path")

    print("T10 event park/wake protocol: PASS")
