# spike/colorless_runtime/tests/t9_cancel_races.mojo
#
# A0.8 (issue #17) — cooperative cancellation + deterministic race hooks
# (TDD: written RED against cancel.mojo / race_hooks.mojo, goes GREEN once
# the implementation lands in this lane's branch).
#
# Covers spec A0-T9/T10/T11/T12 (pure-Mojo subset — integration with the
# real Event/park path lands in the A0.7 fold; these drivers exercise the
# cancellation flag semantics and the hook-driven park pipeline model):
#
#   cancel.mojo unit semantics:
#     - request() idempotent; is_requested()
#     - checkpoint() raises ONLY when requested (silent otherwise)
#     - child propagation: parent requested -> child observes
#     - reset(): pre-observation clearing allowed; spike policy forbids
#       reset AFTER an observation (checkpoint raised)
#   race_hooks.mojo deterministic schedules:
#     - wake-before-park: readiness delivered at VALIDATE time so the
#       pipeline never reaches COMMIT (no lost wakeup, no sleep)
#     - double-wake: WAKE fired twice -> second is a generation-guarded
#       no-op (single accepted wake per park epoch)
#     - cancel-vs-ready race: both sides fire in one pipeline -> exactly
#       ONE winner recorded, deterministic under the hook order
#   WinnerRecord: exactly-one-winner invariant directly assertable.
#
# Pure Mojo: `mojo run -I spike/colorless_runtime` with no dylib.

from cancel import CancelFlag, make_cancel_flag, make_child_flag
from race_hooks import (
    HookAction,
    HookPoint,
    HookScript,
    RaceContext,
    WinnerRecord,
    make_hook_script,
    run_park_pipeline,
    script_add,
)


# --- helpers ---------------------------------------------------------------

def expect(cond: Bool, what: String) raises:
    if not cond:
        raise Error("check failed: " + what)



def raises_cancellation(fp: UnsafePointer[CancelFlag, MutAnyOrigin]) raises:
    """Checkpoint() must raise when the flag observes cancellation."""
    var raised = False
    try:
        fp[].checkpoint()
    except Error:
        raised = True
    expect(raised, "checkpoint() did not raise on requested flag")


# --- test body -------------------------------------------------------------

def main() raises:
    # ------------------------------------------------------------------
    # 1. Fresh flag: not requested, never observed.
    # ------------------------------------------------------------------
    var fresh = make_cancel_flag()
    expect(not fresh.is_requested(), "fresh flag is not requested")
    expect(not fresh.observed(), "fresh flag is not observed")

    # ------------------------------------------------------------------
    # 2. request() is idempotent.
    # ------------------------------------------------------------------
    var req = make_cancel_flag()
    req.request()
    expect(req.is_requested(), "request() sets the flag")
    req.request()
    expect(req.is_requested(), "second request() keeps the flag (idempotent)")

    # ------------------------------------------------------------------
    # 3. checkpoint() is SILENT when not requested.
    # ------------------------------------------------------------------
    var quiet = make_cancel_flag()
    quiet.checkpoint()  # must not raise
    expect(not quiet.observed(), "silent checkpoint does not observe")

    # ------------------------------------------------------------------
    # 4. checkpoint() raises CancellationError ONLY when requested, and
    #    stamps the observation.
    # ------------------------------------------------------------------
    var obs2 = make_cancel_flag()
    obs2.request()
    raises_cancellation(UnsafePointer[CancelFlag, MutAnyOrigin](to=obs2))
    expect(obs2.observed(), "raising checkpoint stamps the observation")
    # Observation is sticky: a second checkpoint still raises.
    raises_cancellation(UnsafePointer[CancelFlag, MutAnyOrigin](to=obs2))

    # ------------------------------------------------------------------
    # 5. reset(): allowed BEFORE any observation; forbidden AFTER.
    #    (spike policy: no reset after observe — a sibling may already
    #    have acted on the cancellation.)
    # ------------------------------------------------------------------
    var rst = make_cancel_flag()
    rst.request()
    expect(rst.is_requested(), "requested before reset")
    rst.reset()  # pre-observation clear: legal
    expect(not rst.is_requested(), "reset() clears a pre-observation request")
    expect(not rst.observed(), "reset leaves no observation behind")

    var rst2 = make_cancel_flag()
    rst2.request()
    raises_cancellation(UnsafePointer[CancelFlag, MutAnyOrigin](to=rst2))
    var reset_raised = False
    try:
        rst2.reset()
    except Error:
        reset_raised = True
    expect(reset_raised, "reset() after observe must raise (spike policy)")

    # ------------------------------------------------------------------
    # 6. Child propagation: parent requested -> child observes.
    # ------------------------------------------------------------------
    var parent = make_cancel_flag()
    var child = make_child_flag(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=parent)
    )
    expect(not child.is_requested(), "clean parent -> clean child")
    parent.request()
    expect(child.is_requested(), "child observes requested parent")
    expect(not parent.observed(), "child observation is stamped on the CHILD")
    raises_cancellation(UnsafePointer[CancelFlag, MutAnyOrigin](to=child))
    expect(child.observed(), "child stamped its own observation")

    # Grandchild chains transitively.
    var mid = make_child_flag(UnsafePointer[CancelFlag, MutAnyOrigin](to=parent))
    var leaf = make_child_flag(UnsafePointer[CancelFlag, MutAnyOrigin](to=mid))
    expect(leaf.is_requested(), "grandchild observes through the chain")

    # Child requests do NOT propagate upward.
    var up = make_cancel_flag()
    var down = make_child_flag(UnsafePointer[CancelFlag, MutAnyOrigin](to=up))
    down.request()
    expect(down.is_requested(), "self request visible")
    expect(not up.is_requested(), "child request does NOT cancel the parent")

    # ------------------------------------------------------------------
    # 7. HookPoint: four distinct boundaries.
    # ------------------------------------------------------------------
    expect(HookPoint.PREPARE != HookPoint.VALIDATE, "PREPARE != VALIDATE")
    expect(HookPoint.VALIDATE != HookPoint.COMMIT, "VALIDATE != COMMIT")
    expect(HookPoint.COMMIT != HookPoint.WAKE, "COMMIT != WAKE")
    expect(HookPoint.PREPARE != HookPoint.WAKE, "PREPARE != WAKE")
    expect(HookAction.NOOP != HookAction.SET_READY, "actions distinct")
    expect(HookAction.REQUEST_CANCEL != HookAction.WAKE, "actions distinct 2")

    # ------------------------------------------------------------------
    # 8. Wake-before-park: readiness delivered at VALIDATE time so the
    #    park pipeline NEVER reaches COMMIT (lost-wakeup defense, T11).
    # ------------------------------------------------------------------
    var wb = make_hook_script()
    script_add(wb, HookPoint.VALIDATE, HookAction.SET_READY)
    var wbc = RaceContext(make_cancel_flag())
    run_park_pipeline(wbc, wb)
    expect(wbc.winners.winner() == WinnerRecord.READY, "early wake -> READY wins")
    expect(wbc.winners.exactly_one_winner(), "exactly one winner recorded")
    expect(not wbc.slept(), "pipeline never slept (wake-before-park)")
    expect(wbc.hooks_fired(HookPoint.VALIDATE) == 1, "VALIDATE fired once")
    expect(wbc.hooks_fired(HookPoint.COMMIT) == 0, "COMMIT never reached")

    # ------------------------------------------------------------------
    # 9. Double-wake: WAKE fired twice inside COMMIT -> the second wake is
    #    a generation-guarded no-op (T12: single enqueue per generation).
    # ------------------------------------------------------------------
    var dw = make_hook_script()
    script_add(dw, HookPoint.COMMIT, HookAction.WAKE)
    script_add(dw, HookPoint.COMMIT, HookAction.WAKE)
    var dwc = RaceContext(make_cancel_flag())
    run_park_pipeline(dwc, dw)
    expect(dwc.winners.winner() == WinnerRecord.READY, "double wake -> READY")
    expect(dwc.winners.exactly_one_winner(), "double wake: one winner")
    expect(dwc.slept(), "pipeline did sleep before the wakes")
    expect(dwc.wake_attempts() == 2, "two wake attempts were made")
    expect(dwc.wakes_accepted() == 1, "generation guard rejected the second")
    # The guard is shared by every delivery path: mixing WAKE and SET_READY
    # inside one boundary still yields exactly one accepted edge.
    var dw2 = make_hook_script()
    script_add(dw2, HookPoint.COMMIT, HookAction.WAKE)
    script_add(dw2, HookPoint.COMMIT, HookAction.SET_READY)
    script_add(dw2, HookPoint.COMMIT, HookAction.WAKE)
    var dwc2 = RaceContext(make_cancel_flag())
    run_park_pipeline(dwc2, dw2)
    expect(dwc2.wake_attempts() == 3, "every delivery counted as attempt")
    expect(dwc2.wakes_accepted() == 1, "guard accepted only the first edge")
    expect(dwc2.winners.exactly_one_winner(), "still exactly one winner")

    # ------------------------------------------------------------------
    # 10. Cancel-vs-ready race: BOTH sides fire inside one pipeline ->
    #     exactly ONE winner, deterministically (T10).
    # ------------------------------------------------------------------
    var cvr = make_hook_script()
    script_add(cvr, HookPoint.PREPARE, HookAction.REQUEST_CANCEL)
    script_add(cvr, HookPoint.PREPARE, HookAction.SET_READY)

    # Run the identical schedule twice: the outcome must be identical.
    var r1 = RaceContext(make_cancel_flag())
    run_park_pipeline(r1, cvr)
    var r2 = RaceContext(make_cancel_flag())
    run_park_pipeline(r2, cvr)

    expect(r1.winners.winner() == WinnerRecord.CANCEL, "race: CANCEL wins")
    expect(r2.winners.winner() == WinnerRecord.CANCEL, "race deterministic")
    expect(r1.winners.exactly_one_winner(), "race: exactly one winner")
    expect(r1.winners.attempts(WinnerRecord.CANCEL) == 1, "cancel fired once")
    expect(r1.winners.attempts(WinnerRecord.READY) == 1, "ready fired too...")
    expect(
        r1.winners.attempts(WinnerRecord.READY) >= 1
        and r1.winners.winner() == WinnerRecord.CANCEL,
        "...but the losing side never took the slot",
    )
    expect(r1.cancelled(), "race loser side: cancellation visible on ctx")

    # ------------------------------------------------------------------
    # 11. WinnerRecord unit: exactly-one-winner invariant directly.
    # ------------------------------------------------------------------
    var wr = WinnerRecord()
    expect(not wr.decided(), "fresh record undecided")
    wr.record(WinnerRecord.READY)
    wr.record(WinnerRecord.READY)  # repeat attempt: counted, not re-won
    expect(wr.winner() == WinnerRecord.READY, "first claim sticks")
    expect(wr.exactly_one_winner(), "repeat same-kind claims keep it single")
    wr.record(WinnerRecord.CANCEL)  # late loser: recorded, never wins
    expect(wr.winner() == WinnerRecord.READY, "late claim cannot overwrite")
    expect(wr.exactly_one_winner(), "late loser does not break the invariant")
    expect(wr.attempts(WinnerRecord.CANCEL) == 1, "loser attempt counted")

    # ------------------------------------------------------------------
    # 12. Baseline: empty script, no cancellation -> the pipeline runs all
    #     four boundaries and parks (undecided; real Event/park integration
    #     lands in the A0.7 fold).
    # ------------------------------------------------------------------
    var base = make_hook_script()
    var basec = RaceContext(make_cancel_flag())
    run_park_pipeline(basec, base)
    expect(basec.slept(), "baseline pipeline reached COMMIT")
    expect(basec.hooks_fired(HookPoint.PREPARE) == 1, "PREPARE fired")
    expect(basec.hooks_fired(HookPoint.VALIDATE) == 1, "VALIDATE fired")
    expect(basec.hooks_fired(HookPoint.COMMIT) == 1, "COMMIT fired")
    expect(basec.hooks_fired(HookPoint.WAKE) == 1, "WAKE fired")
    expect(not basec.winners.decided(), "nothing raced: undecided")

    print("T9 cancel/race hooks: PASS")
