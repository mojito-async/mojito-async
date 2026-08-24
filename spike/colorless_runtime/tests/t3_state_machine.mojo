# spike/colorless_runtime/tests/t3_state_machine.mojo
#
# A0.3 (issue #12) — pure-Mojo task state machine tests (TDD: written RED
# against task.mojo, goes GREEN once the implementation lands).
#
# Covers spec A0.5:
#   - full happy path NEW .. COMPLETED
#   - early wake (PARKING -> RUNNABLE)
#   - cancellation (WAITING -> RUNNABLE)
#   - every illegal transition raises (assert_illegal helper, try/except)
#   - consume-once result (second take_result raises)
#   - waiter generation increments across park cycles
#
# Pure Mojo: `mojo run` with no dylib.

from task import ResultValue, TaskControlBlock

# Result type for the generic TCB slot (must conform to ResultValue: a
# zero-arg __init__; values are copied in/out in this lane).
struct TInt(ResultValue):
    var v: Int
    def __init__(out self):
        self.v = 0
    def __init__(out self, val: Int):
        self.v = val

comptime TB = TaskControlBlock[TInt]


# --- helpers ---------------------------------------------------------------

# Build a fresh TCB positioned at `state` through only-legal transitions.
def tcb_at(state: Int) raises -> TB:
    var t = TB.create()
    if state == TB.RUNNABLE:
        t.transition(TB.RUNNABLE)
    elif state == TB.RUNNING:
        t.transition(TB.RUNNABLE)
        t.transition(TB.RUNNING)
    elif state == TB.PARKING:
        t.transition(TB.RUNNABLE)
        t.transition(TB.RUNNING)
        t.transition(TB.PARKING)
    elif state == TB.WAITING:
        t.transition(TB.RUNNABLE)
        t.transition(TB.RUNNING)
        t.transition(TB.PARKING)
        t.transition(TB.WAITING)
    elif state == TB.COMPLETED:
        t.transition(TB.RUNNABLE)
        t.transition(TB.RUNNING)
        t.transition(TB.COMPLETED)
    elif state == TB.CANCELLED:
        t.transition(TB.RUNNABLE)
        t.transition(TB.RUNNING)
        t.transition(TB.CANCELLED)
    return t


# Assert that `transition(to)` from the TCB at `tp` raises (an illegal edge).
# Uses try/except per the spec's assert-illegal helper.
def assert_illegal(tp: UnsafePointer[TB, MutAnyOrigin], to: Int, what: String) raises:
    try:
        tp[0].transition(to)
    except Error:
        return
    raise Error("illegal transition was accepted: " + what)


def expect(cond: Bool, what: String) raises:
    if not cond:
        raise Error("check failed: " + what)


# --- test body -------------------------------------------------------------

def main() raises:
    # 1. Full happy path: NEW -> RUNNABLE -> RUNNING -> PARKING -> WAITING ->
    #    RUNNABLE -> RUNNING -> COMPLETED, with result.
    var t = TB.create()
    expect(t.state() == TB.NEW, "fresh TCB starts NEW")
    t.transition(TB.RUNNABLE)
    expect(t.state() == TB.RUNNABLE, "NEW -> RUNNABLE")
    t.transition(TB.RUNNING)
    expect(t.state() == TB.RUNNING, "RUNNABLE -> RUNNING")
    t.transition(TB.PARKING)
    expect(t.state() == TB.PARKING, "RUNNING -> PARKING")
    t.transition(TB.WAITING)
    expect(t.state() == TB.WAITING, "PARKING -> WAITING")
    t.mark_result(TInt(7))
    t.transition(TB.RUNNABLE)  # readiness wake
    expect(t.state() == TB.RUNNABLE, "WAITING -> RUNNABLE (readiness)")
    t.transition(TB.RUNNING)
    t.transition(TB.COMPLETED)
    expect(t.is_completed(), "RUNNING -> COMPLETED")
    var r = t.take_result()
    expect(r.v == 7, "result preserved through completion")

    # 2. Early wake: PARKING -> RUNNABLE (skip WAITING).
    var e = TB.create()
    e.transition(TB.RUNNABLE)
    e.transition(TB.RUNNING)
    e.transition(TB.PARKING)
    e.transition(TB.RUNNABLE)
    expect(e.state() == TB.RUNNABLE, "PARKING -> RUNNABLE (early wake)")

    # 3. Cancellation: WAITING -> RUNNABLE (same edge as readiness; the
    #    cause is stamped on the wait node by the scheduler lane).
    var tcr = TB.create()
    tcr.transition(TB.RUNNABLE)
    tcr.transition(TB.RUNNING)
    tcr.transition(TB.PARKING)
    tcr.transition(TB.WAITING)
    tcr.transition(TB.RUNNABLE)
    expect(tcr.state() == TB.RUNNABLE, "WAITING -> RUNNABLE (cancellation)")

    # 3b. RUNNING -> CANCELLED, then CANCELLED -> COMPLETED (legal).
    var tx = TB.create()
    tx.transition(TB.RUNNABLE)
    tx.transition(TB.RUNNING)
    tx.transition(TB.CANCELLED)
    expect(tx.is_cancelled(), "RUNNING -> CANCELLED")
    tx.transition(TB.COMPLETED)
    expect(tx.is_completed(), "CANCELLED -> COMPLETED")

    # 4. Every illegal transition raises (try/except via assert_illegal).
    var fresh0 = tcb_at(TB.NEW)
    var p0 = UnsafePointer[TB, MutAnyOrigin](to=fresh0)
    assert_illegal(p0, TB.NEW, "NEW -> NEW")
    assert_illegal(p0, TB.RUNNING, "NEW -> RUNNING")
    assert_illegal(p0, TB.PARKING, "NEW -> PARKING")
    assert_illegal(p0, TB.WAITING, "NEW -> WAITING")
    assert_illegal(p0, TB.COMPLETED, "NEW -> COMPLETED")
    assert_illegal(p0, TB.CANCELLED, "NEW -> CANCELLED")

    var fresh1 = tcb_at(TB.RUNNABLE)
    var p1 = UnsafePointer[TB, MutAnyOrigin](to=fresh1)
    assert_illegal(p1, TB.NEW, "RUNNABLE -> NEW")
    assert_illegal(p1, TB.RUNNABLE, "RUNNABLE -> RUNNABLE")
    assert_illegal(p1, TB.PARKING, "RUNNABLE -> PARKING")
    assert_illegal(p1, TB.WAITING, "RUNNABLE -> WAITING")
    assert_illegal(p1, TB.COMPLETED, "RUNNABLE -> COMPLETED")
    assert_illegal(p1, TB.CANCELLED, "RUNNABLE -> CANCELLED")

    var fresh2 = tcb_at(TB.RUNNING)
    var p2 = UnsafePointer[TB, MutAnyOrigin](to=fresh2)
    assert_illegal(p2, TB.NEW, "RUNNING -> NEW")
    assert_illegal(p2, TB.RUNNABLE, "RUNNING -> RUNNABLE")
    assert_illegal(p2, TB.WAITING, "RUNNING -> WAITING")
    assert_illegal(p2, TB.RUNNING, "RUNNING -> RUNNING")

    var fresh3 = tcb_at(TB.PARKING)
    var p3 = UnsafePointer[TB, MutAnyOrigin](to=fresh3)
    assert_illegal(p3, TB.NEW, "PARKING -> NEW")
    assert_illegal(p3, TB.RUNNING, "PARKING -> RUNNING")
    assert_illegal(p3, TB.PARKING, "PARKING -> PARKING")
    assert_illegal(p3, TB.COMPLETED, "PARKING -> COMPLETED")
    assert_illegal(p3, TB.CANCELLED, "PARKING -> CANCELLED")
    # PARKING -> RUNNABLE is LEGAL (early wake); exercised in test 2.

    var fresh4 = tcb_at(TB.WAITING)
    var p4 = UnsafePointer[TB, MutAnyOrigin](to=fresh4)
    assert_illegal(p4, TB.NEW, "WAITING -> NEW")
    assert_illegal(p4, TB.RUNNING, "WAITING -> RUNNING")
    assert_illegal(p4, TB.PARKING, "WAITING -> PARKING")
    assert_illegal(p4, TB.WAITING, "WAITING -> WAITING")
    assert_illegal(p4, TB.COMPLETED, "WAITING -> COMPLETED")
    assert_illegal(p4, TB.CANCELLED, "WAITING -> CANCELLED")

    var fresh5 = tcb_at(TB.COMPLETED)
    var p5 = UnsafePointer[TB, MutAnyOrigin](to=fresh5)
    assert_illegal(p5, TB.NEW, "COMPLETED -> NEW")
    assert_illegal(p5, TB.RUNNABLE, "COMPLETED -> RUNNABLE")
    assert_illegal(p5, TB.RUNNING, "COMPLETED -> RUNNING")
    assert_illegal(p5, TB.PARKING, "COMPLETED -> PARKING")
    assert_illegal(p5, TB.WAITING, "COMPLETED -> WAITING")
    assert_illegal(p5, TB.COMPLETED, "COMPLETED -> COMPLETED")
    assert_illegal(p5, TB.CANCELLED, "COMPLETED -> CANCELLED")

    var fresh6 = tcb_at(TB.CANCELLED)
    var p6 = UnsafePointer[TB, MutAnyOrigin](to=fresh6)
    assert_illegal(p6, TB.NEW, "CANCELLED -> NEW")
    assert_illegal(p6, TB.RUNNABLE, "CANCELLED -> RUNNABLE")
    assert_illegal(p6, TB.RUNNING, "CANCELLED -> RUNNING")
    assert_illegal(p6, TB.PARKING, "CANCELLED -> PARKING")
    assert_illegal(p6, TB.WAITING, "CANCELLED -> WAITING")
    assert_illegal(p6, TB.CANCELLED, "CANCELLED -> CANCELLED")

    # 4e. try_transition (CAS-style): right from+legal applies (True); wrong
    #     from or illegal pair does not apply (False, no raise).
    var c = TB.create()
    expect(not c.try_transition(TB.RUNNING, TB.PARKING), "wrong from rejected")
    expect(c.state() == TB.NEW, "state unchanged after failed try_transition")
    expect(not c.try_transition(TB.NEW, TB.CANCELLED), "illegal pair rejected")
    expect(c.state() == TB.NEW, "still NEW after illegal CAS pair")
    expect(c.try_transition(TB.NEW, TB.RUNNABLE), "legal CAS path accepted")
    expect(c.state() == TB.RUNNABLE, "CAS transition applied")
    expect(c.try_transition(TB.RUNNABLE, TB.RUNNING), "CAS RUNNABLE -> RUNNING")
    expect(c.try_transition(TB.RUNNING, TB.COMPLETED), "CAS RUNNING -> COMPLETED")
    expect(c.is_completed(), "CAS-advanced task completed")

    # 5. Consume-once: take before mark raises; second take raises.
    var cr = TB.create()
    var raised1 = False
    try:
        var _junk0 = cr.take_result()
    except Error:
        raised1 = True
    expect(raised1, "take_result before any result raises")
    raised1 = False
    try:
        var _junk1 = cr.take_result()
    except Error:
        raised1 = True
    expect(raised1, "second take_result raises")

    var dv = TB.create()
    dv.mark_result(TInt(3))
    var got = dv.take_result()
    expect(got.v == 3, "first take returns the value")
    var raised2 = False
    try:
        var _junk2 = dv.take_result()
    except Error:
        raised2 = True
    expect(raised2, "consume-once: second take after mark raises")

    # 6. Generation: bumped on each park commit (WAITING entry).
    var g = TB.create()
    expect(g.generation() == 1, "fresh TCB generation == 1")
    g.transition(TB.RUNNABLE)
    g.transition(TB.RUNNING)
    g.transition(TB.PARKING)
    g.transition(TB.WAITING)
    expect(g.generation() == 2, "generation bumped after first park commit")
    expect(
        g.wait_node()[].generation() == 2,
        "wait node stamps the claimed epoch",
    )
    g.transition(TB.RUNNABLE)  # readiness wake
    g.transition(TB.RUNNING)
    g.transition(TB.PARKING)
    g.transition(TB.WAITING)  # second park commit
    expect(g.generation() == 3, "generation bumped again on second park")

    print("T3 state machine: PASS")