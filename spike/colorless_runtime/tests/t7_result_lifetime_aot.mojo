# spike/colorless_runtime/tests/t7_result_lifetime_aot.mojo
#
# A0.6 (issue #15) — RESULT LIFETIME: A0-T6 double-join rejection, A0-T7
# abandoned-result destruction exactly once.
#
#   A0-T6 one-shot join: the FIRST join() consumes the handle; any further
#         consumption attempt (join, or abandon-after-consume) is rejected.
#   A0-T7 abandoned result: a completed task whose result is NEVER joined is
#         torn down by JoinHandle.abandon() with EXACTLY ONE destruction of
#         the stored result; a second abandon raises and destroys nothing;
#         a CONSUMED (joined) result is not re-destroyed by abandonment.
#
# PROBE MECHANICS (b2-honest): the probe counts through a heap counter cell
# reached via a pointer field (the userdata pattern — no module globals).
# b2 passes arguments by value, so the completion path materializes copies:
# the producer's local AND the TCB's stored copy are distinct instances, each
# destroyed exactly once (copy elision across by-value boundaries is absent
# in b2).  The invariant under test is per-instance: every materialized
# result instance is destroyed EXACTLY ONCE — never zero times (leak), never
# twice (double free) — asserted via deterministic deltas at each checkpoint.
#
# Scratch/counter cells are HEAP-backed (malloc); stack_allocation memory is
# not protected from frame reuse once values escape across calls.  The Probe
# values themselves live only in normal (initialized) locals/slots: b2
# crashes when values with __del__ are assigned into raw uninitialized
# storage (observed; see PR notes).
#
# AOT driver (mojo build + execute) per tests/run.sh.  Verdict convention:
# PASS / RED / FAIL with exit 0 / 1 / 1.
from mojito_spike import BytePtr

from runtime import Runtime, create
from task import TaskControlBlock, ResultValue
from spawn import JoinHandle, abandon, execute, spawn


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


@extern("malloc")
def _c_malloc(size: Int) abi("C") -> BytePtr: ...


# --- probe result ------------------------------------------------------------

struct Probe(ResultValue):
    """Counting probe: increments a shared heap cell exactly once per
    instance destruction (guarded by `bound` so unbound copies are silent)."""

    var v: Int
    var cnt: UnsafePointer[Int, MutAnyOrigin]
    var bound: Bool

    def __init__(out self):
        self.v = 0
        self.cnt = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.bound = False

    def __init__(out self, cell: UnsafePointer[Int, MutAnyOrigin], val: Int):
        self.v = val
        self.cnt = cell
        self.bound = True

    def __del__(deinit self):
        if self.bound:
            self.cnt[] = self.cnt[] + 1


def p_step(ud: BytePtr) raises -> Probe:
    var cell = ud.bitcast[Int]()
    return Probe(cell, 42)


def q_step(ud: BytePtr) raises -> Probe:
    var cell = ud.bitcast[Int]()
    return Probe(cell, 43)


# --- driver -------------------------------------------------------------------

def main() raises:
    var failures = List[String]()
    var cnt_cell = _c_malloc(8).bitcast[Int]()
    var ud = cnt_cell.bitcast[Byte]()

    var rt = create()

    # ---- RED detection -------------------------------------------------------
    var tcb_p = TaskControlBlock[Probe]()
    var h_p = JoinHandle[Probe](
        UnsafePointer[TaskControlBlock[Probe], MutAnyOrigin](unsafe_from_address=1), 0
    )
    try:
        h_p = spawn(rt, UnsafePointer[TaskControlBlock[Probe], MutAnyOrigin](to=tcb_p), 0)
    except e:
        var msg = String(e)
        if "not implemented" in msg:
            print("T7 result lifetime: RED (A0.6 surface not implemented)")
            _iso_exit(1)
        print("T7 result lifetime: FAIL (unexpected spawn error: " + msg + ")")
        _iso_exit(1)

    # ==== A0-T7 scenario 1: abandoned result ==================================
    # execute(P): the producer's own bound instance dies inside the task
    # frame; the TCB stores a separate bound copy.
    _ = execute(rt, h_p, p_step, ud)
    var after_exec = cnt_cell[]
    if after_exec != 1:
        failures.append(
            "expected producer-local destruction delta 1 after execute, got "
            + String(after_exec)
        )
    if not h_p.is_completed():
        failures.append("P did not complete")

    # Nobody ever joins P.  Deterministic teardown destroys the STORED result
    # exactly once:
    abandon(h_p)
    var after_abandon = cnt_cell[]
    if after_abandon != after_exec + 1:
        failures.append(
            "A0-T7: stored result not destroyed exactly once by abandon (delta "
            + String(after_abandon - after_exec)
            + ")"
        )

    # A second abandon must be rejected and destroy nothing further:
    try:
        abandon(h_p)
        failures.append("A0-T7: double abandon was NOT rejected")
    except e:
        var m = String(e)
        if not ("already consumed" in m):
            failures.append("A0-T7: double-abandon message unexpected: " + m)
    if cnt_cell[] != after_abandon:
        failures.append("A0-T7: rejected abandon still destroyed state")

    # join-after-abandon must be rejected too:
    try:
        _ = h_p.join()
        failures.append("A0-T7: join after abandon was NOT rejected")
    except e:
        pass
    if cnt_cell[] != after_abandon:
        failures.append("A0-T7: rejected join destroyed state")

    # ==== A0-T6 scenario: one-shot join =======================================
    var tcb_q = TaskControlBlock[Probe]()
    var h_q = spawn(rt, UnsafePointer[TaskControlBlock[Probe], MutAnyOrigin](to=tcb_q), 0)
    _ = execute(rt, h_q, q_step, ud)
    var before_join = cnt_cell[]
    if before_join != after_abandon + 1:
        failures.append(
            "expected producer-local delta 1 after Q execute, got "
            + String(before_join - after_abandon)
        )

    var res_q = h_q.join()
    if res_q.v != 43:
        failures.append("A0-T6: consumed result value corrupted")
    # b2 by-value boundaries materialize a transient copy inside take_result
    # (its out-local dies on return); record the post-consume baseline.
    var after_join = cnt_cell[]

    # The result was moved OUT; abandoning afterwards must be rejected:
    try:
        abandon(h_q)
        failures.append("A0-T6: abandon after join was NOT rejected")
    except e:
        pass

    # Second consumption attempt (fresh gate check via the TCB slot itself):
    try:
        _ = h_q.tcb()[].take_result()
        failures.append("A0-T6: take_result succeeded twice (consume-once broken)")
    except e:
        var m = String(e)
        if not ("no result" in m):
            failures.append("A0-T6: double-take error unexpected: " + m)

    # No destruction may happen around the REJECTED attempts:
    if cnt_cell[] != after_join:
        failures.append(
            "rejected consumptions perturbed lifetime counters (delta "
            + String(cnt_cell[] - after_join)
            + ")"
        )

    # Reclaim of the DEAD slot storage destroys the leftover slot object
    # exactly once (b2 has no move-out of the slot: take_result copied out and
    # flagged it empty, but the slot's value bytes remain until overwritten):
    var before_reclaim = cnt_cell[]
    h_q.tcb()[] = TaskControlBlock[Probe]()
    if cnt_cell[] != before_reclaim + 1:
        failures.append("slot reclaim destroyed the dead slot object more than once")

    rt.shutdown()

    if len(failures) == 0:
        print("T7 result lifetime: PASS")
    else:
        print("T7 result lifetime: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
