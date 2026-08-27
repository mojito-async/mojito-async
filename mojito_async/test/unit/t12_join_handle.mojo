# mojito_async/test/unit/t12_join_handle.mojo
#
# A1.1 (issue #33) — spawn + one-shot JoinHandle semantics.
#
# Acceptance (A0-T4/T5/T6/T7/T8 carried forward):
#   - spawn registers a NEW task (NEW -> RUNNABLE) with a caller-allocated
#     TaskControlBlock cell;
#   - execute settles COMPLETED and stores the result;
#   - join is ONE-SHOT: result consumed once, double-join rejected;
#   - abandon deterministically destroys an unconsumed completed result;
#   - a raising child reaches COMPLETED with the message preserved and
#     join() re-raises it.
#
# Verdict: exit 0 + "PASS"; any RED prints and raises (exit 1).
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, abandon, execute, spawn


def red(what: String) raises -> None:
    print("T12 join handle: RED (" + what + ")")
    raise Error(what)


# --- task bodies (userdata channel; ud is a scratch pointer, unused here) ---

def a_body(ud: BytePtr) raises -> IntResult:
    return IntResult(111)


def b_body(ud: BytePtr) raises -> IntResult:
    return IntResult(999)


def err_body(ud: BytePtr) raises -> IntResult:
    raise Error("fast-disk: child boom")


# --- scratch userdata (a live pointer; bodies ignore it) --------------------

def _scratch() -> BytePtr:
    """A live, never-dereferenced pointer (bodies ignore userdata)."""
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x10)


def _ptr(mut tcb: TaskControlBlock[IntResult]) -> UnsafePointer[
    TaskControlBlock[IntResult], MutAnyOrigin
]:
    return UnsafePointer[TaskControlBlock[IntResult], MutAnyOrigin](to=tcb)


def main() raises:
    var rt = create()
    var scratch = _scratch()

    # 1. spawn + execute + join (consume-once result).
    var tcb_a = TaskControlBlock[IntResult]()
    var h_a = spawn(rt, _ptr(tcb_a), 0)
    _ = execute(h_a, a_body, scratch)
    if not h_a.is_completed():
        red("child not COMPLETED after execute")
    var res_a = h_a.join()
    if res_a.v != 111:
        red("join result not consumed intact (got " + String(res_a.v) + ")")

    # Double join rejected (one-shot).
    var dbl = False
    try:
        _ = h_a.join()
    except Error:
        dbl = True
    if not dbl:
        red("double join was NOT rejected")

    # ---- 2. abandon: deterministically destroy an unconsumed result -------
    var tcb_b = TaskControlBlock[IntResult]()
    var h_b = spawn(rt, _ptr(tcb_b), 0)
    _ = execute(h_b, b_body, scratch)
    abandon(h_b)
    var joined_abandoned = False
    try:
        _ = h_b.join()
    except Error:
        joined_abandoned = True
    if not joined_abandoned:
        red("join after abandon accepted")

    # ---- 3. child error propagation: COMPLETED + join re-raise ------------
    var tcb_e = TaskControlBlock[IntResult]()
    var h_e = spawn(rt, _ptr(tcb_e), 0)
    _ = execute(h_e, err_body, scratch)
    if not h_e.is_completed():
        red("raising child did not reach COMPLETED")
    if not h_e.is_failed():
        red("raising child not marked failed")
    var re_raised = False
    try:
        _ = h_e.join()
    except e:
        re_raised = True
        var msg = String(e)
        if "child boom" not in msg:
            red("child error message not preserved: " + msg)
    if not re_raised:
        red("child error not re-raised by join")

    print("T12 join handle: PASS")