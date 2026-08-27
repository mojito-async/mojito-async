# mojito_async/test/unit/t18_root_api.mojo
#
# A1.1 (issue #33) — the §7.1 root API line, imported verbatim, plus the
# Worker.drive instantiation (fold: drive's dispatcher bound must compile and
# run against scheduler_loop).
#
# Imports the EXACT spec §7.1 root line and exercises each name at compile+
# run time: spawn/join, yield_now (via the root export), checkpoint,
# CancellationToken, Deadline, sleep(Duration).  sleep is a documented stub in
# A1.1: it must COMPILE and raise its precise "not implemented" error.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async import (
    Scope,
    JoinHandle,
    CancellationToken,
    Deadline,
    sleep,
    yield_now,
    checkpoint,
)
from mojito_async.cancellation import CancelFlag, make_cancel_flag
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import make_worker
from mojito_async.task import claim_running, execute, spawn
from mojito_async.time.deadline import Duration, from_millis


def red(what: String) raises -> None:
    print("T18 root api: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def body(ud: BytePtr) raises -> IntResult:
    return IntResult(5)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    _ = execute(h, body, ud)
    return 1


def main() raises:
    # ---- §7.1 names must exist and be usable -------------------------------
    var rt = create()
    var wk = make_worker()
    var d = Deadline(1234)
    if d.at_ms() != 1234:
        red("Deadline round-trip failed")
    var dur = from_millis(250)
    if dur.to_millis() != 250:
        red("Duration.from_millis/to_millis round-trip failed")

    # sleep(Duration) compiles and raises its A1.4 stub error (never blocks).
    var sleep_raised = False
    try:
        sleep(dur)
    except e:
        sleep_raised = True
        if "A1.4 timer lane" not in String(e):
            red("sleep stub error unexpected: " + String(e))
    if not sleep_raised:
        red("sleep(Duration) did not raise its stub error")

    # checkpoint + CancellationToken through the ROOT exports.
    var flag = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=flag)
    var tok = CancellationToken(fp)
    var not_req = True
    try:
        checkpoint(tok)  # not requested: silent
    except Error:
        not_req = False
    if not not_req:
        red("checkpoint on an unrequested token raised")
    tok.request()
    var raised = False
    try:
        checkpoint(tok)
    except Error:
        raised = True
    if not raised:
        red("checkpoint on a requested token did not raise")

    # ---- spawn + yield_now (root) + join through Worker.drive --------------
    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)
    var scratch = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x33)
    _ = execute(h, body, scratch)
    if not h.is_completed():
        red("child did not complete")
    var res = h.join()
    if res.v != 5:
        red("root-import join result wrong: " + String(res.v))

    # yield_now must be callable with (rt, handle) — exercised on a fresh
    # RUNNING task through a Worker-drive (fold: compile-time instantiation
    # of Worker.drive against scheduler_loop's dispatcher bound).
    var tcb2 = TB.create()
    var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb2), 0)
    claim_running(h2)
    _ = rt.pop_ready()
    yield_now(rt, h2)
    if h2.state() != TaskControlBlock.RUNNABLE:
        red("yield_now (root export) did not reschedule")

    # Worker.drive drives ITS OWN runtime: spawn a task into the worker
    # (via wk.handle()) and let the worker's scheduler loop serve it.
    var tcb3 = TB.create()
    _ = spawn(wk.handle()[], UnsafePointer[TB, MutAnyOrigin](to=tcb3), 0)
    var served = wk.drive(dispatch, scratch)
    if served != 1:
        red("Worker.drive served " + String(served) + " records, expected 1")

    print("T18 root api: PASS")