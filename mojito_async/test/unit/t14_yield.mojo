# mojito_async/test/unit/t14_yield.mojo
#
# A1.1 (issue #33) — yield_now reschedules without blocking + the
# single-worker scheduler loop.
#
# Acceptance:
#   - yield_now() on a RUNNING task transitions it to RUNNABLE and re-enqueues
#     it (FIFO) WITHOUT sleeping/allocating; the call returns immediately and
#     the queue still holds the task (observable via rt.pending + state).
#   - scheduler_loop() FIFO-drives the runnable queue to quiet, executing each
#     RUNNABLE record via a statically-known dispatcher (the single worker's
#     cooperative drive).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop, yield_now
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn


def red(what: String) raises -> None:
    print("T14 yield: RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _tcb(mut t: TB) -> UnsafePointer[TB, MutAnyOrigin]:
    return UnsafePointer[TB, MutAnyOrigin](to=t)

def _scratch() -> BytePtr:
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x11)


def body_one(ud: BytePtr) raises -> IntResult:
    return IntResult(1)


# --- scheduler_loop dispatcher: execute a popped record to completion ------

def dispatch_step(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    _ = execute(h, body_one, ud)
    return 1


def main() raises:
    var rt = create()
    var scratch = _scratch()

    # ---- 1. yield_now: RUNNING -> RUNNABLE + FIFO re-enqueue, non-blocking.
    var tcb = TB.create()
    var h = spawn(rt, _tcb(tcb), 0)   # NEW -> RUNNABLE, enqueued once
    claim_running(h)                  # RUNNABLE -> RUNNING
    _ = rt.pop_ready()                # scheduler dequeues A's record
    if rt.pending() != 0:
        red("expected empty ready queue after pop")

    yield_now(rt, h)                  # RUNNING -> RUNNABLE + re-enqueue (FIFO)
    if h.state() != TaskControlBlock.RUNNABLE:
        red("yield_now did not reschedule the task to RUNNABLE")
    if rt.pending() != 1:
        red("yield_now did NOT re-enqueue (it must not block/consume)")

    # Single-worker scheduler loop re-serves the yielded task to quiet.
    var slices = scheduler_loop(rt, dispatch_step, scratch)
    if slices != 1:
        red("unexpected record count after yielding: " + String(slices))
    if not h.is_completed():
        red("rescheduled task did not reach COMPLETED on the re-run")

    print("T14 yield: PASS")