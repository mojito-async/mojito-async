# mojito_async/test/unit/t11_runtime_lifecycle.mojo
#
# A1.1 (issue #33) — run() synchronous lifecycle + error re-raise.
#
# Acceptance: run() executes the ROOT task on the CALLING thread with the
# full TCB lifecycle NEW -> RUNNABLE -> RUNNING -> COMPLETED; no OS thread,
# no hidden blocking (semantics proven at A0; counters observable).  A raising
# root still reaches COMPLETED and the error is re-raised prefixed
# "runtime.run: root task raised:".  A shut-down runtime refuses.
#
# Verdict convention: exit 0 + "PASS"; any RED prints and raises so exit 1.
from mojito_async import Runtime, create


def stage_a():
    """Plain root (non-raising): runs once synchronously."""
    pass


def boom() raises:
    raise Error("root-boom")


def red(what: String) raises -> None:
    print("T11 runtime lifecycle: RED (" + what + ")")
    raise Error("t11 failure: " + what)


def main() raises:
    var rt = create()

    # 1. Normal root: synchronous, single execution, full lifecycle counters.
    rt.run(stage_a)
    if rt.tasks_started() != 1:
        red("tasks_started != 1")
    if rt.tasks_completed() != 1:
        red("tasks_completed != 1")
    if rt.pending() != 0:
        red("ready queue not quiet after run (hidden blocking?)")

    # 2. Raising root: still COMPLETED, error preserved and re-raised.
    var caught = False
    try:
        rt.run(boom)
    except e:
        caught = True
        var msg = String(e)
        if "root-boom" not in msg:
            red("raising-root message not preserved: " + msg)
    if not caught:
        red("root error not re-raised")
    if rt.tasks_completed() != 2:
        red("raising root did not reach COMPLETED")

    # 3. Shutdown: run() refuses afterwards.
    rt.shutdown()
    var refused = False
    try:
        rt.run(stage_a)
    except Error:
        refused = True
    if not refused:
        red("run after shutdown not rejected")

    print("T11 runtime lifecycle: PASS")