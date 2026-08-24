# A0-T1: colorlessness (issue #11; scheduler: #15, A0.6).
#
# Every function here is an ordinary Mojo `def`. There is no `async`/`await`,
# no `Future`, no hidden concurrency machinery anywhere in the call chain.
# That is the whole point of T1: a colorless task is just a plain function.
#
# run() now EXECUTES the root task on the calling thread with a full TCB
# lifecycle (NEW -> RUNNABLE -> RUNNING -> COMPLETED).  Per the Rev24Api
# review, PASS requires an OBSERVED SIDE EFFECT of the task itself, not
# merely "did not raise": each task writes a marker line AND bumps the
# runtime's execution counters, and a second run() must increment them again
# (a stub that swallowed tasks could fake one counter, not two runs).
#
# Verdict convention (suite matrix): print exactly one of PASS / RED / FAIL
# and exit 0 for PASS, 1 for RED/FAIL.
from runtime import Runtime, create


@extern("exit")
def _c_exit(code: Int32) abi("C"): ...


# A colorless task: no async, no await, no Future. Just ordinary def code.
def ordinary_step() -> None:
    print("T1 marker: ordinary_step executed")


def main() raises:
    var rt = create()
    try:
        rt.run(ordinary_step)
    except e:
        var msg = String(e)
        if "not implemented" in msg:
            print("RED: colorless task compiled; run() is a stub yet (A0.4/A0.6)")
            _c_exit(1)
        print("FAIL: unexpected error: " + msg)
        _c_exit(1)

    # Observable effects of REAL execution: the task's own marker line above,
    # plus per-run counters (two runs => two starts/completions).
    var ok_counters = (
        rt.tasks_started() == 1 and rt.tasks_completed() == 1
    )
    try:
        rt.run(ordinary_step)
        ok_counters = ok_counters and (
            rt.tasks_started() == 2 and rt.tasks_completed() == 2
        )
    except e:
        print("FAIL: second run raised: " + String(e))
        _c_exit(1)

    # NOTE: the root-error re-raise path of run() cannot be exercised here:
    # the locked run() surface constrains the task to the NON-raising
    # `def() -> None` callable trait, so an ordinary colorless root cannot
    # raise at all (child error propagation through joiners is covered by
    # t6_join_semantics_aot, A0-T8).
    if not ok_counters:
        print("FAIL: run() did not account both task executions")
        _c_exit(1)

    print("PASS: colorless task accepted and executed by run()")
    rt.shutdown()
