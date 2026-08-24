# A0-T1: colorlessness (issue #11, A0.2).
#
# Every function here is an ordinary Mojo `def`. There is no `async`/`await`,
# no `Future`, no hidden concurrency machinery anywhere in the call chain.
# That is the whole point of T1: a colorless task is just a plain function.
#
# This test is RED while runtime.run() is a stub: the task compiles and is
# accepted by the generic `def` callable slot, but the runtime cannot yet
# actually run it.  Reaching the stub's raise with the expected message
# confirms the plumbing (compile + surface); green lands with A0.4/A0.6 when
# run() executes `ordinary_step` end-to-end and this prints PASS.
#
# Verdict convention (suite matrix): print exactly one of PASS / RED / FAIL.
# The driver always exits 0; run.sh classifies by the printed verdict.
from runtime import Runtime, create


# A colorless task: no async, no await, no Future. Just ordinary def code.
def ordinary_step() -> None:
    pass


def main() raises:
    var rt = create()
    try:
        rt.run(ordinary_step)
        # run() did not raise.  With the A0.4/A0.6 scheduler this means the
        # colorless task actually executed: T1 is green.  (A stub that
        # silently swallowed the task would also land here and is wrong; the
        # scheduler lane may strengthen this to observe task side effects.)
        print("PASS: colorless task accepted and executed by run()")
    except e:
        var msg = String(e)
        if "not implemented" in msg:
            # Expected red state for A0.2: colorless task compiles and reaches
            # the stub error path. The scheduler is simply not implemented.
            print("RED: colorless task compiled; run() is a stub yet (A0.4/A0.6)")
        else:
            print("FAIL: unexpected error: " + msg)
    rt.shutdown()