# mojito_async/test/unit/t27_stack_liveness_aot.mojo
#
# A1.4 (issue #52) — T6 liveness fold negative driver (next free number).
#
# Proves the reuse-gate in StackCache.release() catches invalid release calls:
#   (A) double-release of a cell RAISES ("not live") rather than silently
#       corrupting the free list.  This is the STATE_LIVE guard in release().
#
# Note on the original "release-after-external-free" scenario: that scenario
# tested ms_stack_is_live inside release(), which was REMOVED by issue #145
# Bug 2 (Fiber.destroy no longer calls ms_stack_free for pool-acquired cells
# via the _is_pooled flag, so the external-free path cannot occur in
# production).  Adding ms_stack_is_live back to release() causes false
# negatives when the OS reuses a recently-freed virtual address for a fresh
# allocation from a different pool (the dead-list fires on the new address),
# making the check unreliable in multi-pool scenarios.  The double-release
# test below is the canonical valid negative for the reuse gate.
#
# AOT-only: imports the production vendor seam, so it runs via
# the run.sh unit AOT loop (`*_aot.mojo`).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).

from mojito_async.fiber.stack_pool import make_stack_cache
from mojito_async.vendor.mojito_sys import ms_stack_is_live


def red(what: String) raises -> None:
    print("T27 stack liveness: RED (" + what + ")")
    raise Error(what)


def main() raises:
    # --- (A) double-release raises (STATE_LIVE reuse-gate) -------------------
    var pool_a = make_stack_cache(2, 65536)
    var a = pool_a.acquire()
    var a_base = a[].base
    if ms_stack_is_live(a_base) == 0:
        red("fresh acquire did not produce a live reservation")
    pool_a.release(a)  # first release: STATE_LIVE -> STATE_CACHED, OK
    var double_raised = False
    try:
        pool_a.release(a)  # second release: STATE_CACHED != STATE_LIVE -> raises
    except e:
        double_raised = True
    if not double_raised:
        red("double-release did not raise (reuse-gate violation)")

    print("T27 stack liveness: PASS")
