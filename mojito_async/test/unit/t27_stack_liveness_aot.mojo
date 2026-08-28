# mojito_async/test/unit/t27_stack_liveness_aot.mojo
#
# A1.4 (issue #52) — T6 liveness fold negative driver (next free number).
#
# Proves the liveness guard over the vendored ms_stack_is_live extern closes
# the StackCache->Fiber ownership gap:
#   (A) release() of a cell whose reservation was munmapped underneath the
#       pool (a Fiber.destroy that freed the stack before the owner called
#       release) RAISES a loud "reservation already freed (fiber destroy
#       before release?)" error instead of silently re-adding a dangling base
#       to the free pool;
#   (B) warm acquire() on a cached cell whose reservation was freed
#       underneath it COLD-REALLOCATES a fresh stack (never hands out the
#       dangling base): the returned reservation is live again (cold path —
#       the regenerated mapping, even if it reuses the same address, must be
#       re-registered live, not the munmapped one).
#
# AOT-only: imports the production vendor seam (ms_stack_*), so it runs via
# the run.sh unit AOT loop (`*_aot.mojo`).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).

from mojito_async.fiber.stack_pool import make_stack_cache
from mojito_async.vendor.mojito_sys import ms_stack_is_live, ms_stack_free


def red(what: String) raises -> None:
    print("T27 stack liveness: RED (" + what + ")")
    raise Error(what)


def main() raises:
    # --- (A) release-after-destroy raises (no silent dangling-base pooling) ---
    var pool_a = make_stack_cache(2, 65536)
    var a = pool_a.acquire()
    var a_base = a[].base
    if ms_stack_is_live(a_base) == 0:
        red("fresh acquire did not produce a live reservation")
    # Simulate a Fiber.destroy munmapping this reservation underneath the pool.
    ms_stack_free(a_base)
    if ms_stack_is_live(a_base) != 0:
        red("setup: ms_stack_free failed to drop the reservation")
    var destroyed_raised = False
    try:
        pool_a.release(a)
    except Error:
        destroyed_raised = True
    if not destroyed_raised:
        red("release after destroy did not raise (a dangling base would be pooled)")

    # --- (B) warm-acquire never hands out a freed reservation --------------
    var pool_b = make_stack_cache(2, 65536)
    var b = pool_b.acquire()
    var b_base = b[].base
    pool_b.release(b)  # -> CACHED; reservation still live in the registry
    if ms_stack_is_live(b_base) == 0:
        red("released (cached) reservation unexpectedly not live")
    # Simulate the reservation being freed (munmap) between release and
    # re-acquire — the exact dangling-base hazard the warm path must close.
    ms_stack_free(b_base)
    if ms_stack_is_live(b_base) != 0:
        red("setup: freeing the cached reservation failed")
    var c = pool_b.acquire()  # warm hit on the now-stale cell
    var c_base = c[].base
    if ms_stack_is_live(c_base) == 0:
        red("warm acquire handed out a dead (freed) reservation")
    # The returned cell must be a live reservation again (cold-allocated),
    # never the munmapped base.
    pool_b.release(c)

    print("T27 stack liveness: PASS")