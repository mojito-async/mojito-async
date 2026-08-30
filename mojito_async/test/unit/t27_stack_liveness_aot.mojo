# mojito_async/test/unit/t27_stack_liveness_aot.mojo
#
# A1.4 (issue #52 / #145) — StackCache state-based liveness driver.
#
# Issue #145 Bug 2 (merged PR #164) changed the liveness model: Fiber.destroy()
# no longer calls ms_stack_free for pool-acquired cells (_is_pooled flag);
# pool.retire()/drain() are the only callers of ms_stack_free for pool cells.
# The pool's STATE array is consequently the sole source of truth for cell
# liveness — CACHED cells are guaranteed live without any ms_stack_is_live
# probe, because the pool is the SOLE OS owner of each cell's reservation.
#
# This driver tests the STATE-based invariants that replaced the probe:
#   (A) release() raises on double-release (STATE_LIVE guard enforced).
#       Before #145, this required a ms_stack_is_live probe; now the state
#       machine alone closes the reuse-gate.
#   (B) A CACHED cell's reservation stays live after release() — the pool's
#       sole-ownership guarantee; warm acquire() reuses the same base
#       without cold-allocating.
#
# AOT-only: imports the production vendor seam (ms_stack_*), so it runs via
# the run.sh unit AOT loop (`*_aot.mojo`).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async.fiber.stack_pool import make_stack_cache
from mojito_async.vendor.mojito_sys import ms_stack_is_live


def red(what: String) raises -> None:
    print("T27 stack liveness: RED (" + what + ")")
    raise Error(what)


def main() raises:
    # --- (A) double-release raises (STATE_LIVE guard) -----------------------
    # release() transitions a cell LIVE -> CACHED.  A second release() on the
    # same cell finds STATE_CACHED, not STATE_LIVE, and must raise the
    # reuse-gate error (issue #52: "Attempting to release a cell that is not
    # live raises").
    var pool_a = make_stack_cache(2, 65536)
    var a = pool_a.acquire()
    if ms_stack_is_live(a[].base) == 0:
        red("fresh acquire did not produce a live reservation")
    pool_a.release(a)  # LIVE -> CACHED; must succeed
    var double_raised = False
    try:
        pool_a.release(a)  # STATE_CACHED, not LIVE; must raise
    except Error:
        double_raised = True
    if not double_raised:
        red("double release did not raise (STATE_LIVE guard not enforced)")

    # --- (B) CACHED reservation stays live; warm acquire reuses it ----------
    # The pool is the SOLE OS owner of its cells (issue #145 Bug 2): no
    # external code frees pool cells' reservations.  Therefore a CACHED
    # cell's reservation must remain live (ms_stack_is_live == 1) between
    # release() and the next acquire().  The warm path must return the SAME
    # base address (reuse, not cold-allocate).
    var pool_b = make_stack_cache(2, 65536)
    var b = pool_b.acquire()
    var b_base = b[].base
    if ms_stack_is_live(b_base) == 0:
        red("fresh acquire did not produce a live reservation")
    pool_b.release(b)  # LIVE -> CACHED; reservation must stay live
    if ms_stack_is_live(b_base) == 0:
        red("CACHED reservation is not live — sole-ownership guarantee violated (issue #145 Bug 2)")
    var c = pool_b.acquire()  # warm hit: must reuse the CACHED cell at b_base
    if ms_stack_is_live(c[].base) == 0:
        red("warm acquire handed out a dead reservation")
    if Int(c[].base) != Int(b_base):
        red("warm acquire cold-allocated a fresh reservation when a CACHED live cell was available")
    pool_b.release(c)

    print("T27 stack liveness: PASS")
