# mojito_async/test/unit/t27_stack_liveness_aot.mojo
#
# A1.4 (issue #52) — T6 liveness fold.  Updated after issue #145 Bug 2:
# the pool is the SOLE OWNER of pool cells (Fiber.destroy() sets _is_pooled
# and skips ms_stack_free).  The pool's STATE array is the single source of
# truth for cell liveness; ms_stack_is_live is no longer probed in the
# pool's hot paths (acquire/release) — the ABA dead-list in the C registry
# would yield false negatives for freshly re-allocated addresses anyway.
#
# Tests:
#   (A) State-based reuse gate: double-release of a cell raises
#       (pool's STATE check, not a liveness probe).
#   (B) Warm-path correctness: re-acquire of a cached cell hands back the
#       same cached reservation (freelist works; pool is the sole owner so
#       the reservation is still live between release and re-acquire).
#   (C) Cold-path liveness: a freshly cold-allocated reservation is registered
#       live in the C registry (ms_stack_is_live probe on a FRESH alloc, no
#       ABA concern since no prior ms_stack_free for that address in this run).
#
# AOT-only: imports the production vendor seam (ms_stack_*).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).

from mojito_async.fiber.stack_pool import make_stack_cache
from mojito_async.vendor.mojito_sys import ms_stack_is_live


def red(what: String) raises -> None:
    print("T27 stack liveness: RED (" + what + ")")
    raise Error(what)


def main() raises:
    # --- (A) state-based reuse gate: double-release raises ----------------
    var pool_a = make_stack_cache(2, 65536)
    var a = pool_a.acquire()
    pool_a.release(a)      # first: STATE_LIVE -> STATE_CACHED, OK
    var double_raised = False
    try:
        pool_a.release(a)  # second: STATE_CACHED != STATE_LIVE -> raises
    except Error:
        double_raised = True
    if not double_raised:
        red("double-release did not raise (reuse-gate violated)")

    # --- (B) warm path: re-acquire returns the same cached reservation ----
    var pool_b = make_stack_cache(2, 65536)
    var b = pool_b.acquire()
    var b_base = b[].base
    pool_b.release(b)
    var c = pool_b.acquire()  # warm hit: pool is sole owner, reservation live
    if c[].base != b_base:
        red("warm acquire returned a different base (freelist broken)")
    pool_b.release(c)

    # --- (C) cold path: fresh alloc is registered live in C registry ------
    var pool_c = make_stack_cache(1, 65536)
    var d = pool_c.acquire()
    var d_base = d[].base
    if ms_stack_is_live(d_base) == 0:
        red("fresh cold acquire not registered live in the C registry")
    pool_c.release(d)

    print("T27 stack liveness: PASS")
