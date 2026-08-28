# mojito_async/test/unit/t23_stack_cache_aot.mojo
#
# A1.4 (issue #52) — per-worker stack cache acceptance driver.
#
# Covers the issue's exit criteria over the vendored mojito-sys substrate:
#   - acquire then release of equal-size stacks within budget: a warm
#     re-acquire hits the freelist and ms_stack_total_size() stays flat
#     (no fresh OS allocation after warmup);
#   - bounded capacity: releasing then re-acquiring REUSES the same cell
#     (committed bytes unchanged), and the pool never grows past capacity;
#   - reuse gate: a cell is never recycled into a new acquire while the
#     prior owner is still live — releasing a non-live cell raises;
#   - exhausted pool: acquiring past capacity raises;
#   - retire/drain return reservations to the OS (committed bytes shrink).
#
# AOT-only: this driver imports the production vendor seam (NativeStack /
# ms_* externs), and the b2 JIT cannot resolve dylib symbols through an
# imported module (modular/modular#6971) — so it runs as `mojo build` +
# execute via the run.sh unit AOT loop (`*_aot.mojo`).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).

from mojito_async.fiber.stack_pool import make_stack_cache
from mojito_async.vendor.mojito_sys import NativeStack


def red(what: String) raises -> None:
    print("T23 stack cache: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var pool = make_stack_cache(4, 65536)

    # --- acquire/release round-trip + warm reuse (flat committed bytes) ----
    var a = pool.acquire()
    var b = pool.acquire()
    if pool.live() != 2:
        red("expected 2 live after two acquires")
    var t_cold = pool.total_live_bytes()
    if t_cold <= 0:
        red("cold acquire produced no committed bytes")
    pool.release(a)
    pool.release(b)
    if pool.cached() != 2 or pool.live() != 0:
        red("release did not move cells to the free set")

    var t_warm_release = pool.total_live_bytes()
    # Warm re-acquire of the SAME 2 stacks: freelist hits, NO fresh alloc.
    var c = pool.acquire()
    var d = pool.acquire()
    if pool.live() != 2:
        red("warm acquire did not hand out cells")
    var t_warm = pool.total_live_bytes()
    if t_warm != t_warm_release:
        red("warm re-acquire grew committed bytes (no freelist reuse)")
    pool.release(c)
    pool.release(d)

    # --- bounded capacity / limits respected ------------------------------
    # Acquire past capacity (4) raises: pool cannot grow past bounds.
    var e1 = pool.acquire()
    var e2 = pool.acquire()
    var e3 = pool.acquire()
    var e4 = pool.acquire()
    if pool.live() != 4:
        red("expected 4 live at capacity")
    var exhausted = False
    try:
        var e5 = pool.acquire()
        pool.release(e5)
    except Error:
        exhausted = True
    if not exhausted:
        red("pool did not raise when exhausted past capacity")
    # Return to a usable state.
    pool.release(e1)
    pool.release(e2)
    pool.release(e3)
    pool.release(e4)

    # --- reuse gate: never recycle a live (still-owned) cell --------------
    var g = pool.acquire()
    var released = False
    try:
        # Releasing `g` twice is the reuse-gate violation: the second
        # release sees a cell that is no longer live (already returned).
        pool.release(g)
        pool.release(g)  # must raise
    except Error:
        released = True
    if not released:
        red("release of a non-live cell did not raise (reuse-gate violation)")
    # Releasing a pointer that is not owned by this pool must raise too.
    var foreign = UnsafePointer[NativeStack, MutAnyOrigin](
        unsafe_from_address=1
    )
    var foreign_raised = False
    try:
        pool.release(foreign)
    except Error:
        foreign_raised = True
    if not foreign_raised:
        red("release of a foreign pointer did not raise")

    # --- retire / drain: committed bytes shrink (decommit-equivalent) -----
    var r1 = pool.acquire()
    var r2 = pool.acquire()
    pool.release(r1)
    pool.release(r2)
    var before_retire = pool.total_live_bytes()
    var cached_before = pool.cached()
    # retire() returns a CACHED reservation to the OS (drops it from the
    # free set and munmaps); committed bytes must shrink by 2 reservations.
    pool.retire(r1)
    pool.retire(r2)
    if pool.total_live_bytes() >= before_retire:
        red("retire did not shrink committed bytes (reservation not freed)")
    if pool.cached() != cached_before - 2:
        red("retire did not remove cells from the free set")

    print("T23 stack cache: PASS")
