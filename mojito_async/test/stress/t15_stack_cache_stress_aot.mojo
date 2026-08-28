# mojito_async/test/stress/t15_stack_cache_stress_aot.mojo
#
# A1.4 (issue #52) — stack-cache churn/leak stress driver.
#
# Exit criterion (issue #52): "100k acquire/release cycles end at the memory
# baseline (no leak) with the pool still bounded."
#
# Drives CYCLES = 100_000 acquire -> release round-trips over a SMALL pool
# (capacity 4).  Because the pool is bounded and reuses the free set, a
# warm acquire performs NO fresh ms_stack_alloc: after warm-up the committed
# bytes (ms_stack_total_size) stay flat at exactly capacity * reservation,
# and process peak RSS stops growing.  A per-cycle stack leak would show as
# unbounded committed growth across the 100k cycles.
#
# The driver samples committed bytes after warm-up and at the end, and peak
# RSS (getrusage RUSAGE_SELF ru_maxrss) before/after the churn.  It asserts:
#   - committed bytes after churn == committed bytes after warm-up (flat,
#     bounded — no leak, pool still bounded);
#   - acquire never raises once the pool has capacity available through
#     reuse (a leak that exhausts released cells WOULD raise).
#
# AOT-only: imports the production vendor seam (getrusage + NativeStack /
# ms_* externs), so it runs as `mojo build` + execute via the run.sh AOT
# loop (modular/modular#6971: JIT cannot resolve dylib symbols through an
# imported module).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List

from mojito_async.fiber.stack_pool import make_stack_cache
from mojito_async.vendor.mojito_sys import NativeStack, ms_stack_total_size

def red(what: String) raises -> None:
    print("T15 stack cache stress: RED (" + what + ")")
    raise Error(what)


# --- getrusage (darwin RUSAGE_SELF ru_maxrss, peak RSS in bytes) -----------

struct RUsage(ImplicitlyCopyable, ImplicitlyDeletable):
    var ru_utime_sec: Int64
    var ru_utime_usec: Int64
    var ru_stime_sec: Int64
    var ru_stime_usec: Int64
    var ru_maxrss: Int64
    var ru_ixrss: Int64
    var ru_idrss: Int64
    var ru_isrss: Int64
    var ru_minflt: Int64
    var ru_majflt: Int64
    var ru_nswap: Int64
    var ru_inblock: Int64
    var ru_oublock: Int64
    var ru_msgsnd: Int64
    var ru_msgrcv: Int64
    var ru_nsignals: Int64
    var ru_nvcsw: Int64
    var ru_nivcsw: Int64

    def __init__(out self):
        self.ru_utime_sec = 0
        self.ru_utime_usec = 0
        self.ru_stime_sec = 0
        self.ru_stime_usec = 0
        self.ru_maxrss = 0
        self.ru_ixrss = 0
        self.ru_idrss = 0
        self.ru_isrss = 0
        self.ru_minflt = 0
        self.ru_majflt = 0
        self.ru_nswap = 0
        self.ru_inblock = 0
        self.ru_oublock = 0
        self.ru_msgsnd = 0
        self.ru_msgrcv = 0
        self.ru_nsignals = 0
        self.ru_nvcsw = 0
        self.ru_nivcsw = 0


@extern("getrusage")
def _getrusage(
    who: Int32, usage: UnsafePointer[RUsage, MutAnyOrigin]
) abi("C") -> Int32: ...


def maxrss_bytes() raises -> Int:
    """Peak resident set of THIS process (bytes, darwin RUSAGE_SELF)."""
    var u = RUsage()
    var rc = _getrusage(0, UnsafePointer[RUsage, MutAnyOrigin](to=u))
    if rc != 0:
        raise Error("t15 stack cache stress: getrusage failed")
    return Int(u.ru_maxrss)


comptime CYCLES = Int(100000)
comptime CAP = Int(4)


def main() raises:
    print("T15 stack cache stress: 100k acquire/release churn over capacity-"
          + String(CAP) + " pool")
    var rss_start = maxrss_bytes()
    var pool = make_stack_cache(CAP, 65536)

    # Warm-up: acquire+release every slot so the free set is full and the
    # committed bytes plateau at capacity * reservation.
    var warmup = List[UnsafePointer[NativeStack, MutAnyOrigin]]()
    var wi = 0
    while wi < CAP:
        var s = pool.acquire()
        warmup.append(s)
        wi += 1
    wi = 0
    while wi < CAP:
        pool.release(warmup[wi])
        wi += 1
    var committed_warm = Int(ms_stack_total_size())
    if committed_warm <= 0:
        red("warm-up produced no committed bytes")

    # 100k churn: every iteration acquires (must hit the free set) then
    # returns the reservation.  A leak that stops recycling released cells
    # would eventually exhaust the capacity and raise here.
    var i = 0
    while i < CYCLES:
        var s1 = pool.acquire()
        pool.release(s1)
        i += 1

    var committed_end = Int(ms_stack_total_size())
    if committed_end != committed_warm:
        red(
            "committed bytes drifted across 100k cycles: "
            + String(committed_warm) + " -> " + String(committed_end)
        )
    if pool.live() != 0 or pool.cached() != CAP:
        red("pool state after churn not bounded (live=" + String(pool.live())
            + " cached=" + String(pool.cached()) + ")")

    var rss_end = maxrss_bytes()
    # Peak RSS may legitimately include malloc arena growth, but a 100k-slot
    # stack leak would exceed any realistic slack.  Bound the growth.
    var rss_growth = rss_end - rss_start
    if rss_growth > 128 * 1024 * 1024:
        red(
            "peak RSS grew >128 MiB across the churn (stack leak): "
            + String(rss_growth) + " bytes"
        )

    print("T15 stack cache stress: committed", committed_end,
          "bytes; peak RSS +", rss_growth, "bytes")
    print("T15 stack cache stress: PASS")
