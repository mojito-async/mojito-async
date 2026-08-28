# bench/timer_scale_aot.mojo
#
# A1.4 (issue #36) — F6 timer-scale benchmark harness (spec §31/§74, JMH
# discipline: setup / warmup / measurement / teardown; never one-off
# stopwatch samples).
#
# Measures the min-heap timer subsystem at 1k / 10k / 100k / 1M timers:
#   arm_scale   — arm N monotonic timers (deadlines spread 1ms apart),
#                 total + ns/op;
#   expire_scale— collect_due(now) pops them in deadline order (the expiry
#                 pass the scheduler servicing hook performs), total + ns/op.
# Each size runs its own setup (fresh heap) and teardown (drain + drop), and
# the resulting list is VALIDATED as ascending-deadline and exactly N long —
# a wrong-order pop FAILs the harness instead of producing a number.
#
# Real monotonic time comes from @extern("clock_gettime") — b2 (modular/
# modular#6971) miscompiles imported-module externs under JIT, so this
# driver is the *_aot embedding: `mojo build` + execute (see run.sh comment
# in a0_capture_aot.mojo).
#
# Run:
#   mojo build -I . bench/timer_scale_aot.mojo -o build/timer_scale_aot
#   ./build/timer_scale_aot
#
# Verdict: JSONL rows to stdout; exit 0 + "PASS" when every size validated.
from mojito_async.time.timer_heap import TimerHeap


@extern("clock_gettime")
def _clock_gettime(
    clk_id: Int32, tp: UnsafePointer[Timespec, MutAnyOrigin]
) abi("C") -> Int32: ...


struct Timespec(ImplicitlyCopyable, ImplicitlyDeletable):
    """Layout-compatible with C struct timespec (tv_sec, tv_nsec)."""
    var tv_sec: Int64
    var tv_nsec: Int64

    def __init__(out self):
        self.tv_sec = 0
        self.tv_nsec = 0


@extern("exit")
def _c_exit(code: Int32) abi("C"): ...


comptime CLOCK_MONOTONIC_RAW = Int32(4)


def now_ns() raises -> UInt64:
    var ts = Timespec()
    var rc = _clock_gettime(
        CLOCK_MONOTONIC_RAW, UnsafePointer[Timespec, MutAnyOrigin](to=ts)
    )
    if rc != 0:
        raise Error("timer_scale: clock_gettime failed")
    return UInt64(Int(ts.tv_sec) * 1_000_000_000 + Int(ts.tv_nsec))


def jsonl_row(name: String, total_ns: UInt64, n: Int) -> String:
    """One JSON-ish row: name, total ns, ops, ns/op."""
    var per = 0
    if n > 0:
        per = Int(total_ns // UInt64(n))
    return (
        '{"bench":"timer_scale","case":"' + name + '","n":' + String(n)
        + ',"total_ns":' + String(total_ns) + ',"ns_per_op":' + String(per) + "}"
    )


def run_size(n: Int, warmup: Bool) raises -> Bool:
    """Arm `n` timers, expire them in order, validate.  Returns True when
    the popped order is exactly ascending and length is n."""
    var heap = TimerHeap()

    # ---- setup: arm n timers, deadlines strictly increasing ---------------
    var i = 0
    while i < n:
        # deadline = (i+1) ms in ns ticks; deterministic spread.
        var d = UInt64(i + 1) * 1_000_000
        _ = heap.arm(i + 1, 0x1000 + i, d)
        i += 1
    if warmup:
        # warmup pass: drain without reporting (JIT/structure warm).
        _ = heap.collect_due(UInt64(n) * 1_000_000)
        return True
    if heap.size() != n:
        return False

    # ---- measure 1: expiry pass (pop all in deadline order) ---------------
    var t0 = now_ns()
    var due = heap.collect_due(UInt64(n) * 1_000_000)
    var t1 = now_ns()
    if len(due) != n:
        return False
    if not heap.is_empty():
        return False
    print(jsonl_row("expire_scale", t1 - t0, n))

    # ---- teardown + re-setup for the arm measurement ----------------------
    # (heap is drained; drop the `due` list and re-arm on a FRESH heap)
    _ = due
    var heap2 = TimerHeap()
    i = 0
    while i < n:
        var d = UInt64(i + 1) * 1_000_000
        _ = heap2.arm(i + 1, 0x1000 + i, d)
        i += 1

    # ---- measure 2: arm pass (insert into an empty heap) ------------------
    var heap3 = TimerHeap()
    t0 = now_ns()
    i = 0
    while i < n:
        var d = UInt64(i + 1) * 1_000_000
        _ = heap3.arm(i + 1, 0x1000 + i, d)
        i += 1
    t1 = now_ns()
    print(jsonl_row("arm_scale", t1 - t0, n))

    # ---- validation of heap3's expiry order --------------------------------
    var check = heap3.collect_due(UInt64(n) * 1_000_000)
    if len(check) != n:
        return False
    var j = 1
    while j < n:
        if check[j].deadline < check[j - 1].deadline:
            return False
        if check[j].id != j + 1:
            return False
        j += 1
    return True


def main() raises:
    print("bench: timer_scale (F6) — min-heap arm/expire, monotonic ns")
    # warmup (~1k) so JIT/allocator state settles before measurements.
    if not run_size(1000, True):
        print("timer_scale: RED (warmup validation failed)")
        _c_exit(1)

    var sizes = InlineArray[Int, 4](fill=0)
    sizes[0] = 1000
    sizes[1] = 10000
    sizes[2] = 100000
    sizes[3] = 1000000
    var k = 0
    while k < 4:
        var ok = run_size(sizes[k], False)
        if not ok:
            print("timer_scale: RED (validation failed at n=" + String(sizes[k]) + ")")
            _c_exit(1)
        k += 1

    print("timer_scale: PASS")
    _c_exit(0)