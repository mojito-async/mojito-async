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
# A6.5 (issue #88) extends this driver (does not replace the above) with
# three more cases feeding the F6/Q11 heap-vs-wheel evidence:
#   deadline_sweep_scale — arm N timers CLUSTERED inside a deadline window
#                 (25% of ids in 10% of the window, spec §31/§117 Q11), then
#                 step `now` through the window in fixed increments calling
#                 collect_due(now) incrementally — the steady-state-vs-burst
#                 incremental expiry cost a wheel would replace; reports
#                 ns/op, peak heap length, step count, window span, and
#                 validates an exact ascending-order full drain.
#   cancel_scale  — times a bounded sample of cancel(id) / cancel_token(gen)
#                 calls (the control-plane O(n) rebuild) against a heap of N
#                 live timers; reports ns/op per mode and asserts no live
#                 generation is left behind for the cancelled ids.
#   service_pass_scale — exercises the REAL `service_timers` hook: N-8
#                 background timers (shared dummy TCB, never WAITING) plus 8
#                 really-spawned tasks parked via `sleep_current`, all due
#                 at once; only the `service_timers` call itself (arm+expire
#                 +resume together) is timed, validating exactly 8 wakes and
#                 a fully drained heap — the task-centric load, not just raw
#                 structure.
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
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, spawn
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Duration
from mojito_async.time.sleep import sleep_current
from mojito_async.time.timer_heap import TimerHeap
from mojito_async.time.timer_service import service_timers


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


def jsonl_row_ext(
    name: String, total_ns: UInt64, n: Int, extra: List[String]
) -> String:
    """`jsonl_row` plus caller-supplied extra `"key":value` fields (A6.5,
    issue #88) — the new cases report context (peak length, mode, step
    count) alongside the ns/op the old cases already emit."""
    var per = 0
    if n > 0:
        per = Int(total_ns // UInt64(n))
    var out = (
        '{"bench":"timer_scale","case":"' + name + '","n":' + String(n)
        + ',"total_ns":' + String(total_ns) + ',"ns_per_op":' + String(per)
    )
    for p in extra:
        out += "," + p
    out += "}"
    return out


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


comptime SWEEP_STEPS = Int(1000)  # fixed step count regardless of n (A6.5)


def deadline_sweep_scale(n: Int, warmup: Bool) raises -> Bool:
    """A6.5 (issue #88): arm `n` timers with deadlines CLUSTERED inside a
    window — 25% of ids packed into the first 10% of the window, the rest
    spread across the remainder (spec §31/§117 Q11 density scenario) — then
    step `now` through the window in `SWEEP_STEPS` fixed increments, calling
    `collect_due(now)` incrementally at each step (the steady-state pattern
    a real scheduler tick performs, vs one single burst pop).  Validates the
    incremental drain stays deadline-ascending across step boundaries and
    exactly drains all `n` by the end of the window."""
    var heap = TimerHeap()
    var window = UInt64(n) * 2
    var cluster_span = window // 10
    var cluster_n = n // 4

    # ---- setup: cluster_n ids packed into [1, cluster_span], the rest ------
    # spread across (cluster_span, window] — deterministic, no randomness.
    var i = 0
    while i < cluster_n:
        var d = UInt64(1) + (UInt64(i) * cluster_span) // UInt64(cluster_n + 1)
        _ = heap.arm(i + 1, 0x5000 + i, d)
        i += 1
    var rest_n = n - cluster_n
    var rest_span = window - cluster_span
    while i < n:
        var off = UInt64(i - cluster_n)
        var d = cluster_span + UInt64(1) + (off * rest_span) // UInt64(rest_n + 1)
        _ = heap.arm(i + 1, 0x5000 + i, d)
        i += 1
    var peak_len = heap.size()  # occupancy right after the full arm, pre-drain

    var step = window // UInt64(SWEEP_STEPS) + 1
    if warmup:
        var now = UInt64(0)
        while now < window + step:
            _ = heap.collect_due(now)
            now += step
        return True
    if heap.size() != n:
        return False

    # ---- measure: incremental collect_due across the stepped window -------
    var now = UInt64(0)
    var drained = 0
    var last_deadline = UInt64(0)
    var t0 = now_ns()
    while now < window + step:
        var due = heap.collect_due(now)
        var j = 0
        while j < len(due):
            if due[j].deadline < last_deadline:
                return False
            last_deadline = due[j].deadline
            j += 1
        drained += len(due)
        now += step
    var t1 = now_ns()

    if drained != n:
        return False
    if not heap.is_empty():
        return False
    var extra = List[String]()
    extra.append('"peak_len":' + String(peak_len))
    extra.append('"steps":' + String(SWEEP_STEPS))
    extra.append('"window_ns":' + String(window))
    print(jsonl_row_ext("deadline_sweep_scale", t1 - t0, n, extra))
    return True


comptime CANCEL_SAMPLE = Int(50)  # bounded sample (each cancel() is an O(n)
# control-plane rebuild — sampling keeps the 1M-scale case tractable while
# still reporting the true per-call ns/op at that heap size).


def cancel_scale(n: Int, warmup: Bool) raises -> Bool:
    """A6.5 (issue #88): arm `n` live timers, then time a bounded sample of
    single-call `cancel(id)` (whole-id) and `cancel_token(id, gen)` (exact
    generation) removals against the live heap of size `n`.  Reports ns/op
    per mode and asserts no live generation remains registered for any
    cancelled id."""
    var heap = TimerHeap()
    var i = 0
    while i < n:
        var d = UInt64(i + 1) * 1_000_000
        _ = heap.arm(i + 1, 0x6000 + i, d)
        i += 1

    var k = CANCEL_SAMPLE
    if k > n:
        k = n

    if warmup:
        _ = heap.cancel(1)
        return True
    if heap.size() != n:
        return False

    # ---- measure: whole-id cancel(id), k samples off the live heap --------
    var c = 0
    var t0 = now_ns()
    while c < k:
        if not heap.cancel(c + 1):
            return False
        c += 1
    var t1 = now_ns()
    if heap.size() != n - k:
        return False
    c = 0
    while c < k:
        if heap.live_gen(c + 1) != 0:
            return False
        c += 1
    var extra1 = List[String]()
    extra1.append('"mode":"cancel"')
    extra1.append('"heap_n":' + String(n))
    print(jsonl_row_ext("cancel_scale", t1 - t0, k, extra1))

    # ---- measure: exact (id, gen) cancel_token, k FRESH re-arms -----------
    var gens = List[Int]()
    c = 0
    while c < k:
        var d = UInt64(n + c + 1) * 1_000_000
        var g = heap.arm(n + c + 1, 0x7000 + c, d)
        gens.append(g)
        c += 1
    var t2 = now_ns()
    c = 0
    while c < k:
        if not heap.cancel_token(n + c + 1, gens[c]):
            return False
        c += 1
    var t3 = now_ns()
    if heap.size() != n - k:
        return False
    c = 0
    while c < k:
        if heap.live_gen(n + c + 1) != 0:
            return False
        c += 1
    var extra2 = List[String]()
    extra2.append('"mode":"cancel_token"')
    extra2.append('"heap_n":' + String(n))
    print(jsonl_row_ext("cancel_scale", t3 - t2, k, extra2))
    return True


comptime SP_TASKS = Int(8)  # "a few ready tasks" (issue #88 step 5)
comptime SP_TB = TaskControlBlock[IntResult]


struct SPScene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared scratch for `service_pass_scale`'s dispatcher: the caller-owned
    heap + virtual clock, and a stagger counter so each of the SP_TASKS
    spawned tasks parks on a distinct deadline."""

    var heap: UnsafePointer[TimerHeap, MutAnyOrigin]
    var clock: MonotonicClock
    var next_idx: UnsafePointer[Int, MutAnyOrigin]

    def __init__(
        out self,
        heap: UnsafePointer[TimerHeap, MutAnyOrigin],
        clock: MonotonicClock,
        next_idx: UnsafePointer[Int, MutAnyOrigin],
    ):
        self.heap = heap
        self.clock = clock
        self.next_idx = next_idx


def sp_dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Each spawned task's first (only) entry: claim RUNNING, then park via
    the real `sleep_current` on a distinct staggered deadline."""
    var sc = ud.bitcast[SPScene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[SP_TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var idx = sc[].next_idx[]
    sc[].next_idx[] = idx + 1
    sleep_current(rt, h, sc[].heap[], sc[].clock, Duration(UInt64(idx + 1)))
    return 1


def service_pass_scale(n: Int, warmup: Bool) raises -> Bool:
    """A6.5 (issue #88): exercise the REAL `service_timers` hook over a
    task-centric load — `n - SP_TASKS` background timers sharing one dummy
    (never-WAITING) TCB address, plus SP_TASKS really-spawned tasks parked
    via `sleep_current`, all due at once.  Only the `service_timers` call
    itself is timed (arm+expire+resume together, issue #88 step 5); the
    setup (arming, spawning, parking) stays outside the measured window."""
    if n <= SP_TASKS:
        return True  # below the smallest swept size in practice

    var rt = create()
    var heap = TimerHeap()
    var dummy = SP_TB()
    var dummy_addr = Int(UnsafePointer[SP_TB, MutAnyOrigin](to=dummy))

    var clock_cell = stack_allocation[1, UInt64]()
    clock_cell[0] = 0
    var clock = MonotonicClock(UnsafePointer[UInt64, MutAnyOrigin](to=clock_cell[0]))

    # ---- background noise: n - SP_TASKS timers, never WAITING -------------
    var bg = n - SP_TASKS
    var i = 0
    while i < bg:
        _ = heap.arm(9_000_000 + i, dummy_addr, UInt64(i + 1))
        i += 1

    # ---- SP_TASKS real spawned tasks, parked via the real sleep_current ---
    var tcbs = stack_allocation[SP_TASKS, SP_TB]()
    i = 0
    while i < SP_TASKS:
        tcbs[i] = SP_TB()
        i += 1
    var idx_cell = stack_allocation[1, Int]()
    idx_cell[0] = 0
    var sc = SPScene(
        UnsafePointer[TimerHeap, MutAnyOrigin](to=heap), clock, idx_cell
    )
    var scp = UnsafePointer[SPScene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    i = 0
    while i < SP_TASKS:
        _ = spawn(rt, tcbs + i, 0)
        i += 1
    var served = scheduler_loop(rt, sp_dispatch, ud)
    if served != SP_TASKS:
        return False
    if heap.size() != n:
        return False
    if rt.pending() != 0:
        return False

    # ---- advance the virtual clock past every deadline (bg + real) --------
    clock.set(UInt64(bg) + UInt64(SP_TASKS) + 1)

    if warmup:
        _ = service_timers[IntResult](rt, heap, clock.now())
        return True

    # ---- measure: the service pass itself ----------------------------------
    var t0 = now_ns()
    var woke = service_timers[IntResult](rt, heap, clock.now())
    var t1 = now_ns()

    if woke != SP_TASKS:
        return False
    if not heap.is_empty():
        return False
    if rt.pending() != SP_TASKS:
        return False
    i = 0
    while i < SP_TASKS:
        if (tcbs + i)[].state() != TaskControlBlock.RUNNABLE:
            return False
        i += 1

    var extra = List[String]()
    extra.append('"real_tasks":' + String(SP_TASKS))
    extra.append('"woke":' + String(woke))
    print(jsonl_row_ext("service_pass_scale", t1 - t0, n, extra))
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

    # A6.5 (issue #88) — deadline sweep, cancel, and service-pass cases.
    if not deadline_sweep_scale(1000, True):
        print("timer_scale: RED (deadline_sweep_scale warmup validation failed)")
        _c_exit(1)
    k = 0
    while k < 4:
        if not deadline_sweep_scale(sizes[k], False):
            print(
                "timer_scale: RED (deadline_sweep_scale validation failed at n="
                + String(sizes[k]) + ")"
            )
            _c_exit(1)
        k += 1

    if not cancel_scale(1000, True):
        print("timer_scale: RED (cancel_scale warmup validation failed)")
        _c_exit(1)
    k = 0
    while k < 4:
        if not cancel_scale(sizes[k], False):
            print(
                "timer_scale: RED (cancel_scale validation failed at n="
                + String(sizes[k]) + ")"
            )
            _c_exit(1)
        k += 1

    if not service_pass_scale(1000, True):
        print("timer_scale: RED (service_pass_scale warmup validation failed)")
        _c_exit(1)
    k = 0
    while k < 4:
        if not service_pass_scale(sizes[k], False):
            print(
                "timer_scale: RED (service_pass_scale validation failed at n="
                + String(sizes[k]) + ")"
            )
            _c_exit(1)
        k += 1

    print("timer_scale: PASS")
    _c_exit(0)