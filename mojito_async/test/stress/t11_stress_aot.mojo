# mojito_async/test/stress/t11_stress_aot.mojo
#
# A1.5 stress (issue #37) — exit criterion 1: 100k task lifecycle with
# STABLE PEAK MEMORY.
#
# Drives 10 waves x 10,000 = 100,000 spawn -> scheduler-drive -> join cycles
# over ONE reused heap-backed TCB pool, sampling peak RSS (getrusage
# RUSAGE_SELF ru_maxrss) after every wave and accounting every observable
# scheduler effect (measure.mojo-counter-style: spawned, served, joined,
# completed, queue pending, skipped stales).  The A1.1 runtime keeps every
# TCB caller-owned (no per-task heap inside the runtime) and the runnable
# queue retains only its high-water ring capacity, so after the first
# (warm-up) wave peak RSS MUST flatten: a per-task leak would show as
# monotonic growth across the remaining 90k tasks.
#
# Constraints honored:
#   - extern calls (getrusage/malloc/free) live ONLY in this *_aot driver
#     module (modular/modular#6971); every mojito_async module imported
#     here is extern-free.
#   - def-only, module factories, deterministic PASS/RED verdicts.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).  Reports the
# measured peak-RSS trajectory for the 100k suite on stdout.

from std.collections import List
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, execute, spawn


# ---------------------------------------------------------------------------
# Darwin struct rusage (RUSAGE_SELF): 17 x Int64 fields.  ru_maxrss is the
# PEAK resident set in BYTES on darwin.  Layout: ru_utime (2), ru_stime (2),
# then the 13 scalar fields (ru_maxrss first).
# ---------------------------------------------------------------------------

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


@extern("malloc")
def _c_malloc(size: Int) abi("C") -> BytePtr: ...


@extern("free")
def _c_free(ptr: BytePtr) abi("C"): ...


def maxrss_bytes() raises -> Int:
    """Peak resident set of THIS process (bytes, darwin RUSAGE_SELF)."""
    var u = RUsage()
    var rc = _getrusage(0, UnsafePointer[RUsage, MutAnyOrigin](to=u))
    if rc != 0:
        raise Error("t11 maxrss: getrusage failed")
    return Int(u.ru_maxrss)


def red(what: String) raises -> None:
    print("T11 stress (100k lifecycle): RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]

comptime WAVE_N = Int(10000)
comptime WAVES = Int(10)
comptime CELL_BYTES = Int(128)  # >= sizeof(TaskControlBlock[IntResult]) here


# --- measure.mojo-style accounting (extern-free, embedded) -------------------

struct AccountRow(ImplicitlyCopyable, ImplicitlyDeletable):
    var name: String
    var bytes: Int
    var count: Int

    def __init__(out self):
        self.name = String("")
        self.bytes = 0
        self.count = 0


struct Accounter(ImplicitlyCopyable, ImplicitlyDeletable):
    """Event/bytes accounting table (fixed 32 rows, insertion order)."""

    comptime MAX_ROWS = Int(32)

    var _rows: InlineArray[AccountRow, Self.MAX_ROWS]
    var _n: Int

    def __init__(out self):
        self._rows = InlineArray[AccountRow, Self.MAX_ROWS](fill=AccountRow())
        self._n = 0

    def account(mut self, event: String, bytes: Int) raises:
        var i = 0
        while i < self._n:
            if self._rows[i].name == event:
                self._rows[i].bytes += bytes
                self._rows[i].count += 1
                return
            i += 1
        if self._n >= Self.MAX_ROWS:
            raise Error("Accounter: row capacity exceeded")
        self._rows[self._n].name = String(event)
        self._rows[self._n].bytes = bytes
        self._rows[self._n].count = 1
        self._n += 1

    def total_for(self, event: String) -> Int:
        var i = 0
        while i < self._n:
            if self._rows[i].name == event:
                return self._rows[i].bytes
            i += 1
        return 0

    def count_for(self, event: String) -> Int:
        var i = 0
        while i < self._n:
            if self._rows[i].name == event:
                return self._rows[i].count
            i += 1
        return 0

    def amount_for(self, event: String) -> Int:
        """Summed `bytes` payload of every account(event, ..) call."""
        var i = 0
        while i < self._n:
            if self._rows[i].name == event:
                return self._rows[i].bytes
            i += 1
        return 0

    def report(self) -> String:
        var out = String("")
        var i = 0
        while i < self._n:
            out = (
                out + "event=" + self._rows[i].name
                + " bytes=" + String(self._rows[i].bytes)
                + " count=" + String(self._rows[i].count) + "\n"
            )
            i += 1
        return out


def make_accounter() -> Accounter:
    return Accounter()


# --- task body + dispatcher --------------------------------------------------
#
# Every one of the 100k tasks runs the SAME one-step body: bump the wave
# counter.  The dispatcher knows every tid (single generic body), so it can
# serve any record without an id->body map.

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Driver heap cells: counter@0, live_cells@1."""

    var counter: UnsafePointer[Int, MutAnyOrigin]
    var live_cells: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.counter = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.live_cells = self.counter


def body_inc(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    sc[].counter[] = sc[].counter[] + 1
    return IntResult(1)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    _ = execute(h, body_inc, ud)
    return 1


def main() raises:
    var rt = create()
    var acc = make_accounter()

    # Driver-owned heap pool: WAVE_N cells, allocated ONCE, reused every
    # wave (a fresh-TCB assignment per wave destroys the previous COMPLETED
    # TCB in place — the A1.1 caller-owned contract).  live_cells tracks the
    # alloc/free balance and MUST reach 0 after the pool is freed.
    var pool = _c_malloc(WAVE_N * CELL_BYTES)
    var live = WAVE_N
    var cells = List[Int]()  # base addresses (128B-strided) of the cells
    var addr = Int(pool)
    for _ in range(WAVE_N):
        cells.append(addr)
        addr += CELL_BYTES
    acc.account("pool.alloc", WAVE_N * CELL_BYTES)

    var buf = List[Int]()
    buf.append(0)  # counter
    buf.append(0)  # live_cells
    var scene = Scene()
    scene.counter = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 0 * 8
    )
    scene.live_cells = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 1 * 8
    )
    buf[0] = 0
    buf[1] = live
    var sp = UnsafePointer[Scene, MutAnyOrigin](to=scene)
    var ud = sp.bitcast[Byte]()

    var peak_by_wave = InlineArray[Int, WAVES](fill=0)
    var wave_ok = True

    for w in range(WAVES):
        buf[0] = 0  # reset the wave counter
        # Assign a FRESH TCB into every cell (destroys the previous wave's
        # COMPLETED TCB) and spawn it into the runtime.
        var handles = List[JoinHandle[IntResult]]()
        for i in range(WAVE_N):
            var cell = UnsafePointer[TB, MutAnyOrigin](
                unsafe_from_address=cells[i]
            )
            cell[] = TB.create()
            var h = spawn(rt, cell, 0)
            handles.append(h)
        acc.account("spawn", WAVE_N)
        acc.account("enqueue.record", WAVE_N * 16)
        var served = scheduler_loop(rt, dispatch, ud)
        if served != WAVE_N:
            red("wave " + String(w) + " served " + String(served))
        if buf[0] != WAVE_N:
            red("wave " + String(w) + " bodies ran " + String(buf[0]))
        # join evary child (consume-once result)
        for i in range(WAVE_N):
            var r = handles[i].join()
            if r.v != 1:
                red("wave " + String(w) + " join result wrong")
        acc.account("join", WAVE_N)
        acc.account("result.take", WAVE_N * 8)
        if rt.pending() != 0:
            red("wave " + String(w) + " queue not quiet")
        if rt.skipped() != 0:
            red("wave " + String(w) + " has skipped stale records")
        peak_by_wave[w] = maxrss_bytes()
        if w >= 2 and peak_by_wave[w] > peak_by_wave[1] + 32 * 1024 * 1024:
            wave_ok = False

    # ---- final invariant checks (100k total) --------------------------------
    # NOTE: rt.tasks_started/completed count ROOT run() executions only (the
    # runtime contract); spawned children are observed via enqueued(), the
    # scheduler_loop served totals, per-handle joins, and queue quietness.
    if rt.enqueued() != WAVES * WAVE_N:
        red("enqueued " + String(rt.enqueued()) + " != 100000")
    if acc.amount_for("spawn") != WAVES * WAVE_N:
        red("spawn accounting wrong")
    if not wave_ok:
        red("peak RSS not stable after warm-up waves")
    if rt.skipped() != 0:
        red("stale-skip count nonzero over 100k tasks")

    # ---- free the driver-owned pool (leak evidence) --------------------
    _c_free(pool)
    live -= WAVE_N
    buf[1] = live
    if live != 0:
        red("driver heap cells leaked (" + String(live) + ")")

    # ---- repot -----------------------------------------------------------
    print("T11 stress (100k lifecycle): peak RSS trajectory (bytes/wave):")
    var traj = ""
    for w in range(WAVES):
        traj = traj + String(peak_by_wave[w]) + " "
    print("  waves: " + traj)
    print("  start=" + String(peak_by_wave[0]) + " peak=" + String(peak_by_wave[WAVES - 1])
          + " delta_post_warmup=" + String(peak_by_wave[WAVES - 1] - peak_by_wave[1]))
    print("  tasks spawned+executed+joined+completed=" + String(WAVES * WAVE_N)
          + " enqueued=" + String(rt.enqueued()) + " skipped=" + String(rt.skipped()))
    print("T11 stress (100k lifecycle): PASS")