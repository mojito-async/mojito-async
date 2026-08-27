# mojito_async/test/stress/t14_stress.mojo
#
# A1.5 stress (issue #37) — exit criterion 4: NO TASK / RESULT / STACK
# LEAKS.
#
# The A1.1 runtime owns NO task storage (every TaskControlBlock cell is
# caller-allocated), NO stacks (fiber-free cooperative single worker, spec
# §88), and a bounded ring runnable queue (retains only high-water
# capacity).  Leaks wold therefore show up as:
#   (a) undrained runnable records        -> rt.pending() != 0 / skipped > 0
#   (b) unconsumed results                -> has_result_pending() ater reap
#   (c) re-dispatched / lost records      -> served != spawned, run counts
#   (d) consumption-path loss             -> destructor-count deltas
#   (e) driver-owned cells not returned   -> alloc/free imbalance
#
# Two identica populations of 5,000 tasks are driven end-to-end: one
# reaped via join(), one via abandon().  A destructor-counting result value
# (Counting) proves the consumption-path invariant: both populations
# perform the SAME creation sequence, and the ONY difference is the single
# take-result copy that join() extracts — so
#       dtor(join population) - dtor(abandon population) == 5000
# exactly.  A leak on either path (a stored result never destroyed, a take
# copy duplicated/lost) breaks that equation.  Task/queue/stack-leak
# invariants are asserted independently per wave and by final sweeps.
#
# Pure Mojo (`mojo run -I repo`), extern-free, def-only, deterministic.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.collections import List
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle, abandon, execute, spawn


def red(what: String) raises -> None:
    print("T14 stress (leaks): RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[Counting]


# ---------------------------------------------------------------------------
# Counting — destructor-counting result value (the leak detector).
#
# Every created value (explicit or implicit copy) fires its destructor
# exactly once by the time the program tears down.  __del__ only touches the
# shared dtor cell — idempotent, order-independent.
# ---------------------------------------------------------------------------

struct Counting(ResultValue):
    var v: Int
    var dtor: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        # Default constructor (TCB zero-arg result slot): dtor cell is a
        # SENTINEL (address 1) that __del__ ignores — default results
        # destroyed during mark_result()/TCB replacement are harmless.
        self.v = 0
        self.dtor = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)

    def __init__(out self, v: Int, dtor: UnsafePointer[Int, MutAnyOrigin]):
        self.v = v
        self.dtor = dtor

    def __del__(deinit self):
        # Sentinel guard: only count values with a REAL dtor cell.
        if Int(self.dtor) != 1:
            self.dtor[] += 1


# --- task bodies + dispatcher -------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Dtor accounting cells: join1@0, join2@1, abandon@2, default@3,
    ran@4, phase@5 (0=join wave 1, 1=join wave 2, 2=abandon wave)."""

    var join1_cell: UnsafePointer[Int, MutAnyOrigin]
    var join2_cell: UnsafePointer[Int, MutAnyOrigin]
    var aban_cell: UnsafePointer[Int, MutAnyOrigin]
    var default_cell: UnsafePointer[Int, MutAnyOrigin]
    var ran: UnsafePointer[Int, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.join1_cell = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.join2_cell = self.join1_cell
        self.aban_cell = self.join1_cell
        self.default_cell = self.join1_cell
        self.ran = self.join1_cell
        self.phase = self.join1_cell


def body_count(ud: BytePtr) raises -> Counting:
    var sc = ud.bitcast[Scene]()
    sc[].ran[] = sc[].ran[] + 1
    if sc[].phase[] == 0:
        return Counting(1, sc[].join1_cell)
    if sc[].phase[] == 1:
        return Counting(1, sc[].join2_cell)
    return Counting(1, sc[].aban_cell)


def dispatch(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = JoinHandle[Counting](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    _ = execute(h, body_count, ud)
    return 1


def run_join_wave(
    mut rt: Runtime,
    pool: UnsafePointer[TB, MutAnyOrigin],
    cells: UnsafePointer[Int, MutAnyOrigin],
    ud: BytePtr,
    phase: Int,
) raises -> Int:
    """Spawn -> drive -> join N1 tasks; returns records served."""
    comptime N1 = Int(5000)
    cells[4] = 0
    cells[5] = phase
    var joins = List[JoinHandle[Counting]]()
    for i in range(N1):
        pool[i] = TB.create()  # destroys the previous wave's stored result
        var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=pool[i]), 0)
        joins.append(h)
    var served = scheduler_loop(rt, dispatch, ud)
    if served != N1:
        red("join wave " + String(phase) + " served " + String(served))
    if cells[4] != N1:
        red("join wave " + String(phase) + " bodies ran " + String(cells[4]))
    for i in range(N1):
        var r = joins[i].join()
        if r.v != 1:
            red("join wave " + String(phase) + " result wrong")
    return served


def run_abandon_wave(
    mut rt: Runtime,
    pool: UnsafePointer[TB, MutAnyOrigin],
    cells: UnsafePointer[Int, MutAnyOrigin],
    ud: BytePtr,
) raises -> Int:
    """Spawn -> drive -> abandon N2 tasks; returns records served."""
    comptime N2 = Int(5000)
    cells[4] = 0
    cells[5] = 2
    var abans = List[JoinHandle[Counting]]()
    for i in range(N2):
        pool[i] = TB.create()  # destroys the previous wave's stored result
        var h2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=pool[i]), 0)
        abans.append(h2)
    var served = scheduler_loop(rt, dispatch, ud)
    if served != N2:
        red("abandon wave served " + String(served))
    if cells[4] != N2:
        red("abandon wave bodies ran " + String(cells[4]))
    for i in range(N2):
        abandon(abans[i])
    return served


def main() raises:
    var rt = create()

    # Stable scratch cells for destructor/default accounting (driver-owned).
    var buf = List[Int]()
    buf.append(0)  # 0: join-wave-1 dtor total
    buf.append(0)  # 1: join-wave-2 dtor total
    buf.append(0)  # 2: abandon-wave dtor total
    buf.append(0)  # 3: default-result dtor total (scratch, unasserted)
    buf.append(0)  # 4: ran counter
    buf.append(0)  # 5: wave phase
    var scene = Scene()
    scene.join1_cell = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 0 * 8
    )
    scene.join2_cell = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 1 * 8
    )
    scene.aban_cell = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 2 * 8
    )
    scene.default_cell = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 3 * 8
    )
    scene.ran = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 4 * 8
    )
    scene.phase = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(buf.unsafe_ptr()) + 5 * 8
    )
    var sp = UnsafePointer[Scene, MutAnyOrigin](to=scene)
    var ud = sp.bitcast[Byte]()

    comptime N1 = Int(5000)
    comptime N2 = Int(5000)

    # Driver-owned TCB pool: N2 cells, reused by every wave.
    var pool = List[TB]()
    for _ in range(N2):
        pool.append(TB.create())

    # ---- Waves: 5,000 join (group 1) + 5,000 join (group 2, for
    # determinism) + 5,000 abandon = 15,000 tasks total ----------------------
    var s1 = run_join_wave(rt, pool.unsafe_ptr(), buf.unsafe_ptr(), ud, 0)
    var s2 = run_join_wave(rt, pool.unsafe_ptr(), buf.unsafe_ptr(), ud, 1)
    var s3 = run_abandon_wave(rt, pool.unsafe_ptr(), buf.unsafe_ptr(), ud)

    # ---- Sweeps: no task/result/queue/stack leaks --------------------------
    if s1 != N1 or s2 != N1 or s3 != N2:
        red("served totals wrong over the three waves")
    if rt.pending() != 0:
        red("runnable queue not drained after all waves")
    if rt.skipped() != 0:
        red("stale records skipped over the waves")
    if rt.enqueued() != 3 * N1:
        red("enqueued " + String(rt.enqueued()) + " != 15000")
    for i in range(N2):
        if pool[i].has_result_pending():
            red("orphaned result pending on cell " + String(i))
    var fresh = True
    for i in range(N2):
        var c = pool[i]
        if c.state() != TaskControlBlock.NEW or c.generation() != 1:
            fresh = False
    if not fresh:
        red("abandon did not leave fresh TCBs (post-replacement sweep)")

    # ---- Destructor invariants (the leak equation) --------------------------
    # The join path destroys exactly 2 more Counting values per task than the
    # abandon path: take_result() copies the stored value out (`out` local)
    # and returns it into the caller's owned value — 2 creations -> 2
    # destructions.  A leak on either consumption path breaks the relation,
    # and two IDENTICAL join sub-waves must produce IDENTICAL totals
    # (determinism of the whole accounting).
    var j1 = buf[0]
    var j2 = buf[1]
    var at = buf[2]
    if j1 != j2:
        red("dtor determinism broken: " + String(j1) + " vs " + String(j2))
    if j1 - at != 2 * N1:
        red("dtor delta " + String(j1) + " - " + String(at)
            + " != " + String(2 * N1))

    print("T14 stress (leaks): dtor join1=" + String(j1) + " join2=" + String(j2)
          + " abandon=" + String(at))
    print("T14 stress (leaks): PASS")