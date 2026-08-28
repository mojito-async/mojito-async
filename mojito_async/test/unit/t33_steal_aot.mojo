# mojito_async/test/unit/t33_steal_aot.mojo
#
# A2.4 (issue #70) — unstarted-task stealing acceptance (TDD driver).
#
# TDD acceptance (three scenarios, two REAL worker threads each):
#
#   1. STEAL-SHARE (busy vs idle): worker 0's local deque is seeded with 64
#      small never-run tasks and a LOCAL BUDGET of 8 (it serves at most 8
#      from its own FIFO head, then goes idle-harvesting); idle worker 1
#      probes worker 0 with try_steal_unstarted (spec §21 order: local ->
#      remote -> inject -> STEAL) and steals the remaining 56 from the
#      OPPOSITE end.  Every task runs EXACTLY ONCE (no loss, no duplicate),
#      worker 1's observed steals == the runtime task_steals_total counter
#      (exact — spec §71), and the steal share is nonzero.
#
#   2. MIGRATION-FORBIDDEN (spec §19.1/§19.2, ADR-006): worker 1's deque
#      holds 11 fresh tasks plus ONE STARTED task — a task whose TCB was
#      already walked RUNNING -> PARKING -> RUNNABLE (the yield/re-enqueue
#      shape of a started task, STARTED latched) — seeded at the TAIL.
#      Worker 0 (idle) steals the tail: the STARTED record is popped under
#      the owner deque's guard, its TCB is not pre-start, so it is RETURNED
#      to the owner's deque (re-runs there — nothing lost) and the probe
#      fails: the STARTED task is NEVER observed executing on worker 0, the
#      owner runs it exactly once, and the task_steals_total counter reports
#      ZERO started-fiber steals.
#
#   3. EMPTY PEERS -> E6 HANDOFF: no work anywhere; each worker completes a
#      CAPped probe round (min_idle rounds), finds nothing, and parks —
#      the driver's stand-in for the E6 park_os_thread_until_event (the
#      banner in scheduler.mojo) — instead of spinning forever; counters
#      stay 0 (no fake counters on failed probes).
#
# EXTERN DISCIPLINE (modular/modular#6971): the driver declares pthread
# externs at concrete driver scope and imports the vendor firewall for
# c_malloc/c_free/entry_pointer, so it MUST be AOT (`mojo build` +
# execute; *_aot.mojo pattern) exactly like t23/t25/t26.  The 2-worker
# recipe is the lane-standard minimal 2-thread harness (pthread externs at
# concrete driver scope; the vendor thread bindings (#67) are not in this
# base).
#
# Race/teardown notes (b2):
#   - a foreign (pthread) thread that RETURNS crashes the runtime, so
#     worker bodies never return — after parking they tail-spin until main
#     _iso_exits the process;
#   - the compiler hoists plain reads of cross-thread state in wait loops,
#     so the quiesce wait reads the phase cells through address-dependent
#     indices (never loop-invariant), and the workers use a release fence
#     before parking (main: acquire fence) for write visibility.
#
# Verdict: exit 0 + "PASS"; any failure prints FAIL + _iso_exit(1).
from std.atomic import Atomic, fence, Ordering
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.vendor.mojito_sys import c_free, c_malloc, entry_pointer
from mojito_async.runtime.queue import TaskRecord
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker
from mojito_async.task import JoinHandle, claim_running


comptime TB = TaskControlBlock[IntResult]
# Per-TCB heap stride (generous; sizeof is not available in b2 — the cells
# are only ever addressed individually via tcb_addrs).
comptime TCB_STRIDE = Int(128)


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Int,
    start_routine: UnsafePointer[Byte, MutAnyOrigin],
    arg: BytePtr,
) abi("C") -> Int32: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


# ---------------------------------------------------------------------------
# Failure ledger (b2: List[String] cannot nest in pointer types; the ledger
# is a count cell + immediate prints, t17-style)
# ---------------------------------------------------------------------------

def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


# ---------------------------------------------------------------------------
# Shared scene (heap-backed; threads read/write through pointers)
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    """Per-scenario shared state.  All mutable cells are heap blocks reached
    through pointers (t26/t27 pattern; never stack-carved escapees)."""

    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var n_tasks: Int            # task ids are 1..n_tasks
    var local_budget: Int       # worker 0 serves at most this many LOCAL tasks
    var min_idle: Int           # CAPped probe rounds before a worker parks
    var started_task: Int       # tid of the pre-started (migration) task; 0 = none
    var owner_of_started: Int   # worker index that MUST run the started task
    var pre_steals_w0: Int      # task_steals_total snapshots taken pre-scenario
    var pre_steals_w1: Int
    var observed_steals: Int    # driver-side count of successful try_steal calls
    var tcb_cells: Int          # heap base of the TCB cell block
    var tcb_addrs: UnsafePointer[Int, MutUntrackedOrigin]
    var run_counts: UnsafePointer[Int, MutUntrackedOrigin]
    var executor: UnsafePointer[Int, MutUntrackedOrigin]
    var phases: UnsafePointer[Int, MutUntrackedOrigin]      # 0 run, 1 parked, 2 err
    var thread_error: UnsafePointer[Int, MutUntrackedOrigin]
    # Atomic heartbeat: an atomic store (never deleted, a compiler barrier)
    # per serve-loop iteration — it forces the loop to re-read the peer's
    # deque state every round instead of caching it across inlined calls
    # (b2 -O2 miscompiles cross-thread deque reads without such a barrier).
    var progress: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.n_tasks = 0
        self.local_budget = 0
        self.min_idle = 1024
        self.started_task = 0
        self.owner_of_started = 0
        self.pre_steals_w0 = 0
        self.pre_steals_w1 = 0
        self.observed_steals = 0
        self.tcb_cells = 1
        self.tcb_addrs = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.run_counts = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.executor = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.phases = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.thread_error = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](unsafe_from_address=1)


def _worker_of(
    sc: UnsafePointer[Scene, MutAnyOrigin], me: Int
) -> UnsafePointer[Worker, MutAnyOrigin]:
    if me == 0:
        return sc[].w0
    return sc[].w1


def _all_run_once(sc: UnsafePointer[Scene, MutAnyOrigin]) -> Bool:
    """True when every task id 1..n_tasks has run EXACTLY once."""
    for tid in range(1, sc[].n_tasks + 1):
        if sc[].run_counts[tid] != 1:
            return False
    return True


def _park_forever(sc: UnsafePointer[Scene, MutAnyOrigin], me: Int) -> None:
    """E6 handoff surrogate: the real scheduler parks the OS thread until an
    event (park_os_thread_until_event; the banner in scheduler.mojo).  A
    foreign thread that RETURNS crashes the b2 runtime (thread-teardown
    bug), so the driver's parked worker spins until main _iso_exits the
    process microseconds later.  The spin is OBSERVABLE (progress write per
    iteration; the stop condition is never set): b2 -O2 deletes an empty
    `while True: pass`, which would let the thread fall through to its
    return and abort the runtime — the phase/flush bookkeeping, not the
    spin, is the observable acceptance."""
    while sc[].phases[me] == 1:
        _ = sc[].progress[me].fetch_add[ordering=Ordering.RELAXED](Int64(1))


def run_record(sc: UnsafePointer[Scene, MutAnyOrigin], tcb_addr: Int, tid: Int, me: Int) raises:
    """Dispatcher slice for one record: claim RUNNABLE -> RUNNING (the body
    entry — latches STARTED), run the body (the observable counters), then
    COMPLETED with the task id as its result."""
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    sc[].run_counts[tid] = sc[].run_counts[tid] + 1
    sc[].executor[tid] = me
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(tid))


def _serve(sc: UnsafePointer[Scene, MutAnyOrigin], me: Int) raises:
    """The §21 worker loop (local -> remote -> inject -> STEAL -> E6 sleep),
    as far as this base has sources: the E4 probe sits at the exact §21
    position (remote/inject are E5/E3 lanes — not in this base)."""
    var wp = _worker_of(sc, me)
    var served_local = 0
    var idle_rounds = 0
    while True:
        _ = sc[].progress[me].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        # 1) local deque (owner FIFO head).  The local budget caps ONLY
        #    worker 0 (it serves at most BUDGET local tasks, leaving the
        #    tail for the thief); worker 1 always serves its own locals.
        if me == 1 or served_local < sc[].local_budget:
            var rec = wp[].pop_local()
            if rec:
                var r = rec.value()
                run_record(sc, r.tcb_addr, r.task_id, me)
                served_local += 1
                idle_rounds = 0
                continue
        # 2/3) remote-ready (E5) and injection (E3) — not in this base
        # 4) STEAL probe (E4, issue #70)
        var stolen = wp[].try_steal_unstarted[IntResult]()
        if stolen:
            var r = stolen.value()
            run_record(sc, r.tcb_addr, r.task_id, me)
            sc[].observed_steals = sc[].observed_steals + 1
            idle_rounds = 0
            continue
        # 5) nothing local/remote/inject/steal: E6 sleep handoff (issue
        #    #70 step 3/4) — the worker stops probing after the CAPped round
        #    and parks instead of spinning on empty deques.  A real E6 park
        #    would block the OS thread until an event; the phase-1 park here
        #    is the driver's stand-in (scheduler.mojo banner).
        #
        #    Starvation guard: a BUDGETED owner (worker 0) that has served
        #    its local budget parks at its FIRST idle round — it must stop
        #    tight-spinning its deque lock or the test-and-set lock starves
        #    the thief (b2 -O2 test-and-set is unfair under asymmetric
        #    re-arming).  The thief (worker 1) parks once every task ran.
        idle_rounds += 1
        var park_after = sc[].min_idle
        if me == 0 and served_local >= sc[].local_budget:
            park_after = 1
        var done = False
        if me == 1:
            done = _all_run_once(sc)
        if idle_rounds >= park_after and (me == 0 or done):
            # Release fence: all writes this worker made before parking are
            # visible to main once it observes the phase (acquire side).
            fence[Ordering.RELEASE]()
            sc[].phases[me] = 1
            return


@export("t33_worker0")
def t33_worker0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        _serve(sc, 0)
    except e:
        sc[].thread_error[0] = 1
        sc[].phases[0] = 2
    _park_forever(sc, 0)


@export("t33_worker1")
def t33_worker1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        _serve(sc, 1)
    except e:
        sc[].thread_error[1] = 1
        sc[].phases[1] = 2
    _park_forever(sc, 1)


# ---------------------------------------------------------------------------
# Scenario runner + verification
# ---------------------------------------------------------------------------

def run_scenario(
    failures: UnsafePointer[Int, MutAnyOrigin],
    sc: UnsafePointer[Scene, MutAnyOrigin],
) raises:
    """Spawn the two worker threads over a fully-seeded scene, wait for both
    to quiesce (phase == 1), and verify the acceptance invariants."""
    var t0 = Int(0)
    var t1 = Int(0)
    var t0p = UnsafePointer[Int, MutAnyOrigin](to=t0)
    var t1p = UnsafePointer[Int, MutAnyOrigin](to=t1)
    var arg = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(sc))
    var rc0 = _pthread_create(t0p, 0, entry_pointer["t33_worker0"](), arg)
    var rc1 = _pthread_create(t1p, 0, entry_pointer["t33_worker1"](), arg)
    if rc0 != 0 or rc1 != 0:
        _fail(failures, "pthread_create failed (" + String(rc0) + ", "
                        + String(rc1) + ")")
        return
    var since = failures[0]
    var budget = 600_000_000
    while budget > 0:
        # Address-dependent loads (the index derives from `budget`, so the
        # load is NOT loop-invariant and CANNOT be hoisted — b2 keeps plain
        # reads of cross-thread state stale otherwise; verified probe).
        var s0 = sc[].phases[budget & 1]
        var s1 = sc[].phases[1 - (budget & 1)]
        if s0 == 1 and s1 == 1:
            break
        budget -= 1
    # Acquire fence: pairs with each worker's release fence at park, so the
    # run_counts/executor/counter writes observed below are all visible.
    fence[Ordering.ACQUIRE]()
    print("scenario wait done budget=" + String(budget)
          + " progress="
          + String(sc[].progress[0].load[ordering=Ordering.RELAXED]()) + ","
          + String(sc[].progress[1].load[ordering=Ordering.RELAXED]())
          + " obs_steals=" + String(sc[].observed_steals))
    if budget == 0:
        _fail(failures, "workers did not quiesce (phases "
                        + String(sc[].phases[0]) + "," + String(sc[].phases[1])
                        + " err " + String(sc[].thread_error[0]) + ","
                        + String(sc[].thread_error[1]) + ")")
        return
    if sc[].thread_error[0] != 0 or sc[].thread_error[1] != 0:
        _fail(failures, "worker thread raised an error in the serve loop")
        return
    _verify(failures, sc)
    if failures[0] == since:
        _report_pass(sc)


def _verify(failures: UnsafePointer[Int, MutAnyOrigin], sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """Acceptance checks (scenario-independent; scene carries the shape)."""
    var w0 = sc[].w0
    var w1 = sc[].w1
    # 1) exactly-once: every task ran exactly once (no loss, no duplicate)
    for tid in range(1, sc[].n_tasks + 1):
        if sc[].run_counts[tid] != 1:
            _fail(failures, "task " + String(tid) + " ran "
                            + String(sc[].run_counts[tid]) + " times (want 1)")
    # 2) migration-forbidden: a STARTED task ran ONLY on its owner worker
    if sc[].started_task != 0:
        var st = sc[].started_task
        if sc[].executor[st] != sc[].owner_of_started:
            _fail(failures, "STARTED task " + String(st) + " executed on worker "
                            + String(sc[].executor[st]) + " (owner "
                            + String(sc[].owner_of_started) + ") — migrated!")
        if sc[].run_counts[st] != 1:
            _fail(failures, "STARTED task " + String(st) + " ran "
                            + String(sc[].run_counts[st])
                            + " times (returned-to-owner must re-run once)")
    # 3) counter exactness: task_steals_total deltas == observed successful
    #    steals (per worker); failed probes bump nothing.
    var d0 = w0[]._runtime.task_steals_total() - sc[].pre_steals_w0
    var d1 = w1[]._runtime.task_steals_total() - sc[].pre_steals_w1
    if d0 + d1 != sc[].observed_steals:
        _fail(failures, "task_steals_total mismatch: observed "
                        + String(sc[].observed_steals) + " successful steals, "
                        "counters w0+" + String(d0) + " w1+" + String(d1))
    if d0 != 0:
        _fail(failures, "worker 0 reported " + String(d0)
                        + " steals (must be 0: it only probes, never steals)")


def _report_pass(sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """Per-scenario accounting for the PR evidence (w0_runs/w1_runs split,
    observed steals, exact counter values)."""
    var w0_runs = 0
    for tid in range(1, sc[].n_tasks + 1):
        if sc[].executor[tid] == 0:
            w0_runs += 1
    var w1_runs = sc[].n_tasks - w0_runs
    var w0 = sc[].w0
    var w1 = sc[].w1
    var d0 = w0[]._runtime.task_steals_total() - sc[].pre_steals_w0
    var d1 = w1[]._runtime.task_steals_total() - sc[].pre_steals_w1
    print("T33 " + _scenario_label(sc) + ": PASS — observed_steals="
          + String(sc[].observed_steals) + " w0_runs=" + String(w0_runs)
          + " w1_runs=" + String(w1_runs) + " counters(w0,w1)=("
          + String(d0) + "," + String(d1) + ")")


def _scenario_label(sc: UnsafePointer[Scene, MutAnyOrigin]) -> String:
    if sc[].started_task != 0:
        return "migration-forbidden (issue #70)"
    if sc[].n_tasks == 0:
        return "empty-peers E6 handoff (issue #70)"
    return "steal-share (issue #70)"


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------

def seed_cell(sc: UnsafePointer[Scene, MutAnyOrigin], tid: Int) raises:
    """Create ONE fresh RUNNABLE TCB cell for `tid` (no record pushed).
    Cells are addressed by TASK ID ((tid-1) stride), so non-contiguous tid
    ranges never collide."""
    var cell = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=sc[].tcb_cells + (tid - 1) * TCB_STRIDE
    )
    cell[0] = TB.create()
    # Spawn bookkeeping: NEW -> RUNNABLE (a queued record is a RUNNABLE
    # never-run task — the §19.1 stealable shape).
    cell[].transition(TaskControlBlock.RUNNABLE)
    sc[].tcb_addrs[tid] = Int(cell)


def seed_tasks(
    sc: UnsafePointer[Scene, MutAnyOrigin],
    wp: UnsafePointer[Worker, MutAnyOrigin],
    first: Int,
    count: Int,
) raises:
    """Heap-allocate `count` fresh TCB cells and push one pre-start
    TaskRecord per task onto wp's local deque (tids first..first+count-1,
    FIFO order)."""
    for k in range(count):
        var tid = first + k
        seed_cell(sc, tid)
        wp[]._local.push(TaskRecord(sc[].tcb_addrs[tid], tid))


def mark_started_reenqueued(sc: UnsafePointer[Scene, MutAnyOrigin], tid: Int) raises:
    """Walk a seeded task's TCB RUNNING -> PARKING -> RUNNABLE: the STARTED
    latch fires at RUNNING and STAYS latched through the re-enqueue —
    the exact shape of a started task whose yield re-queued it (a started
    record that must NEVER be stolen)."""
    var addr = sc[].tcb_addrs[tid]
    var tcbp = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=addr)
    # RUNNABLE -> RUNNING latches STARTED, then the yield re-enqueue walks
    # back to RUNNABLE.
    tcbp[].transition(TaskControlBlock.RUNNING)
    tcbp[].transition(TaskControlBlock.PARKING)
    tcbp[].transition(TaskControlBlock.RUNNABLE)


# ---------------------------------------------------------------------------
# main — the three acceptance scenarios
# ---------------------------------------------------------------------------

def main() raises:
    var failures = Int(0)
    var failuresp = UnsafePointer[Int, MutAnyOrigin](to=failures)

    # --- the 2-worker pool: main-stack Workers (main's frame outlives every
    # worker thread; b2 Workers are not movable/copyable, so no heap cells).
    var w0 = Worker()
    var w1 = Worker()
    var w0p = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    var w1p = UnsafePointer[Worker, MutAnyOrigin](to=w1)
    var peers = UnsafePointer[UnsafePointer[Worker, MutAnyOrigin], MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(2 * 8))
    )
    peers[0] = w0p
    peers[1] = w1p
    w0._index = 0
    w0._peers = peers
    w0._n_workers = 2
    w0._steal_cursor = 1
    w1._index = 1
    w1._peers = peers
    w1._n_workers = 2
    w1._steal_cursor = 0

    # --- scenario 1: steal-share (busy worker 0 budget 8 of 64; idle
    # worker 1 steals the remaining 56 from the opposite end) -------------
    _scenario_steal_share(failuresp, w0p, w1p)

    # --- scenario 2: migration-forbidden (a STARTED task is never stolen
    # or run off-owner; the returned-to-owner record re-runs exactly once)
    _scenario_migration(failuresp, w0p, w1p)

    # --- scenario 3: empty peers -> E6 handoff (no work anywhere; workers
    # probe a CAPped round, find nothing, park; counters stay 0)
    _scenario_empty_peers(failuresp, w0p, w1p)

    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(peers)))

    if failures == 0:
        print("T33 steal (issue #70): PASS")
        _iso_exit(0)
    print("T33 steal (issue #70): FAIL (" + String(failures) + ")")
    _iso_exit(1)


def _fresh_scene(
    failures: UnsafePointer[Int, MutAnyOrigin],
    w0p: UnsafePointer[Worker, MutAnyOrigin],
    w1p: UnsafePointer[Worker, MutAnyOrigin],
    n_tasks: Int,
    budget: Int,
    min_idle: Int,
) raises -> UnsafePointer[Scene, MutAnyOrigin]:
    """Heap-allocate a per-scenario Scene + its cell arrays (run_counts,
    executor, phases, thread_error, tcb_addrs, TCB block)."""
    var scell = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(512))
    )
    scell[0] = Scene()
    scell[].w0 = w0p
    scell[].w1 = w1p
    scell[].n_tasks = n_tasks
    scell[].local_budget = budget
    scell[].min_idle = min_idle
    scell[].pre_steals_w0 = w0p[]._runtime.task_steals_total()
    scell[].pre_steals_w1 = w1p[]._runtime.task_steals_total()
    if n_tasks > 0:
        scell[].tcb_cells = Int(c_malloc(n_tasks * TCB_STRIDE))
        var addrs = UnsafePointer[Int, MutUntrackedOrigin](
            unsafe_from_address=Int(c_malloc((n_tasks + 1) * 8))
        )
        for i in range(n_tasks + 1):
            addrs[i] = 0
        scell[].tcb_addrs = addrs
    var rc = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc((n_tasks + 1) * 8))
    )
    var ex = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc((n_tasks + 1) * 8))
    )
    for i in range(n_tasks + 1):
        rc[i] = 0
        ex[i] = -1
    scell[].run_counts = rc
    scell[].executor = ex
    var ph = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(2 * 8))
    )
    var te = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(2 * 8))
    )
    var pr = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2 * 64))
    )
    ph[0] = 0
    ph[1] = 0
    te[0] = 0
    te[1] = 0
    pr[0] = Atomic[DType.int64](0)
    pr[1] = Atomic[DType.int64](0)
    scell[].phases = ph
    scell[].thread_error = te
    scell[].progress = pr
    return scell


def _scenario_steal_share(
    failures: UnsafePointer[Int, MutAnyOrigin],
    w0p: UnsafePointer[Worker, MutAnyOrigin],
    w1p: UnsafePointer[Worker, MutAnyOrigin],
) raises:
    comptime N = Int(64)
    comptime BUDGET = Int(8)
    var sc = _fresh_scene(failures, w0p, w1p, N, BUDGET, 1024)
    seed_tasks(sc, w0p, 1, N)   # worker 0's deque: tids 1..64, FIFO
    run_scenario(failures, sc)


def _scenario_migration(
    failures: UnsafePointer[Int, MutAnyOrigin],
    w0p: UnsafePointer[Worker, MutAnyOrigin],
    w1p: UnsafePointer[Worker, MutAnyOrigin],
) raises:
    comptime N = Int(12)
    comptime STARTED = Int(7)
    var sc = _fresh_scene(failures, w0p, w1p, N, 0, 1024)
    sc[].started_task = STARTED
    sc[].owner_of_started = 1   # the owner worker
    # worker 1's deque: fresh 1..6, fresh 8..12, THEN the STARTED record at
    # the TAIL — the thief's very first steal_front pops it, the STARTED
    # guard rejects it and returns it to the owner, and the probe fails.
    # Every cell is created first (including the STARTED task's own), then
    # records are pushed 1..6 / 8..12, then the STARTED record is pushed
    # last so it sits at the thief-facing tail.
    for k in range(6):
        seed_cell(sc, k + 1)
    for k in range(5):
        seed_cell(sc, k + 8)
    seed_cell(sc, STARTED)
    for k in range(6):
        w1p[]._local.push(TaskRecord(sc[].tcb_addrs[k + 1], k + 1))
    for k in range(5):
        w1p[]._local.push(TaskRecord(sc[].tcb_addrs[k + 8], k + 8))
    mark_started_reenqueued(sc, STARTED)
    w1p[]._local.push(TaskRecord(sc[].tcb_addrs[STARTED], STARTED))
    run_scenario(failures, sc)


def _scenario_empty_peers(
    failures: UnsafePointer[Int, MutAnyOrigin],
    w0p: UnsafePointer[Worker, MutAnyOrigin],
    w1p: UnsafePointer[Worker, MutAnyOrigin],
) raises:
    var sc = _fresh_scene(failures, w0p, w1p, 0, 0, 1024)
    run_scenario(failures, sc)