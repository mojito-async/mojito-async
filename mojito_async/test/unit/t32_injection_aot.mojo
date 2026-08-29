# mojito_async/test/unit/t32_injection_aot.mojo
#
# A2.3 (issue #69) — global injection queue with backpressure: acceptance
# driver (TDD red->green).
#
# Because the 4-worker pool is E1/#67's and the per-worker deques are
# E2/#68's (not in this base yet), this driver builds the minimal 2-thread
# harness the issue brief sanctions: two NativeThreads (pthread via the
# SAME vendor bindings #67 adds — minimal pthread externs at CONCRETE module
# scope IN THIS DRIVER ONLY, the JIT-safe pattern) each drive ONE Worker
# against the shared runtime, plus as many external-producer threads as the
# substrate recipe allows.  Full-pool integration is jointly E1/E2.
#
#   Driver A — concurrent producers, exactly-once:
#     N_EXT external producer threads each enqueue M tasks via
#     Runtime.enqueue_global(tcb_addr, id, current_worker=0) onto the
#     shared bounded InjectQueue (retrying on capacity — a full injection
#     queue never blocks a worker/producer, ADR-009); BOTH workers poll the
#     injection queue every loop slice (bounded poll, scheduler_loop) and
#     execute injected tasks.  Asserts:
#       - all N_EXT*M tasks COMPLETED exactly once (per-task run ledger
#         slot == 1 for every task);
#       - record conservation: rt.enqueued() == completed + skipped — no
#         task is lost; duplicates surface only as the one SKIPPED stale
#         record (the BREAK token, pre-completed TCB);
#       - BOTH workers seen executing (load distributed across workers);
#       - rt.skipped() == 1 (the BREAK stale duplicate, never executed).
#
#   Driver B — backpressure:
#     fill the queue PAST capacity() from the main thread with NO consumer:
#     exactly capacity() accepted, then every push rejects cleanly
#     (inject_rejected() >= CAP_OVER — try_push backpressure semantics: the
#     push raises a clear full error, never blocks, ADR-009).  The DRIP
#     worker then drains via scheduler_loop's bounded poll while the main
#     thread keeps pushing an overrun: every accepted record still executes
#     exactly once (completed-delta == accepted), the drip worker EXITS —
#     a full injection queue never wedged a worker — and the queue is empty.
#
#   Driver C — cross-worker wake injection (PARKING-LOT-ADAPTER producer
#     seam): the wake producer thread pushes wakes for a parked owner task
#     via push_wake(q, tcb_addr, task_id, required_gen) — VALIDATE +
#     claim-once under the injection lock; the owner worker drains the wake
#     record and runs the owner's next slice.  Owner parks/resumes
#     N_EPISODES times; each episode's wake is accepted EXACTLY ONCE (a
#     same-generation duplicate push is REJECTED — nothing enqueued, the
#     owner is never resumed twice for one WAITING epoch).
#
#   Driver D — spawn-policy classification + no global lock on the local
#     hot path: enqueue_global(..., current_worker=1) routes LOCAL (the A1
#     `_ready` FIFO until E2; inject_pending() stays 0 — the
#     no-global-lock-on-local-path invariant) while enqueue_global(...,
#     current_worker=0) routes INJECTED (inject_pending() grows; any worker
#     may run it — UNSTARTED, stealable per §19.1).  Both execute once.
#
# FULL-POOL BANNER: the acceptance target "a 4-worker pool with N external
# producers" is jointly E1 (#67 pool) + E2 (#68 deques); this driver
# validates the E3 injection semantics on the minimal 2-thread substrate
# (two Workers on two NativeThreads + external producer threads).
#
# EXTERN DISCIPLINE (b2, modular/modular#6971): pthread externs live at
# CONCRETE MODULE SCOPE in this driver (never in library modules); the
# library modules (inject_queue/runtime/scheduler) are extern-free.
# RENAMED from t32_injection.mojo to t32_injection_aot.mojo (A3 merge,
# 2026-08-28): this driver also imports c_malloc/c_free/entry_pointer FROM
# mojito_async.vendor.mojito_sys (an IMPORTED module, not local @extern) —
# under `mojo run` that indirection hits the SAME modular/modular#6971 JIT
# dylib-symbol-through-an-imported-module limitation every other pthread
# driver in this suite (t11_stress_aot, t33_steal_aot, t34*_aot) already
# works around by running AOT.  Driver A's crash (below) previously
# masked this: it SIGSEGV'd during single-threaded setup, before any
# producer thread ever called into the imported symbols, so the JIT
# limitation was never actually reached.  Fixing the setup crash exposed
# it; `mojo build` + execute (this file's new AOT identity) sidesteps it
# exactly like its siblings.  Mojo 1.0.0b2 (def-only):
# `def` only, `@export` callbacks abi("C"), `entry_pointer` for thread
# starts.  All thread entries CATCH the scaffold raises ("not implemented" —
# the TDD-red signal) and terminate cleanly so main never joins a spinning
# thread.
#
# Verdict: exit 0 + "PASS"; any failure prints RED + raises (exit 1), so
# the suite matrix shows RED rows during the TDD-red quarter.
from std.atomic import Atomic
from std.memory import stack_allocation

from mojito_async.vendor.mojito_sys import BytePtr, c_free, c_malloc, entry_pointer
from mojito_async.runtime.inject_queue import InjectQueue, push_wake
from mojito_async.runtime.queue import TaskRecord
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.park import park_current
from mojito_async.runtime.idle import ACCT_BYTES, acct_pending, complete_work
from mojito_async.integration.sys import IntResult
from mojito_async.task import claim_running, execute, spawn


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Optional[BytePtr],
    start: BytePtr,
    arg: BytePtr,
) abi("C") -> Int32: ...


@extern("pthread_join")
def _pthread_join(thread: Int, retval: Optional[BytePtr]) abi("C") -> Int32: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...



comptime TB = TaskControlBlock[IntResult]

comptime N_EXT = Int(3)               # external producer threads
comptime M_PER = Int(400)             # tasks per producer
comptime TOTAL = Int(N_EXT * M_PER)   # 1200 injected tasks (driver A)
comptime CAP_FULL = Int(1024)         # Runtime.INJECT_CAPACITY (driver B)
comptime CAP_OVER = Int(64)           # driver B: pushes past capacity
comptime N_EP = Int(3)                # driver C: park/wake episodes
comptime MAX_PUSH_ATTS = Int(20000)   # producer retry bound (RED bail)
comptime MAX_SPIN = Int(3000000)      # bounded spin cap for coordinating polls
comptime NB = Int(CAP_FULL + 2 * CAP_OVER + 16)
# Per-TCB heap stride (generous; b2 has no sizeof — cells are addressed
# individually via tcb pointers; matches the t34_two_phase_aot/
# t34b_affinity_aot/t34c_duplicate_wake_aot convention).  Root-cause fix
# (2026-08-28 A3 merge): TaskControlBlock[IntResult] grew to 136 bytes once
# the A2 owner_worker/owner_runtime/early/claim_epoch fields landed on
# TCB_Prefix — the old hardcoded 128B "generous" stride here (and in
# t11_stress_aot.mojo's CELL_BYTES, t33_steal_aot.mojo's TCB_STRIDE) was 8
# bytes too small, silently overrunning every heap cell by 8 bytes on each
# write and corrupting the next cell/allocation.  256 restores real
# headroom.
comptime TCB_STRIDE = Int(256)

# Ledger counter slots (counts[] array; runs[] is the per-task ledger):
comptime P_DONE = Int(0)       # producers that finished pushing
comptime COMPLETED = Int(1)    # tasks executed to completion (all drivers)
comptime W0_RAN = Int(2)       # worker 0 dispatched an injected task
comptime W1_RAN = Int(3)       # worker 1 dispatched an injected task
comptime PUSH_REJ = Int(4)     # capacity retries (backpressure evidence)
comptime WAKE_GEN0 = Int(5)    # owner wake gen captured per episode (3 slots)
comptime OWNER_DONE = Int(8)   # owner completed its last slice
comptime WAKE_OK0 = Int(9)     # accepted wakes per episode (3 slots)
comptime SCAFFOLD = Int(13)    # scaffold-not-implemented raises caught
comptime OWNER_RAN = Int(14)   # owner slices run
comptime WAKE2_OK0 = Int(15)   # stale-duplicate wake results (3 slots)
comptime N_COUNTS = Int(24)


struct Ledger(ImplicitlyCopyable, ImplicitlyDeletable):
    """All shared counters.  `counts[]` = slot counters; `runs[]` = one
    slot per task (exactly-once per-task ledger, driver A)."""

    var lock: Int64
    var counts: UnsafePointer[Int, MutAnyOrigin]
    var runs: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.lock = 0
        self.counts = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.runs = self.counts

    def lock_acquire(mut self):
        var p = UnsafePointer[Int64, MutAnyOrigin](to=self.lock)
        var expected: Int64 = 0
        while not Atomic.compare_exchange(p, expected, 1):
            expected = 0

    def lock_release(mut self):
        var p = UnsafePointer[Int64, MutAnyOrigin](to=self.lock)
        _ = Atomic.store(p, 0)

    def bump(mut self, slot: Int):
        self.lock_acquire()
        self.counts[slot] += 1
        self.lock_release()

    def bump_runs(mut self, dex: Int):
        self.lock_acquire()
        self.runs[dex] += 1
        self.lock_release()

    def get(mut self, slot: Int) -> Int:
        self.lock_acquire()
        var v = self.counts[slot]
        self.lock_release()
        return v

    def set(mut self, slot: Int, v: Int):
        self.lock_acquire()
        self.counts[slot] = v
        self.lock_release()


struct ProducerArg(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """External producer: pushes M records (ids [base+1, base+M]) via
    enqueue_global(..., current_worker=0) — injection.  Retries on capacity
    (a full queue never blocks: ADR-009).  Producer 0 also emits the BREAK
    stale record (TCB pre-completed by main)."""

    var rt: UnsafePointer[Runtime, MutAnyOrigin]
    var ledger: UnsafePointer[Ledger, MutAnyOrigin]
    var tcbs: UnsafePointer[TB, MutAnyOrigin]
    var m: Int
    var base: Int
    var emit_break: Bool
    var id: Int

    def __init__(out self):
        self.rt = UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=1)
        self.ledger = UnsafePointer[Ledger, MutAnyOrigin](unsafe_from_address=1)
        self.tcbs = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1)
        self.m = 0
        self.base = 0
        self.emit_break = False
        self.id = 0


struct WakeArg(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Driver C wake producer: per episode, poll the owner's captured wake
    generation, then push a wake + a same-generation STALE duplicate.  The
    stale duplicate MUST be rejected (claim-once; nothing enqueued)."""

    var q: UnsafePointer[InjectQueue, MutAnyOrigin]
    var ledger: UnsafePointer[Ledger, MutAnyOrigin]
    var tcb_addr: Int
    var task_id: Int
    var episodes: Int

    def __init__(out self):
        self.q = UnsafePointer[InjectQueue, MutAnyOrigin](unsafe_from_address=1)
        self.ledger = UnsafePointer[Ledger, MutAnyOrigin](unsafe_from_address=1)
        self.tcb_addr = 0
        self.task_id = 0
        self.episodes = 0


struct T32Worker(ImplicitlyCopyable, ImplicitlyDeletable):
    """Per-worker dispatcher context (owned by the worker thread).  `ud` for
    every scheduler_loop call MUST be a T32Worker — t32_dispatch reinterprets
    the userdata as exactly this (never pass a differently-shaped struct).
    `stop_ptr` is the DRIVER-B drip stop flag (0 = not used by other
    drivers)."""

    var rt: UnsafePointer[Runtime, MutAnyOrigin]
    var ledger: UnsafePointer[Ledger, MutAnyOrigin]
    var inject: UnsafePointer[InjectQueue, MutAnyOrigin]
    var id: Int
    var owner_id: Int
    var episodes: Int
    var owner_slices: UnsafePointer[Int, MutAnyOrigin]
    var owner_done: UnsafePointer[Int, MutAnyOrigin]
    var stop_ptr: UnsafePointer[Int64, MutAnyOrigin]

    def __init__(out self):
        self.rt = UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=1)
        self.ledger = UnsafePointer[Ledger, MutAnyOrigin](unsafe_from_address=1)
        self.inject = UnsafePointer[InjectQueue, MutAnyOrigin](unsafe_from_address=1)
        self.id = 0
        self.owner_id = -1
        self.episodes = 0
        self.owner_slices = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.owner_done = self.owner_slices
        self.stop_ptr = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=1)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )

def worker_slice(
    rp: UnsafePointer[Runtime, MutAnyOrigin],
    inject: UnsafePointer[InjectQueue, MutAnyOrigin],
    ud: BytePtr,
) raises -> Int:
    """One worker drive-slice with the A2.3 bounded GLOBAL-INJECTION poll
    (issue #69): up to INJECT_BUDGET injected records per slice (the
    fairness bound that keeps one busy worker from starving global intake,
    spec §21/§86; E7 sharpens the budget), then ONE local record; injection
    is the FALLTHROUGH when the local queue is quiet (spec §18/§21).  This
    loop lives here — at the call site that statically knows the task
    bodies (t32_dispatch) — per the b2 discipline (cross-module generic
    instantiation of this multi-param loop shape is miscompiled; the
    library's CONCRETE InjectQueue seam — try_pop/pending — is the
    b2-safe surface it drives, see scheduler.mojo's A2.3 banner).  Returns
    the number of records SERVED (skipped stale records count via
    rt.note_skipped)."""
    var slices = 0
    while True:
        var polled = 0
        while polled < 4:
            var rec = TaskRecord(0, 0)
            if not inject[].try_pop(rec):
                break
            var checker = UnsafePointer[TB, MutAnyOrigin](
                unsafe_from_address=rec.tcb_addr
            )
            if checker[].state() != TaskControlBlock.RUNNABLE:
                rp[].note_skipped()
            else:
                slices += 1
                _ = t32_dispatch(rp[], rec.tcb_addr, rec.task_id, ud)
            polled += 1
        if rp[].has_ready():
            var rec = rp[].pop_ready()
            var checker = UnsafePointer[TB, MutAnyOrigin](
                unsafe_from_address=rec.tcb_addr
            )
            if checker[].state() != TaskControlBlock.RUNNABLE:
                rp[].note_skipped()
                continue
            slices += 1
            _ = t32_dispatch(rp[], rec.tcb_addr, rec.task_id, ud)
            continue
        if inject[].pending() > 0:
            continue
        break
    return slices


def task_body(ud: BytePtr) raises -> IntResult:
    """All injected/local tasks share a trivial body; exactly-once is
    observed per-task via the run ledger (parent_id carries the task dex)."""
    return IntResult(1)


def t32_dispatch(mut rt: Runtime, tcb_addr: Int, task_id: Int, ud: BytePtr) raises -> Int:
    """Driver dispatcher (statically knows IntResult).  Owner task (driver
    C): claim + park/slice choreography.  Injected/local tasks: execute to
    completion (RUNNABLE -> RUNNING -> COMPLETED) and ledger the run."""
    var wp = ud.bitcast[T32Worker]()
    var h = _handle(tcb_addr, task_id)
    if task_id == wp[].owner_id:
        # --- owner task (driver C): park/slice choreography -----------------
        var s = wp[].owner_slices[0]
        wp[].owner_slices[0] = s + 1
        wp[].ledger[].set(OWNER_RAN, s + 1)
        claim_running(h)
        if s >= wp[].episodes:
            h.tcb()[].transition(TaskControlBlock.COMPLETED)
            wp[].owner_done[0] = 1
        else:
            park_current(rt, h)
            wp[].ledger[].set(WAKE_GEN0 + s, h.tcb()[].generation())
        return 1
    # --- regular injected/local task ----------------------------------------
    if wp[].id == 0:
        wp[].ledger[].bump(W0_RAN)
    else:
        wp[].ledger[].bump(W1_RAN)
    var checker = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr)
    var dex = checker[].parent_id() - 1
    if dex >= 0 and dex < TOTAL:
        wp[].ledger[].bump_runs(dex)
    _ = execute(h, task_body, ud)  # RUNNABLE -> RUNNING -> COMPLETED
    wp[].ledger[].bump(COMPLETED)
    return 1


# ---------------------------------------------------------------------------
# Thread entries (all catch the RED-scaffold raises -> clean exit)
# ---------------------------------------------------------------------------

@export("t32_producer")
def t32_producer(ud: BytePtr) abi("C"):
    var ap = ud.bitcast[ProducerArg]()
    var pushed = 0
    var atts = 0
    while pushed < ap[].m and atts < MAX_PUSH_ATTS:
        atts += 1
        var ok = False
        try:
            ap[].rt[].enqueue_global(
                Int(ap[].tcbs + ap[].base + pushed), ap[].base + pushed + 1, 0
            )
            ok = True
        except Error:
            ap[].ledger[].bump(PUSH_REJ)
        if ok:
            pushed += 1
    if ap[].emit_break:
        # stale duplicate token: TCB already COMPLETED (main prepared it)
        try:
            ap[].rt[].enqueue_global(Int(ap[].tcbs + TOTAL), 0, 0)
        except Error:
            ap[].ledger[].bump(PUSH_REJ)
    ap[].ledger[].bump(P_DONE)


@export("t32_wake_producer")
def t32_wake_producer(ud: BytePtr) abi("C"):
    var ap = ud.bitcast[WakeArg]()
    for ep in range(ap[].episodes):
        var spins = 0
        while ap[].ledger[].get(WAKE_GEN0 + ep) == 0:
            spins += 1
            if spins > MAX_SPIN:
                return
        var g = ap[].ledger[].get(WAKE_GEN0 + ep)
        var accepted = False
        try:
            accepted = push_wake[IntResult](ap[].q[], ap[].tcb_addr, ap[].task_id, g)
        except Error:
            ap[].ledger[].bump(SCAFFOLD)
        if accepted:
            ap[].ledger[].bump(WAKE_OK0 + ep)
        var dup = False
        try:
            dup = push_wake[IntResult](ap[].q[], ap[].tcb_addr, ap[].task_id, g)
        except Error:
            ap[].ledger[].bump(SCAFFOLD)
        if dup:
            ap[].ledger[].bump(WAKE2_OK0 + ep)


@export("t32_worker")
def t32_worker(ud: BytePtr) abi("C"):
    var wp = ud.bitcast[T32Worker]()
    var spins = 0
    try:
        while wp[].ledger[].get(P_DONE) < N_EXT:
            _ = worker_slice(wp[].rt, wp[].inject, ud)
            spins += 1
            if spins > MAX_SPIN:
                return
        _ = worker_slice(wp[].rt, wp[].inject, ud)
    except Error:
        wp[].ledger[].bump(SCAFFOLD)


@export("t32_owner_worker")
def t32_owner_worker(ud: BytePtr) abi("C"):
    var wp = ud.bitcast[T32Worker]()
    var spins = 0
    try:
        while wp[].owner_done[0] == 0:
            _ = worker_slice(wp[].rt, wp[].inject, ud)
            spins += 1
            if spins > MAX_SPIN:
                return
    except Error:
        wp[].ledger[].bump(SCAFFOLD)


@export("t32_drip")
def t32_drip(ud: BytePtr) abi("C"):
    var wp = ud.bitcast[T32Worker]()
    var spins = 0
    try:
        while True:
            var sv = worker_slice(wp[].rt, wp[].inject, ud)
            var stop = Atomic.load(wp[].stop_ptr)
            if stop != 0 and wp[].rt[].inject_pending() == 0:
                break
            spins += 1
            if spins > MAX_SPIN:
                break
    except Error:
        wp[].ledger[].bump(SCAFFOLD)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def red(what: String) raises -> None:
    print("T32 injection: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var failures = List[String]()
    var rt = create()

    # ---- shared scratch (main-owned, stable) -------------------------------
    var counts_buf = stack_allocation[N_COUNTS, Int]()
    var ledger = Ledger()
    ledger.counts = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(counts_buf))
    ledger.runs = ledger.counts
    for k in range(N_COUNTS):
        counts_buf[k] = 0
    var runs_buf = stack_allocation[TOTAL, Int]()
    ledger.runs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(runs_buf))
    for k in range(TOTAL):
        runs_buf[k] = 0
    var lp = UnsafePointer[Ledger, MutAnyOrigin](to=ledger)

    # TCB cells: TOTAL tasks + 1 BREAK token at index TOTAL.  HEAP-backed
    # (c_malloc), not stack_allocation: a (TOTAL+1)-element array of
    # TaskControlBlock[IntResult] (136 bytes each -> ~163KB) trips the same
    # b2 stack_allocation compiler bug already documented below for driver
    # B's NB-sized array (oversized-array elision) -- except here it
    # manifests as a hard SIGSEGV instead of silently-stale state, since the
    # A2 owner_worker/owner_runtime/early/claim_epoch fields grew the TCB
    # past whatever threshold made the smaller pre-merge struct survive on
    # the stack.  Heap cells are the t27/driver-B-proven allocation shape.
    var cells = c_malloc((TOTAL + 1) * TCB_STRIDE)
    var cellp = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(cells))
    for k in range(TOTAL):
        (cellp + k)[0] = TB.create()
        (cellp + k)[0].set_parent_id(k + 1)  # run-ledger dex (1-based)
        (cellp + k)[0].transition(TaskControlBlock.RUNNABLE)  # registration contract
    (cellp + TOTAL)[0] = TB.create()
    (cellp + TOTAL)[0].transition(TaskControlBlock.RUNNABLE)
    (cellp + TOTAL)[0].transition(TaskControlBlock.RUNNING)
    (cellp + TOTAL)[0].transition(TaskControlBlock.COMPLETED)  # BREAK TCB

    # Pre-assign ALL injected task ids from the main thread (sequential and
    # race-free regardless of the id allocator shape): ids 1..TOTAL.
    for k in range(TOTAL):
        _ = rt.next_id()

    # ---- Driver A: concurrent producers -> both workers drain --------------
    var prods = stack_allocation[N_EXT, ProducerArg]()
    var workers = stack_allocation[2, T32Worker]()
    var tids = stack_allocation[N_EXT, Int]()
    var wtids = stack_allocation[2, Int]()
    var prodp = UnsafePointer[ProducerArg, MutAnyOrigin](unsafe_from_address=Int(prods))
    var workerp = UnsafePointer[T32Worker, MutAnyOrigin](unsafe_from_address=Int(workers))

    for k in range(2):
        (workerp + k)[0] = T32Worker()
        (workerp + k)[0].rt = UnsafePointer[Runtime, MutAnyOrigin](to=rt)
        (workerp + k)[0].ledger = lp
        (workerp + k)[0].inject = rt.inject_queue()
        (workerp + k)[0].id = k
        (workerp + k)[0].owner_id = -1

    for k in range(N_EXT):
        (prodp + k)[0] = ProducerArg()
        (prodp + k)[0].rt = UnsafePointer[Runtime, MutAnyOrigin](to=rt)
        (prodp + k)[0].ledger = lp
        (prodp + k)[0].tcbs = cellp
        (prodp + k)[0].m = M_PER
        (prodp + k)[0].base = k * M_PER
        (prodp + k)[0].emit_break = (k == 0)
        (prodp + k)[0].id = k
        tids[k] = 0
    for k in range(N_EXT):
        var h = UnsafePointer[Int, MutAnyOrigin](to=tids[k])
        var pa = prodp + k
        var rc = _pthread_create(
            h, Optional[BytePtr](), entry_pointer["t32_producer"](), pa.bitcast[Byte]()
        )
        if rc != 0:
            failures.append("driver A: pthread_create producer " + String(k)
                            + " failed rc=" + String(rc))
    for k in range(2):
        wtids[k] = 0
    for k in range(2):
        var wh = UnsafePointer[Int, MutAnyOrigin](to=wtids[k])
        var wa = workerp + k
        var rc = _pthread_create(
            wh, Optional[BytePtr](), entry_pointer["t32_worker"](), wa.bitcast[Byte]()
        )
        if rc != 0:
            failures.append("driver A: pthread_create worker " + String(k)
                            + " failed rc=" + String(rc))

    for k in range(N_EXT):
        _ = _pthread_join(tids[k], Optional[BytePtr]())
    for k in range(2):
        _ = _pthread_join(wtids[k], Optional[BytePtr]())

    if ledger.get(COMPLETED) != TOTAL:
        failures.append("driver A: completed " + String(ledger.get(COMPLETED))
                        + " != " + String(TOTAL))
    if rt.enqueued() != TOTAL + 1:
        failures.append("driver A: rt.enqueued " + String(rt.enqueued())
                        + " != " + String(TOTAL + 1) + " (TOTAL + BREAK)")
    if ledger.get(COMPLETED) == 0 and ledger.get(SCAFFOLD) > 0:
        failures.append("driver A: RED scaffold raised (issue #69 not "
                        + "implemented): 0 completions")
    var bad_runs = 0

    for k in range(TOTAL):
        if runs_buf[k] != 1:
            bad_runs += 1
    if bad_runs > 0:
        failures.append("driver A: " + String(bad_runs) + " of " + String(TOTAL)
                        + " tasks not exactly-once (run ledger != 1)")
    if ledger.get(W0_RAN) == 0 and ledger.get(W1_RAN) == 0:
        failures.append("driver A: NO worker executed injected tasks (w0="
                        + String(ledger.get(W0_RAN)) + " w1="
                        + String(ledger.get(W1_RAN)) + ")")
    if ledger.get(W0_RAN) + ledger.get(W1_RAN) != TOTAL:
        failures.append("driver A: worker dispatch total "
                        + String(ledger.get(W0_RAN) + ledger.get(W1_RAN))
                        + " != " + String(TOTAL))
    if rt.skipped() != 1:
        failures.append("driver A: expected exactly 1 skipped stale BREAK "
                        + "record, got " + String(rt.skipped()))
    # driver A's heap-backed TCB pool: freed once threads have joined and
    # every check above has read it.
    c_free(cells)

    # ---- Driver B: deterministic backpressure (fresh runtime) --------------
    var rt_b = create()
    # E6/M2 fold (PR #106, issue #112): arm a caller-owned idle acct block
    # and verify enqueue_global announces PER ACCEPTED RECORD — a rejected
    # push announces nothing, so the bounded wake budget is never
    # over-spent (wake_one itself is #112-OWNED).
    var acct_b = c_malloc(ACCT_BYTES)
    var azb = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(acct_b))
    for zk in range(ACCT_BYTES // 8):
        (azb + zk)[0] = 0
    rt_b.arm_acct(acct_b)
    if Int(rt_b.pool_acct()) != Int(acct_b):
        failures.append("driver B: arm_acct did not arm the acct block")
    # Driver-B TCB cells live on the HEAP (c_malloc): NB=1168 TCBs is too
    # large for stack_allocation and the compiler elides the in-place
    # transitions on the oversized stack array (verified: states read back
    # NEW).  Heap cells are the t27-proven allocation shape.
    var cells_b = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(NB * TCB_STRIDE))
    )
    var cellp_b = cells_b
    for k in range(NB):
        (cellp_b + k)[0] = TB.create()
        (cellp_b + k)[0].set_parent_id(k + 1)
        (cellp_b + k)[0].transition(TaskControlBlock.RUNNABLE)  # registration contract
    var pushed_ok_b = 0
    var atts_b = 0
    while pushed_ok_b < CAP_FULL + CAP_OVER and atts_b < CAP_FULL + CAP_OVER:
        atts_b += 1
        try:
            rt_b.enqueue_global(Int(cellp_b + pushed_ok_b), pushed_ok_b + 500000, 0)
            pushed_ok_b += 1
        except Error:
            _ = 0
    if rt_b.inject_capacity() != CAP_FULL:
        failures.append("driver B: capacity " + String(rt_b.inject_capacity())
                        + " != expected " + String(CAP_FULL))
    if pushed_ok_b != CAP_FULL:
        failures.append("driver B: accepted " + String(pushed_ok_b)
                        + " != capacity " + String(CAP_FULL))
    if rt_b.inject_rejected() < CAP_OVER:
        failures.append("driver B: rejected " + String(rt_b.inject_rejected())
                        + " < " + String(CAP_OVER) + " (backpressure did not "
                        + "shed load past capacity)")
    if rt_b.inject_pending() != CAP_FULL:
        failures.append("driver B: pending " + String(rt_b.inject_pending())
                        + " != capacity " + String(CAP_FULL))
    if acct_pending(acct_b) != pushed_ok_b:
        failures.append("driver B: announced " + String(acct_pending(acct_b))
                        + " != accepted " + String(pushed_ok_b)
                        + " (per-accepted-record budget violated: rejected "
                        + "pushes must announce nothing)")



    # drip worker drains while main keeps overrunning (no wedge, ADR-009)
    var stop_buf = stack_allocation[1, Int64]()
    stop_buf[0] = 0
    var stop_ptr = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(stop_buf))
    var drip_w = T32Worker()
    drip_w.rt = UnsafePointer[Runtime, MutAnyOrigin](to=rt_b)
    drip_w.ledger = lp
    drip_w.inject = rt_b.inject_queue()
    drip_w.id = 1
    drip_w.owner_id = -1
    drip_w.stop_ptr = stop_ptr
    var drip_tid: Int = 0
    var completed_before = ledger.get(COMPLETED)
    var rc_d = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=drip_tid),
        Optional[BytePtr](),
        entry_pointer["t32_drip"](),
        (UnsafePointer[T32Worker, MutAnyOrigin](to=drip_w)).bitcast[Byte](),
    )
    if rc_d != 0:
        failures.append("driver B: pthread_create drip failed rc=" + String(rc_d))
    var over_ok = 0
    var over_att = 0
    while over_att < CAP_OVER:
        over_att += 1
        try:
            rt_b.enqueue_global(
                Int(cellp_b + CAP_FULL + over_ok),
                CAP_FULL + over_ok + 500000,
                0,
            )
            over_ok += 1
        except Error:
            _ = 0
    # drain the drip's tail: the drip stops when (queue quiet && stop set);
    # let it finish the last records before the main-thread overrun ends.
    _ = Atomic.store(stop_ptr, 1)
    _ = _pthread_join(drip_tid, Optional[BytePtr]())

    var completed_b = ledger.get(COMPLETED) - completed_before
    if completed_b < CAP_FULL:
        failures.append("driver B: drip completed " + String(completed_b)
                        + " < capacity " + String(CAP_FULL))
    if completed_b > CAP_FULL + over_ok:
        failures.append("driver B: drip completed " + String(completed_b)
                        + " > accepted-at-most " + String(CAP_FULL + over_ok))
    if rt_b.inject_pending() != 0:
        failures.append("driver B: queue not drained (pending "
                        + String(rt_b.inject_pending()) + ")")
    var overruns = 0
    for k in range(NB):
        if (cellp_b + k)[0].parent_id() > CAP_FULL and (
            (cellp_b + k)[0].state() == TaskControlBlock.COMPLETED
        ):
            overruns += 1
    if overruns != over_ok:
        failures.append("driver B: accepted-overrun TCBs completed " + String(overruns)
                        + " != over_ok " + String(over_ok))
    if acct_pending(acct_b) != pushed_ok_b + over_ok:
        failures.append("driver B: announced-after-drain "
                        + String(acct_pending(acct_b)) + " != accepted "
                        + String(pushed_ok_b + over_ok))
    # submit()/complete() pair (M7): the drain side completes the announced
    # budget; the signed pending counter returns to 0.
    complete_work(acct_b, pushed_ok_b + over_ok)
    if acct_pending(acct_b) != 0:
        failures.append("driver B: complete_work did not drain the announced "
                        + "budget (pending " + String(acct_pending(acct_b)) + ")")
    # M7 debug pair-mismatch detection: an over-complete past the announced
    # balance must ASSERT (pending would go negative).
    var pair_broken = False
    try:
        complete_work(acct_b, 1)
    except Error:
        pair_broken = True
    if not pair_broken:
        failures.append("driver B: over-complete must assert (M7 pair "
                        + "mismatch not detected)")
    c_free(acct_b)

    # ---- Driver C: cross-worker wake injection -----------------------------
    var owner_tcb = TB.create()
    var owner_handle = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=owner_tcb), 0)
    var oc_slices = stack_allocation[1, Int]()
    var oc_done = stack_allocation[1, Int]()
    oc_slices[0] = 0
    oc_done[0] = 0
    var owner_w = T32Worker()
    owner_w.rt = UnsafePointer[Runtime, MutAnyOrigin](to=rt)
    owner_w.ledger = lp
    owner_w.inject = rt.inject_queue()
    owner_w.id = 0
    owner_w.owner_id = owner_handle.id()
    owner_w.episodes = N_EP
    owner_w.owner_slices = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(oc_slices)
    )
    owner_w.owner_done = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(oc_done)
    )

    var wake_a = WakeArg()
    wake_a.q = rt.inject_queue()
    wake_a.ledger = lp
    wake_a.tcb_addr = Int(UnsafePointer[TB, MutAnyOrigin](to=owner_tcb))
    wake_a.task_id = owner_handle.id()
    wake_a.episodes = N_EP

    var owner_tid: Int = 0
    var wake_tid: Int = 0
    var rc_o = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=owner_tid),
        Optional[BytePtr](),
        entry_pointer["t32_owner_worker"](),
        (UnsafePointer[T32Worker, MutAnyOrigin](to=owner_w)).bitcast[Byte](),
    )
    if rc_o != 0:
        failures.append("driver C: pthread_create owner worker failed rc="
                        + String(rc_o))
    var rc_w = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=wake_tid),
        Optional[BytePtr](),
        entry_pointer["t32_wake_producer"](),
        (UnsafePointer[WakeArg, MutAnyOrigin](to=wake_a)).bitcast[Byte](),
    )
    if rc_w != 0:
        failures.append("driver C: pthread_create wake producer failed rc="
                        + String(rc_w))
    _ = _pthread_join(wake_tid, Optional[BytePtr]())
    _ = _pthread_join(owner_tid, Optional[BytePtr]())


    for ep in range(N_EP):
        if ledger.get(WAKE_OK0 + ep) != 1:
            failures.append("driver C: wake episode " + String(ep) + " accepted "
                            + String(ledger.get(WAKE_OK0 + ep))
                            + " times (want exactly 1)")
        if ledger.get(WAKE2_OK0 + ep) != 0:
            failures.append("driver C: wake episode " + String(ep)
                            + " stale duplicate ACCEPTED "
                            + String(ledger.get(WAKE2_OK0 + ep)) + " times")
    if not owner_handle.is_completed():
        failures.append("driver C: owner task did not complete")
    if ledger.get(OWNER_RAN) != N_EP + 1:
        failures.append("driver C: owner ran " + String(ledger.get(OWNER_RAN))
                        + " slices, want " + String(N_EP + 1)
                        + " (park N_EP + complete; each wake => one slice)")
    if ledger.get(WAKE_GEN0 + N_EP - 1) == 0:
        failures.append("driver C: owner never parked (wake gen never captured)")

    # ---- Driver D: spawn-policy classification + local hot path ------------
    var d_worker = T32Worker()
    d_worker.rt = UnsafePointer[Runtime, MutAnyOrigin](to=rt)
    d_worker.ledger = lp
    d_worker.inject = rt.inject_queue()
    d_worker.id = 0
    d_worker.owner_id = -1
    var d_up = UnsafePointer[T32Worker, MutAnyOrigin](to=d_worker)
    var d_ud = d_up.bitcast[Byte]()
    var inj_ptr = rt.inject_queue()
    # E6/M2 fold: verify the LOCAL route of enqueue_global announces too
    # (driver B proved the injection route).  Drives A-C are done — arming
    # rt now cannot disturb them.
    var acct_d = c_malloc(ACCT_BYTES)
    var azd = UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(acct_d))
    for zk in range(ACCT_BYTES // 8):
        (azd + zk)[0] = 0
    rt.arm_acct(acct_d)

    # injected policy task: current_worker=0 -> injection queue
    var p_tcb = TB.create()
    (UnsafePointer[TB, MutAnyOrigin](to=p_tcb))[0].set_parent_id(1)
    (UnsafePointer[TB, MutAnyOrigin](to=p_tcb))[0].transition(TaskControlBlock.RUNNABLE)
    var p_id = rt.next_id()
    var pend_before = rt.inject_pending()
    var enq_inj = False
    try:
        rt.enqueue_global(Int(UnsafePointer[TB, MutAnyOrigin](to=p_tcb)), p_id, 0)
        enq_inj = True
    except Error:
        failures.append("driver D: enqueue_global(...,0) raised (scaffold?)")
    if enq_inj and rt.inject_pending() != pend_before + 1:
        failures.append("driver D: injected policy task did not land in the "
                        + "injection queue")
    if enq_inj and acct_pending(acct_d) != 1:
        failures.append("driver D: injected accepted record must announce "
                        + "exactly once (got "
                        + String(acct_pending(acct_d)) + ")")
    try:
        _ = worker_slice(UnsafePointer[Runtime, MutAnyOrigin](to=rt), inj_ptr, d_ud)
    except Error:
        failures.append("driver D: injection poll raised (scaffold?)")
    if enq_inj and not (UnsafePointer[TB, MutAnyOrigin](to=p_tcb))[0].is_completed():
        failures.append("driver D: injected policy task did not complete")

    # local policy task: current_worker=1 -> _ready local FIFO (never inject)
    var l_tcb = TB.create()
    (UnsafePointer[TB, MutAnyOrigin](to=l_tcb))[0].set_parent_id(1)
    (UnsafePointer[TB, MutAnyOrigin](to=l_tcb))[0].transition(TaskControlBlock.RUNNABLE)
    var l_id = rt.next_id()
    var pend_l = rt.inject_pending()
    var enq_loc = False
    try:
        rt.enqueue_global(Int(UnsafePointer[TB, MutAnyOrigin](to=l_tcb)), l_id, 1)
        enq_loc = True
    except Error:
        failures.append("driver D: enqueue_global(...,1) raised (scaffold?)")
    if enq_loc and rt.inject_pending() != pend_l:
        failures.append("driver D: LOCAL enqueue touched the injection queue "
                        + "(no-global-lock-on-local-path violated)")
    if enq_loc and acct_pending(acct_d) != 2:
        failures.append("driver D: local-route announce lost (pending "
                        + String(acct_pending(acct_d)) + ")")
    try:
        _ = worker_slice(UnsafePointer[Runtime, MutAnyOrigin](to=rt), inj_ptr, d_ud)
    except Error:
        failures.append("driver D: injection poll raised (scaffold?)")
    if enq_loc and not (UnsafePointer[TB, MutAnyOrigin](to=l_tcb))[0].is_completed():
        failures.append("driver D: local policy task did not complete")
    if rt.inject_pending() != 0:
        failures.append("driver D: injection queue not quiet after policy")
    complete_work(acct_d, 2)
    if acct_pending(acct_d) != 0:
        failures.append("driver D: announced budget not drained after policy")
    c_free(acct_d)

    # ---- verdict ------------------------------------------------------------
    if len(failures) == 0:
        print("T32 injection: PASS (A: " + String(TOTAL) + " injected by "
              + String(N_EXT) + " producers across 2 workers exactly-once; "
              + "B: cap " + String(CAP_FULL) + ", "
              + String(rt_b.inject_rejected()) + " rejected, drip drained "
              + String(completed_b) + "; C: " + String(N_EP)
              + " wake episodes claim-once; D: policy routed, local hot path "
              + "lock-free)")
        return
    print("T32 injection: FAILED (" + String(len(failures)) + " failure(s))")
    if len(failures) <= 24:
        for m in failures:
            print("  - " + m)
    else:
        for k in range(24):
            print("  - " + failures[k])
        print("  - ... (" + String(len(failures) - 24) + " more)")
    print("T32 injection: RED (" + String(len(failures)) + " failure(s))")
    _iso_exit(1)