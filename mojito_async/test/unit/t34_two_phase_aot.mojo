# mojito_async/test/unit/t34_two_phase_aot.mojo
#
# A2.5 (issue #71) — TWO-PHASE PARK (PREPARE/VALIDATE/COMMIT) + cross-worker
# wake: TDD red->green driver.
#
# Proves the issue #71 exit criteria on TWO REAL worker OS threads:
#
#   S1 THE CROSS-WORKER WAKE (A0-T12 — generation claim EXACTLY ONCE):
#     a task is seeded on worker 0 (owner, driven through the REAL
#     scheduler_loop with worker_id=1, which stamps owner_worker +
#     owner_runtime at first dispatch).  Its slices:
#       slice 1 — body + two-phase park (park_prepare/park_validate/
#                 park_commit) -> WAITING, generation 2 (episode 1);
#       slice 2 — resumed by the cross-worker wake -> body parks AGAIN
#                 (episode 2) -> WAITING, generation 3;
#       slice 3 — resumed -> COMPLETED.
#     Worker 1 (the FOREIGN WAKER, its OWN Runtime):
#       ep1: wakes with required_gen=2 (claims EXACTLY once) + a duplicate
#            wake of the same epoch (no-op — nothing double-enqueued);
#       ep2: wakes with required_gen=2 (episode 1's STALE generation) —
#            REJECTED by the fresh-generation guard (still WAITING, nothing
#            enqueued), then wakes with required_gen=3 (claims).
#     Every wake is delivered to the OWNER's remote-ready queue (spec
#     §19.2 — never injection, never a steal candidate); the owner resumes
#     exactly once per claim; the stale/duplicate wakes enqueue nothing
#     (final skipped() == 0 and exactly 2 owner resumes).
#
#   S2 THE LOST-WAKEUP WINDOW (A0-T11 — PREPARE then VALIDATE re-check):
#     the wake is delivered BETWEEN PREPARE and COMMIT.  The waker's
#     unpark_current latches readiness (no claim, no enqueue — the task is
#     RUNNING/PARKING); park_validate() returns READY; park_commit unwinds
#     PARKING -> RUNNABLE — WAITING is NEVER entered and the wait generation
#     is NEVER bumped (stays 1).  The task completes in its FIRST slice
#     (never slept; body ran exactly once).
#
# Choreography: worker 0 runs the real scheduler drive loop over the real
# per-worker queues; worker 1 is the cross-worker wake producer.  Raw
# pthread harness (t33 pattern; the pool_worker_loop seam is E2-#68's and
# not wired to per-task dispatchers yet, so the driver threads serve
# directly).  AOT only (pthread externs; modular/modular#6971).
#
# Verdict: exit 0 + "PASS"; any failure prints FAIL + _iso_exit(1).
#
# BUILD REQUIREMENT (H4-partial/M10, PR #109): this driver MUST be built at
# `mojo build -O 0`.  Its cross-thread handshake cells (phase, wake_latched,
# parked_*) are PLAIN Ints published with release/acquire fences; a higher
# optimization level can hoist the plain handshake reads and deadlock or
# desynchronize the driver.  mojito_async/test/run.sh carries the -O 0
# build flag for this driver.  Live scenarios: BOTH S1 and S2 run in
# main() — S2 (the lost-wakeup window) is invoked, not dead code (H4).
from std.atomic import Atomic, fence, Ordering
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.park import (
    park_commit,
    park_prepare,
    park_validate,
    unpark_current,
)
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.task import JoinHandle, claim_running
from mojito_async.vendor.mojito_sys import c_malloc, entry_pointer


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Int,
    start_routine: UnsafePointer[Byte, MutAnyOrigin],
    arg: BytePtr,
) abi("C") -> Int32: ...


@extern("pthread_join")
def _pthread_join(thread: UInt, retval: UInt) abi("C") -> Int32: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]
# Per-TCB heap stride (generous; b2 has no sizeof — cells are addressed
# individually via tcb pointers).
comptime TCB_STRIDE = Int(256)

# Phase machine (both threads write; main's spin reads via budget&1 to
# defeat b2's cross-thread hoisting — t33 probe).
comptime PH_RUN = Int(0)
comptime PH_PARKED1 = Int(1)   # ep1 committed WAITING (gen 2)
comptime PH_RESUME1 = Int(2)   # waker woke ep1 (claims + dup probe done)
comptime PH_PARKED2 = Int(3)   # ep2 committed WAITING (gen 3)
comptime PH_RESUME2 = Int(4)   # waker: stale probe + current wake done
comptime PH_DONE = Int(5)
comptime PH_ERR = Int(6)
# S2 uses its own phase set:
comptime PH2_PREPARING = Int(7)  # parker announced its PREPARE window
comptime PH2_DONE = Int(8)

comptime W0_ID = Int(1)  # the driver's owner worker identity
comptime W1_ID = Int(2)  # the driver's waker worker identity


def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


def set_phase(sc: UnsafePointer[Int, MutAnyOrigin], v: Int):
    sc[0] = v
    sc[1] = v


# ---------------------------------------------------------------------------
# Shared scene
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]       # 2-cell array
    var failures: UnsafePointer[Int, MutAnyOrigin]
    var thread_err: UnsafePointer[Int, MutAnyOrigin]  # 2-cell array
    var progress: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]  # 2 slots
    var parked_tcb: UnsafePointer[Int, MutAnyOrigin]
    var parked_id: UnsafePointer[Int, MutAnyOrigin]
    var parked_gen: UnsafePointer[Int, MutAnyOrigin]
    var slices: UnsafePointer[Int, MutAnyOrigin]      # dispatch count
    var body_entries: UnsafePointer[Int, MutAnyOrigin]
    var resumes: UnsafePointer[Int, MutAnyOrigin]     # owner post-wake slices
    var ready_seen: UnsafePointer[Int, MutAnyOrigin]
    var committed_waiting: UnsafePointer[Int, MutAnyOrigin]
    var wake_latched: UnsafePointer[Int, MutAnyOrigin]  # S2 handshake
    var early: UnsafePointer[Int, MutAnyOrigin]       # 1 = run S2 shape
    var w1_observed: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.failures = self.phase
        self.thread_err = self.phase
        self.progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
            unsafe_from_address=1
        )
        self.parked_tcb = self.phase
        self.parked_id = self.phase
        self.parked_gen = self.phase
        self.slices = self.phase
        self.body_entries = self.phase
        self.resumes = self.phase
        self.ready_seen = self.phase
        self.committed_waiting = self.phase
        self.wake_latched = self.phase
        self.early = self.phase
        self.w1_observed = self.phase


# ---------------------------------------------------------------------------
# The parked task body: every slice reports + returns.
# ---------------------------------------------------------------------------

def parker_body(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[Scene]()
    sc[].body_entries[] += 1
    return IntResult(7)


def _park_two_phase(
    h: JoinHandle[IntResult],
    sc: UnsafePointer[Scene, MutAnyOrigin],
) raises -> Bool:
    """park_prepare / park_validate / park_commit; returns True when the
    park COMMITTED to WAITING (False = early wake unwound)."""
    park_prepare(h)
    if h.state() != TaskControlBlock.PARKING:
        _fail(sc[].failures, "park_prepare did not reach PARKING")
    var rdy = park_validate(h)
    if rdy:
        _fail(sc[].failures, "VALIDATE said READY with no wake delivered")
    park_commit(h, 3)  # SuspendReason.PARK
    if h.state() != TaskControlBlock.WAITING:
        _fail(sc[].failures, "park_commit did not reach WAITING")
    sc[].committed_waiting[] += 1
    return True


# ---------------------------------------------------------------------------
# Owner (worker 0) dispatcher — driven by the REAL scheduler_loop.
# ---------------------------------------------------------------------------

def dispatch_w0(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    if sc[].early[] == 0:
        # ---- S1: park ep1 -> wake -> park ep2 -> wake -> complete ----
        if not h.tcb()[].is_started():
            # FIRST dispatch (scheduler_loop already stamped owner).
            if h.tcb()[].owner_worker() != W0_ID:
                _fail(sc[].failures, "owner_worker not stamped at first run")
            claim_running(h)
            _ = parker_body(ud)
            _ = _park_two_phase(h, sc)
            sc[].parked_tcb[] = tcb_addr
            sc[].parked_id[] = tid
            sc[].parked_gen[] = h.tcb()[].generation()
            set_phase(sc[].phase, PH_PARKED1)
            return 1
        sc[].resumes[] += 1
        claim_running(h)
        _ = parker_body(ud)
        if sc[].resumes[] == 1:
            # SECOND dispatch: park episode 2 (fresh epoch).
            _ = _park_two_phase(h, sc)
            sc[].parked_tcb[] = tcb_addr
            sc[].parked_id[] = tid
            sc[].parked_gen[] = h.tcb()[].generation()
            set_phase(sc[].phase, PH_PARKED2)
            return 1
        # THIRD dispatch: complete.
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(IntResult(7))
        set_phase(sc[].phase, PH_DONE)
        return 1
    # ---- S2: one slice — body + two-phase park with a WAKE IN THE WINDOW
    if not h.tcb()[].is_started():
        claim_running(h)
        _ = parker_body(ud)
        park_prepare(h)
        # publish the waiter identity for the waker, then announce the open
        # window; wait for the waker's latch (the wake was delivered
        # between PREPARE and the VALIDATE re-check).
        sc[].parked_tcb[] = tcb_addr
        sc[].parked_id[] = tid
        set_phase(sc[].phase, PH2_PREPARING)
        var spins = 0
        while sc[].wake_latched[] == 0:
            _ = sc[].progress[0].fetch_add[ordering=Ordering.RELAXED](Int64(1))
            spins += 1
            if spins > 4000000:
                _fail(sc[].failures, "S2: waker never latched the wake")
                set_phase(sc[].phase, PH_ERR)
                return 1
            sleep(0.00001)
        fence[Ordering.ACQUIRE]()  # M10: acquire the waker's latch publication
        var rdy = park_validate(h)
        if not rdy:
            _fail(sc[].failures, "S2: VALIDATE missed the early wake (LOST WAKEUP)")
        sc[].ready_seen[] = 1
        park_commit(h, 3)  # unwinds PARKING -> RUNNABLE (no bump)
        if h.state() != TaskControlBlock.RUNNABLE:
            _fail(sc[].failures, "S2: commit did not unwind to RUNNABLE")
        if h.tcb()[].generation() != 1:
            _fail(sc[].failures, "S2: early wake BUMPED the generation")
        if sc[].committed_waiting[] != 0:
            _fail(sc[].failures, "S2: task entered WAITING despite early wake")
        # the task never slept: complete it in this slice.
        claim_running(h)  # RUNNABLE -> RUNNING
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(IntResult(7))
        set_phase(sc[].phase, PH2_DONE)
        return 1
    raise Error("t34 dispatch: unexpected started slice in S2")


# ---------------------------------------------------------------------------
# Worker 0 serve loop: the REAL scheduler_loop drive, polled to quiesce.
# ---------------------------------------------------------------------------

def serve_worker0(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w0[].runtime()
    var spins = 0
    while True:
        _ = sc.progress[0].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        var ph = sc.phase[0]
        if ph == PH_DONE or ph == PH2_DONE or ph == PH_ERR:
            return
        _ = scheduler_loop(rt[], dispatch_w0, scp.bitcast[Byte](), worker_id=W0_ID)
        fence[Ordering.RELEASE]()
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "w0: serve loop did not quiesce")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


# ---------------------------------------------------------------------------
# Worker 1 serve loop: the FOREIGN WAKER.
# ---------------------------------------------------------------------------

def serve_worker1(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var spins = 0
    var woke_ep1 = 0
    var woke_ep2 = 0
    while True:
        _ = sc.progress[1].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        var ph = sc.phase[1]
        if ph == PH_ERR or ph == PH_DONE or ph == PH2_DONE:
            return
        if sc.early[] == 1 and ph == PH2_PREPARING and sc.wake_latched[] == 0:
            # S2: the wake lands INSIDE the PREPARE window (task RUNNING/
            # PARKING): unpark must LATCH (no claim, no enqueue).
            var h = _waker_handle(scp)
            unpark_current(sc.w1[].runtime()[], h)
            sc.wake_latched[] = 1
            fence[Ordering.RELEASE]()
            continue
        if sc.early[] == 0 and ph == PH_PARKED1 and woke_ep1 == 0:
            var h = _waker_handle(scp)
            if h.state() != TaskControlBlock.WAITING:
                _fail(sc.failures, "waker: ep1 parker not WAITING")
            sc.w1_observed[] += 1
            # the cross-worker wake, claiming episode 1's epoch ONCE.
            unpark_current(sc.w1[].runtime()[], h, required_gen=sc.parked_gen[])
            # duplicate wake of the same epoch: no-op (already RUNNABLE).
            unpark_current(sc.w1[].runtime()[], h, required_gen=sc.parked_gen[])
            woke_ep1 = 1
            set_phase(sc.phase, PH_RESUME1)
            fence[Ordering.RELEASE]()
            continue
        if sc.early[] == 0 and ph == PH_PARKED2 and woke_ep2 == 0:
            var h = _waker_handle(scp)
            if h.state() != TaskControlBlock.WAITING:
                _fail(sc.failures, "waker: ep2 parker not WAITING")
            # STALE wake: episode 1's generation vs the current epoch —
            # the fresh-generation guard must REJECT it (still WAITING,
            # nothing enqueued).
            unpark_current(
                sc.w1[].runtime()[], h, required_gen=sc.parked_gen[] - 1
            )
            if h.state() != TaskControlBlock.WAITING:
                _fail(sc.failures, "STALE wake re-transitioned a WAITING task")
            # current wake: claims episode 2's epoch exactly once.
            unpark_current(sc.w1[].runtime()[], h, required_gen=sc.parked_gen[])
            woke_ep2 = 1
            set_phase(sc.phase, PH_RESUME2)
            fence[Ordering.RELEASE]()
            continue
        fence[Ordering.RELEASE]()
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "w1: never observed the park choreography")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


def _waker_handle(sc: UnsafePointer[Scene, MutAnyOrigin]) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](
            unsafe_from_address=sc[].parked_tcb[]
        ),
        sc[].parked_id[],
    )


@export("t34_worker0")
def t34_worker0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker0(sc)
    except e:
        sc[].thread_err[0] = 1
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 0)


@export("t34_worker1")
def t34_worker1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker1(sc)
    except e:
        sc[].thread_err[1] = 1
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 1)


def _park_forever(sc: UnsafePointer[Scene, MutAnyOrigin], me: Int) -> None:
    """E6 handoff surrogate (t33): a foreign thread that RETURNS crashes the
    b2 runtime, so after the serve loop the worker spins (OBSERVABLE per-
    iteration progress) until main _iso_exits microseconds later."""
    while True:
        _ = sc[].progress[me].fetch_add[ordering=Ordering.RELAXED](Int64(1))


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

def run_scenario(failures: UnsafePointer[Int, MutAnyOrigin], early: Int) raises:
    var w0 = make_worker()
    var w1 = make_worker()
    var w0p = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    var w1p = UnsafePointer[Worker, MutAnyOrigin](to=w1)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = w0p
    sc[].w1 = w1p
    var cells = Int(c_malloc(18 * 8 + 2 * 64))
    var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(18):
        p[i] = 0
    sc[].phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 0)
    sc[].failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 2 * 8)
    sc[].thread_err = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 3 * 8
    )
    sc[].parked_tcb = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 5 * 8
    )
    sc[].parked_id = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 6 * 8
    )
    sc[].parked_gen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 7 * 8
    )
    sc[].slices = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 8 * 8)
    sc[].body_entries = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 9 * 8
    )
    sc[].resumes = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 10 * 8
    )
    sc[].ready_seen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 11 * 8
    )
    sc[].committed_waiting = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 12 * 8
    )
    sc[].wake_latched = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 13 * 8
    )
    sc[].early = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 14 * 8)
    sc[].w1_observed = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 15 * 8
    )
    sc[].progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=cells + 18 * 8
    )
    sc[].early[] = early
    set_phase(sc[].phase, PH_RUN)

    # the parker task on worker 0 (fresh cell, RUNNABLE, on the local deque).
    var tcb = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(TCB_STRIDE))
    )
    tcb[0] = TB.create()
    tcb[].transition(TaskControlBlock.RUNNABLE)
    var w0rt = w0p[].runtime()
    var tid = w0rt[].next_id()
    w0rt[].enqueue_local(Int(tcb), tid)

    var scp = sc
    var arg = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(scp))
    var t0 = Int(0)
    var t1 = Int(0)
    var t0p = UnsafePointer[Int, MutAnyOrigin](to=t0)
    var t1p = UnsafePointer[Int, MutAnyOrigin](to=t1)
    var rc0 = _pthread_create(t0p, 0, entry_pointer["t34_worker0"](), arg)
    var rc1 = _pthread_create(t1p, 0, entry_pointer["t34_worker1"](), arg)
    if rc0 != 0 or rc1 != 0:
        _fail(failures, "pthread_create failed (" + String(rc0) + ", "
                        + String(rc1) + ")")
        # NOT freed: if EITHER pthread_create succeeded, that thread is
        # already running t34_worker0/t34_worker1 against `scp` — see the
        # issue #138 note at the bottom of this function for why `scp`/
        # `tcb`/`cells` are never freed by this driver.
        return
    var budget = 600_000_000
    while budget > 0:
        # address-dependent load (index derives from `budget`) defeats b2's
        # hoisting of cross-thread plain reads (t33 probe).
        var s0 = sc[].phase[budget & 1]
        var s1 = sc[].phase[1 - (budget & 1)]
        var done0 = s0 == PH_DONE or s0 == PH2_DONE or s0 == PH_ERR
        var done1 = s1 == PH_DONE or s1 == PH2_DONE or s1 == PH_ERR
        if done0 or done1:
            break
        budget -= 1
    fence[Ordering.ACQUIRE]()
    if budget == 0:
        _fail(failures, "workers did not quiesce (phase "
                        + String(sc[].phase[0]) + " err "
                        + String(sc[].thread_err[0]) + ","
                        + String(sc[].thread_err[1]) + ")")

    # ---- assertions ----
    if failures[0] == 0:
        if sc[].thread_err[0] != 0 or sc[].thread_err[1] != 0:
            _fail(failures, "a worker thread raised in its serve loop")
        if early == 0:
            # S1: two parks, two claims, exactly-once resumes, stale + dup
            # wakes rejected.
            if sc[].ready_seen[] != 0:
                _fail(failures, "S1: VALIDATE must not see READY")
            if sc[].committed_waiting[] != 2:
                _fail(failures, "S1: expected 2 WAITING commits (got "
                      + String(sc[].committed_waiting[]) + ")")
            if sc[].w1_observed[] != 1:
                _fail(failures, "S1: waker never observed a WAITING park")
            if sc[].body_entries[] != 3:
                _fail(failures, "S1: body ran " + String(sc[].body_entries[])
                      + " times (want 3: park1 + park2 + complete)")
            if sc[].resumes[] != 2:
                _fail(failures, "S1: owner resumes = " + String(sc[].resumes[])
                      + " (want 2: one per wake claim)")
            if tcb[].generation() != 3:
                _fail(failures, "S1: generation = " + String(tcb[].generation())
                      + " (want 3: two WAITING bumps)")
            if sc[].parked_gen[] != 3:
                _fail(failures, "S1: ep2 parked_gen = " + String(sc[].parked_gen[])
                      + " (want 3)")
            var skipped = w0p[].runtime()[].skipped()
            if skipped != 0:
                _fail(failures, "S1: " + String(skipped)
                      + " stale records skipped (duplicate/stale wakes must "
                      + "enqueue NOTHING)")
        else:
            # S2: the wake in the PREPARE window -> VALIDATE READY -> the
            # task never slept, never bumped, body ran exactly once.
            if sc[].ready_seen[] != 1:
                _fail(failures, "S2: VALIDATE did not catch the early wake")
            if sc[].committed_waiting[] != 0:
                _fail(failures, "S2: task entered WAITING despite early wake")
            if sc[].body_entries[] != 1:
                _fail(failures, "S2: body ran " + String(sc[].body_entries[])
                      + " times (want exactly 1: never slept)")
        if not tcb[].is_completed():
            _fail(failures, "parker not COMPLETED")
        if tcb[].owner_worker() != W0_ID:
            _fail(failures, "owner_worker = " + String(tcb[].owner_worker())
                  + " (want " + String(W0_ID) + ")")
        var w0pending = w0p[].runtime()[].pending()
        if w0pending != 0:
            _fail(failures, "w0 not quiet after the wake (pending "
                  + String(w0pending) + ")")
        var w1pending = w1p[].runtime()[].pending()
        if w1pending != 0:
            _fail(failures, "w1 enqueued the wake locally (pending "
                  + String(w1pending)
                  + " — the wake must route to the OWNER remote queue)")

    # issue #138 (teardown SIGSEGV, reproduced under host contention on a
    # pristine origin/main): `scp` (this Scene's backing cell) and `tcb`
    # must NOT be freed here.  t34_worker0/t34_worker1 (spawned above)
    # NEVER return — `_park_forever` (see its docstring) spins on
    # `sc[].progress[me]` FOREVER after their serve loop quiesces, because
    # a foreign pthread that actually returns crashes the b2 1.0.0b2
    # runtime (the same documented constraint `_park_forever` works
    # around).  `run_scenario` runs TWICE from main() (S2 then S1): if
    # this call's `scp`/`tcb` were freed here, S2's two zombie threads
    # would keep dereferencing THAT freed memory for the entire duration
    # of S1's run_scenario call — a genuine use-after-free race (S1's own
    # c_malloc calls request the SAME sizes and routinely get the SAME
    # freed addresses back from the allocator) that reproduces as a SIGSEGV
    # in a still-running t34_worker0/t34_worker1 zombie thread under host
    # contention, independent of optimization level (confirmed: still
    # crashes at -O 0).  `cells` (the Scene's field-backing data block,
    # allocated above) was ALREADY never freed for the identical reason;
    # `scp`/`tcb` now match it — every allocation a zombie thread can still
    # reach is intentionally leaked for the remaining life of the process,
    # which `main()`'s final `_iso_exit` reclaims in one shot.  Not a
    # WorkerPool issue — this driver drives raw pthreads directly and never
    # touches WorkerPool.join_all/finalize.


def main() raises:
    var failures_s2 = Int(0)
    var failures_s1 = Int(0)
    # S2 first (the lost-wakeup window, A0-T11) — LIVE per H4 (PR #109);
    # then S1 (the cross-worker wake + duplicate/stale rejection).  Each
    # scenario uses its own failure counter so both assertion blocks run
    # independently.
    var fp2 = UnsafePointer[Int, MutAnyOrigin](to=failures_s2)
    run_scenario(fp2, 1)  # S2: wake inside the PREPARE window
    var fp1 = UnsafePointer[Int, MutAnyOrigin](to=failures_s1)
    run_scenario(fp1, 0)  # S1: cross-worker wake (A0-T12)
    var failures = failures_s2 + failures_s1
    if failures != 0:
        print("T34 two-phase: FAIL (" + String(failures) + ")")
        _iso_exit(1)
    print("T34 two-phase (issue #71): PASS")
    _iso_exit(0)