# mojito_async/test/unit/t34c_duplicate_wake_aot.mojo
#
# H2 (PR #109) — RACING DUPLICATE WAKES ARE QUIET NO-OPS: TDD red->green
# driver for the unpark_current duplicate/stale-claim fold.
#
# The driver races TWO waker threads (A and B) against the SAME epoch — the
# ep1 generation captured at the WAITING commit — and proves that a losing
# duplicate claim is a QUIET NO-OP in EVERY task state:
#
#   P1 CONCURRENT SAME-EPOCH CLAIMS: A and B both observe the WAITING park
#     (ep1, generation 2) and both call unpark_current(required_gen=2) at
#     once.  Under the owner's remote-ready queue guard exactly ONE claims
#     (wake_claim + one enqueue); the loser must return quietly whatever it
#     observes (RUNNABLE right after the claim, RUNNING mid-resume, or a
#     later epoch) — never a raise, never a second enqueue.  Both wakers
#     rendezvous on wA_pending/wB_pending (each sets its own flag after
#     observing WAITING, then spins on the other's) BEFORE either calls
#     unpark_current (issue #176): a plain poll-and-sleep race here let the
#     loser's ~100us blind spot miss the whole PH_PARKED1 window when the
#     winner + owner redispatch raced through it first, especially under
#     host contention — a driver-harness timing assumption, not a
#     park.mojo/unpark_current race (see the git history for the full
#     root-cause writeup this comment summarizes).
#   P2 MID-RUNNING DUPLICATE (deterministic window): while the owner runs
#     the ep2 slice (task RUNNING, generation still 2, ep1 already claimed),
#     B delivers the ep1 duplicate (required_gen=2).  It must NOT latch the
#     early-wake readiness: latching would fabricate a PHANTOM early wake
#     that corrupts the ep2 park (VALIDATE READY with no wake, WAITING never
#     entered).  The owner spins until B's probe completes BEFORE park_
#     prepare, so the probe ALWAYS lands in the RUNNING window.
#   P3 STALE-WAITING DUPLICATE: while the task waits ep2 (generation 3), B
#     redelivers the ep1 epoch (required_gen=2) — rejected by the fresh-
#     generation guard, nothing enqueued, still WAITING.
#   P4 POST-COMPLETION DUPLICATE (deterministic window): after the task
#     COMPLETES, B redelivers the ep1 epoch (required_gen=2).  This is the
#     H2 bug: the pre-fold code raised IllegalTransitionError (COMPLETED ->
#     RUNNABLE is illegal) under the racing duplicate; the fold makes it a
#     QUIET NO-OP (no raise, no enqueue).  The driver asserts no raise and
#     no thread error.
#
# Exactly-once resume: ep1 claimed ONCE (one owner resume), ep2 claimed
# ONCE (one owner resume) — paused into WAITING exactly twice, generation 1
# -> 2 -> 3, and every duplicate/stale probe enqueues NOTHING (skipped() ==
# 0, quiet queues).
#
# Choreography: worker 0 runs the REAL scheduler_loop (the owner, driving
# the task across its three slices); workers A and B are the two FOREIGN
# WAKERS, each with its OWN Runtime, delivering wakes through THAT runtime
# (the wake must route to the OWNER's remote-ready queue).  Raw pthread
# harness (t33/t34 pattern; AOT only — modular/modular#6971).
#
# BUILD REQUIREMENT (H4-partial/M10, PR #109): this driver MUST be built at
# `mojo build -O 0`.  Its cross-thread handshake cells (phase, dup2_done,
# wake1_done, parked_*) are PLAIN Ints published with release/acquire fences
# (see the per-writer fence[Ordering.RELEASE] and the per-reader
# fence[Ordering.ACQUIRE] annotation below); a higher optimization level can
# hoist the plain handshake reads and deadlock or desynchronize the driver.
# mojito_async/test/run.sh carries the -O 0 build flag for this driver.
#
# Verdict: exit 0 + "PASS"; any failure prints FAIL + _iso_exit(1).
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


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime TB = TaskControlBlock[IntResult]
# Per-TCB heap stride (generous; b2 has no sizeof — cells are addressed
# individually via tcb pointers).
comptime TCB_STRIDE = Int(256)

# Phase machine (all threads write; main's spin reads via budget&1 and the
# wakers' phase reads are followed by an ACQUIRE fence — M10 handshake
# discipline).
comptime PH_RUN = Int(0)
comptime PH_PARKED1 = Int(1)   # ep1 committed WAITING (gen 2)
comptime PH_RUNNING2 = Int(2)  # owner is INSIDE the ep2 slice, task RUNNING
comptime PH_PARKED2 = Int(3)   # ep2 committed WAITING (gen 3)
comptime PH_RESUME2 = Int(4)   # waker A claimed ep2
comptime PH_DONE = Int(5)      # task COMPLETED
comptime PH_ERR = Int(6)

comptime W0_ID = Int(1)  # the driver's owner worker identity
comptime WA_ID = Int(2)  # waker A identity
comptime WB_ID = Int(3)  # waker B identity


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
    var wA: UnsafePointer[Worker, MutAnyOrigin]
    var wB: UnsafePointer[Worker, MutAnyOrigin]
    var phase: UnsafePointer[Int, MutAnyOrigin]        # 2-cell array
    var failures: UnsafePointer[Int, MutAnyOrigin]
    var thread_err: UnsafePointer[Int, MutAnyOrigin]   # 3-cell array
    var parked_tcb: UnsafePointer[Int, MutAnyOrigin]
    var parked_id: UnsafePointer[Int, MutAnyOrigin]
    var parked_gen: UnsafePointer[Int, MutAnyOrigin]
    var ep1_gen: UnsafePointer[Int, MutAnyOrigin]      # B's captured ep1 epoch
    var slices: UnsafePointer[Int, MutAnyOrigin]       # dispatch count
    var body_entries: UnsafePointer[Int, MutAnyOrigin]
    var resumes: UnsafePointer[Int, MutAnyOrigin]      # owner post-wake slices
    var committed_waiting: UnsafePointer[Int, MutAnyOrigin]
    var wake1_done: UnsafePointer[Int, MutAnyOrigin]   # ep1 wake calls made
    var dup2_done: UnsafePointer[Int, MutAnyOrigin]    # P2 probe completed
    var dup_raised: UnsafePointer[Int, MutAnyOrigin]   # P4 probe raised?
    var wA_observed: UnsafePointer[Int, MutAnyOrigin]
    var wA_pending: UnsafePointer[Int, MutAnyOrigin]
    var wB_pending: UnsafePointer[Int, MutAnyOrigin]
    var progress: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]  # 3 slots

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.wA = self.w0
        self.wB = self.w0
        self.phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.failures = self.phase
        self.thread_err = self.phase
        self.parked_tcb = self.phase
        self.parked_id = self.phase
        self.parked_gen = self.phase
        self.ep1_gen = self.phase
        self.slices = self.phase
        self.body_entries = self.phase
        self.resumes = self.phase
        self.committed_waiting = self.phase
        self.wake1_done = self.phase
        self.dup2_done = self.phase
        self.dup_raised = self.phase
        self.wA_observed = self.phase
        self.wA_pending = self.phase
        self.wB_pending = self.phase
        self.progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
            unsafe_from_address=1
        )


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
    if not h.tcb()[].is_started():
        # FIRST dispatch (scheduler_loop already stamped owner).
        if h.tcb()[].owner_worker() != W0_ID:
            _fail(sc[].failures, "owner_worker not stamped at first run")
        claim_running(h)
        _ = parker_body(ud)
        _ = _park_two_phase(h, sc)
        sc[].parked_tcb[] = tcb_addr
        sc[].parked_id[] = tid
        sc[].parked_gen[] = h.tcb()[].generation()  # 2
        set_phase(sc[].phase, PH_PARKED1)
        return 1
    sc[].resumes[] += 1
    claim_running(h)
    _ = parker_body(ud)
    if sc[].resumes[] == 1:
        # SECOND dispatch: park episode 2 (fresh epoch).  P2: announce the
        # RUNNING window and SPIN until waker B's mid-RUNNING duplicate probe
        # completed — the probe therefore ALWAYS lands while the task is
        # RUNNING (generation still 2, ep1 already claimed).
        set_phase(sc[].phase, PH_RUNNING2)
        var spins = 0
        while sc[].dup2_done[] == 0:
            _ = sc[].progress[0].fetch_add[ordering=Ordering.RELAXED](Int64(1))
            spins += 1
            if spins > 4000000:
                _fail(sc[].failures, "w0: B never ran the mid-RUNNING probe")
                set_phase(sc[].phase, PH_ERR)
                return 1
            sleep(0.00001)
        fence[Ordering.ACQUIRE]()  # M10: acquire the dup2_done publication
        _ = _park_two_phase(h, sc)
        sc[].parked_tcb[] = tcb_addr
        sc[].parked_id[] = tid
        sc[].parked_gen[] = h.tcb()[].generation()  # 3
        set_phase(sc[].phase, PH_PARKED2)
        return 1
    # THIRD dispatch: complete.
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    h.tcb()[].mark_result(IntResult(7))
    set_phase(sc[].phase, PH_DONE)
    return 1


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
        if ph == PH_DONE or ph == PH_ERR:
            return
        _ = scheduler_loop(rt[], dispatch_w0, scp.bitcast[Byte](), worker_id=W0_ID)
        fence[Ordering.RELEASE]()  # publish every dispatcher write (M10)
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "w0: serve loop did not quiesce")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


# ---------------------------------------------------------------------------
# Waker A: claims ep1 (one of the two racing claimants), claims ep2.
# ---------------------------------------------------------------------------

def serve_wakerA(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rtA = sc.wA[].runtime()
    var spins = 0
    var a_ep1 = 0
    var a_ep2 = 0
    while True:
        _ = sc.progress[1].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        var ph = sc.phase[1]
        if ph == PH_ERR or ph == PH_DONE:
            return
        if ph == PH_PARKED1 and a_ep1 == 0:
            fence[Ordering.ACQUIRE]()  # M10: acquire parked_*/phase publication
            var h = _waker_handle(scp)
            if h.state() != TaskControlBlock.WAITING:
                _fail(sc.failures, "wakerA: ep1 parker not WAITING (state "
                      + String(h.state()) + ")")
            sc.wA_observed[] += 1
            sc.wA_pending[] = 1
            fence[Ordering.RELEASE]()  # M10: publish the arrival
            # #176 RENDEZVOUS: block the race until B has ALSO observed the
            # ep1 WAITING park.  Without this, a poll-interval blind spot
            # (this loop only re-checks `phase` every ~100us) lets B win the
            # claim, get requeued, and have the owner race clean through
            # PH_PARKED1 -> PH_RUNNING2 -> PH_PARKED2 before A's NEXT poll —
            # A then falls straight into the PH_PARKED2 branch below,
            # skipping ep1 entirely (wake1_done stuck at 1, wA_observed
            # stuck at 0).  The rendezvous makes "both wakers observed the
            # WAITING park before either claims" an invariant instead of a
            # timing assumption: nobody can call unpark_current, so the
            # owner cannot be requeued, so phase cannot leave PH_PARKED1,
            # until BOTH pending flags are up.
            var bspins = 0
            while sc.wB_pending[] == 0:
                bspins += 1
                if bspins > 2000000:
                    _fail(sc.failures, "wakerA: B never arrived at the ep1 rendezvous")
                    set_phase(sc.phase, PH_ERR)
                    return
                sleep(0.00001)
            fence[Ordering.ACQUIRE]()  # M10: acquire B's arrival
            # P1: the same-epoch claim as waker B — exactly one of the two
            # wins; the loser's call is a quiet no-op.
            unpark_current(rtA[], h, required_gen=sc.parked_gen[])
            a_ep1 = 1
            sc.wake1_done[] += 1
            fence[Ordering.RELEASE]()
            continue
        if ph == PH_PARKED2 and a_ep2 == 0:
            fence[Ordering.ACQUIRE]()  # M10
            var h = _waker_handle(scp)
            # the ep2 claim (a FRESH epoch — generation 3): exactly once.
            unpark_current(rtA[], h, required_gen=sc.parked_gen[])
            a_ep2 = 1
            set_phase(sc.phase, PH_RESUME2)
            fence[Ordering.RELEASE]()
            continue
        fence[Ordering.RELEASE]()
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "wakerA: never observed the park choreography")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


# ---------------------------------------------------------------------------
# Waker B: the RACING DUPLICATE — same-epoch claim as A, then the three
# deterministic duplicate probes (P2 mid-RUNNING, P3 stale-WAITING,
# P4 post-COMPLETION).  Every probe must be a quiet no-op (H2).
# ---------------------------------------------------------------------------

def serve_wakerB(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rtB = sc.wB[].runtime()
    var spins = 0
    var b_ep1 = 0
    var b_p2 = 0
    var b_p3 = 0
    while True:
        _ = sc.progress[2].fetch_add[ordering=Ordering.RELAXED](Int64(1))
        var ph = sc.phase[1]
        if ph == PH_ERR or ph == PH_DONE:
            return
        if ph == PH_PARKED1 and b_ep1 == 0:
            fence[Ordering.ACQUIRE]()  # M10
            sc.ep1_gen[] = sc.parked_gen[]  # remember ep1's epoch for P2-P4
            sc.wB_pending[] = 1
            fence[Ordering.RELEASE]()  # M10: publish the arrival
            # #176 RENDEZVOUS: mirror of waker A's — block the race until A
            # has ALSO observed the ep1 WAITING park (see the long comment
            # there for the poll-blind-spot this closes).
            var aspins = 0
            while sc.wA_pending[] == 0:
                aspins += 1
                if aspins > 2000000:
                    _fail(sc.failures, "wakerB: A never arrived at the ep1 rendezvous")
                    set_phase(sc.phase, PH_ERR)
                    return
                sleep(0.00001)
            fence[Ordering.ACQUIRE]()  # M10: acquire A's arrival
            # P1: the RACING duplicate claim of ep1's epoch — same
            # required_gen as waker A; exactly one of the two claims.
            unpark_current(rtB[], _waker_handle(scp), required_gen=sc.parked_gen[])
            b_ep1 = 1
            sc.wake1_done[] += 1
            fence[Ordering.RELEASE]()
            continue
        if ph == PH_RUNNING2 and b_p2 == 0:
            fence[Ordering.ACQUIRE]()  # M10
            # P2: duplicate of ep1's epoch landing MID-RUNNING (the owner
            # spins on dup2_done before park_prepare).  Must NOT latch the
            # early-wake readiness (the ep1 epoch is already claimed) and
            # must NOT raise.
            try:
                unpark_current(rtB[], _waker_handle(scp), required_gen=sc.ep1_gen[])
            except e:
                sc.dup_raised[] += 1
                _fail(sc.failures, "P2: mid-RUNNING duplicate wake raised")
            sc.dup2_done[] = 1
            fence[Ordering.RELEASE]()
            continue
        if ph == PH_PARKED2 and b_p3 == 0:
            fence[Ordering.ACQUIRE]()  # M10
            # P3: duplicate of ep1's epoch while the task waits ep2 (gen 3)
            # — the fresh-generation guard rejects it (still WAITING,
            # nothing enqueued); must not raise.
            try:
                unpark_current(rtB[], _waker_handle(scp), required_gen=sc.ep1_gen[])
            except e:
                sc.dup_raised[] += 1
                _fail(sc.failures, "P3: stale-WAITING duplicate wake raised")
            b_p3 = 1
            fence[Ordering.RELEASE]()
            continue
        fence[Ordering.RELEASE]()
        spins += 1
        if spins > 2000000:
            _fail(sc.failures, "wakerB: never observed the park choreography")
            set_phase(sc.phase, PH_ERR)
            return
        sleep(0.0001)


def _b_post_done(sc: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """P4 — the post-COMPLETION duplicate probe, issued by B once the task
    is COMPLETED (PH_DONE, observed by the caller under the ACQUIRE
    fence).  Must be a QUIET NO-OP: no raise, no enqueue."""
    unpark_current(
        sc[].wB[].runtime()[], _waker_handle(sc), required_gen=sc[].ep1_gen[]
    )


def _waker_handle(sc: UnsafePointer[Scene, MutAnyOrigin]) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](
            unsafe_from_address=sc[].parked_tcb[]
        ),
        sc[].parked_id[],
    )


@export("t34c_worker0")
def t34c_worker0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_worker0(sc)
    except e:
        sc[].thread_err[0] = 1
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 0)


@export("t34c_wakerA")
def t34c_wakerA(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_wakerA(sc)
    except e:
        sc[].thread_err[1] = 1
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 1)


@export("t34c_wakerB")
def t34c_wakerB(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_wakerB(sc)
        # P4: the post-COMPLETION duplicate — must not raise (H2).
        var spins = 0
        while sc[].phase[1] != PH_DONE:
            _ = sc[].progress[2].fetch_add[ordering=Ordering.RELAXED](Int64(1))
            spins += 1
            if spins > 4000000:
                _fail(sc[].failures, "wakerB: task never COMPLETED for P4")
                set_phase(sc[].phase, PH_ERR)
                return
            sleep(0.00001)
        fence[Ordering.ACQUIRE]()  # M10: acquire the DONE publication
        _b_post_done(sc)
    except e:
        sc[].thread_err[2] = 1
        sc[].dup_raised[] += 1
        _fail(sc[].failures, "P4: POST-COMPLETION duplicate wake raised")
        set_phase(sc[].phase, PH_ERR)
    _park_forever(sc, 2)


def _park_forever(sc: UnsafePointer[Scene, MutAnyOrigin], me: Int) -> None:
    """E6 handoff surrogate (t33): a foreign thread that RETURNS crashes the
    b2 runtime, so after the serve loop the worker spins (OBSERVABLE per-
    iteration progress) until main _iso_exits microseconds later."""
    while True:
        _ = sc[].progress[me].fetch_add[ordering=Ordering.RELAXED](Int64(1))


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

def run_scenario(failures: UnsafePointer[Int, MutAnyOrigin]) raises:
    var w0 = make_worker()
    var wA = make_worker()
    var wB = make_worker()
    var w0p = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    var wAp = UnsafePointer[Worker, MutAnyOrigin](to=wA)
    var wBp = UnsafePointer[Worker, MutAnyOrigin](to=wB)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = w0p
    sc[].wA = wAp
    sc[].wB = wBp
    var cells = Int(c_malloc(20 * 8 + 3 * 64))
    var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(20):
        p[i] = 0
    sc[].phase = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 0)
    sc[].failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 2 * 8)
    sc[].thread_err = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 3 * 8
    )
    sc[].parked_tcb = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 6 * 8
    )
    sc[].parked_id = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 7 * 8
    )
    sc[].parked_gen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 8 * 8
    )
    sc[].ep1_gen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 9 * 8
    )
    sc[].slices = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 10 * 8
    )
    sc[].body_entries = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 11 * 8
    )
    sc[].resumes = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 12 * 8
    )
    sc[].committed_waiting = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 13 * 8
    )
    sc[].wake1_done = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 14 * 8
    )
    sc[].dup2_done = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 15 * 8
    )
    sc[].dup_raised = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 16 * 8
    )
    sc[].wA_observed = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 17 * 8
    )
    sc[].wA_pending = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 18 * 8
    )
    sc[].wB_pending = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=cells + 19 * 8
    )
    sc[].progress = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=cells + 20 * 8
    )
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
    var tA = Int(0)
    var tB = Int(0)
    var t0p = UnsafePointer[Int, MutAnyOrigin](to=t0)
    var tAp = UnsafePointer[Int, MutAnyOrigin](to=tA)
    var tBp = UnsafePointer[Int, MutAnyOrigin](to=tB)
    var rc0 = _pthread_create(t0p, 0, entry_pointer["t34c_worker0"](), arg)
    var rcA = _pthread_create(tAp, 0, entry_pointer["t34c_wakerA"](), arg)
    var rcB = _pthread_create(tBp, 0, entry_pointer["t34c_wakerB"](), arg)
    if rc0 != 0 or rcA != 0 or rcB != 0:
        _fail(failures, "pthread_create failed (" + String(rc0) + ", "
                        + String(rcA) + ", " + String(rcB) + ")")
        # issue #138 precedent (applied here for #176): do NOT free `scp`/
        # `tcb` — a partial pthread_create failure (rc0==0 but rcA/rcB!=0,
        # or any other partial combination) can leave an already-started
        # thread spinning in _park_forever, which dereferences `sc[].
        # progress[me]` forever; freeing scp out from under it is a
        # use-after-free.  `main()`'s `_iso_exit` reclaims everything.
        return
    var budget = 600_000_000
    while budget > 0:
        # address-dependent load (index derives from `budget`) defeats b2's
        # hoisting of cross-thread plain reads (t33 probe); the -O 0 build
        # (H4-partial) guarantees it.
        var s0 = sc[].phase[budget & 1]
        var s1 = sc[].phase[1 - (budget & 1)]
        var done = s0 == PH_DONE or s0 == PH_ERR or s1 == PH_ERR or s1 == PH_DONE
        if done:
            break
        budget -= 1
    fence[Ordering.ACQUIRE]()
    if budget == 0:
        _fail(failures, "workers did not quiesce (phase "
                        + String(sc[].phase[0]) + " err "
                        + String(sc[].thread_err[0]) + ","
                        + String(sc[].thread_err[1]) + ","
                        + String(sc[].thread_err[2]) + ")")

    # ---- assertions ----
    if failures[0] == 0:
        if sc[].thread_err[0] != 0 or sc[].thread_err[1] != 0 or sc[].thread_err[2] != 0:
            _fail(failures, "a worker thread raised in its serve loop (err "
                  + String(sc[].thread_err[0]) + ","
                  + String(sc[].thread_err[1]) + ","
                  + String(sc[].thread_err[2]) + ")")
        if sc[].dup_raised[] != 0:
            _fail(failures, "a duplicate/stale wake probe RAISED (H2: must be "
                  + "a quiet no-op; dup_raised=" + String(sc[].dup_raised[]) + ")")
        if sc[].wake1_done[] != 2:
            _fail(failures, "expected BOTH wakers to issue the ep1 claim (got "
                  + String(sc[].wake1_done[]) + ")")
        if sc[].wA_observed[] != 1:
            _fail(failures, "wakerA never observed the ep1 WAITING park")
        if sc[].committed_waiting[] != 2:
            _fail(failures, "expected 2 WAITING commits (got "
                  + String(sc[].committed_waiting[]) + ")")
        if sc[].body_entries[] != 3:
            _fail(failures, "body ran " + String(sc[].body_entries[])
                  + " times (want 3: park1 + park2 + complete)")
        if sc[].resumes[] != 2:
            _fail(failures, "owner resumes = " + String(sc[].resumes[])
                  + " (want 2: EXACTLY ONCE per wake claim)")
        if tcb[].generation() != 3:
            _fail(failures, "generation = " + String(tcb[].generation())
                  + " (want 3: two WAITING bumps)")
        if sc[].parked_gen[] != 3:
            _fail(failures, "ep2 parked_gen = " + String(sc[].parked_gen[])
                  + " (want 3)")
        if not tcb[].is_completed():
            _fail(failures, "parker not COMPLETED")
        if tcb[].owner_worker() != W0_ID:
            _fail(failures, "owner_worker = " + String(tcb[].owner_worker())
                  + " (want " + String(W0_ID) + ")")
        var w0pending = w0p[].runtime()[].pending()
        if w0pending != 0:
            _fail(failures, "w0 not quiet after the wakes (pending "
                  + String(w0pending) + ")")
        var skipped = w0p[].runtime()[].skipped()
        if skipped != 0:
            _fail(failures, "w0 skipped " + String(skipped)
                  + " stale records (duplicate/stale wakes must enqueue "
                  + "NOTHING)")
        var wApending = wAp[].runtime()[].pending()
        if wApending != 0:
            _fail(failures, "wakerA enqueued the wake locally (pending "
                  + String(wApending)
                  + " — the wake must route to the OWNER remote queue)")
        var wBpending = wBp[].runtime()[].pending()
        if wBpending != 0:
            _fail(failures, "wakerB enqueued the wake locally (pending "
                  + String(wBpending)
                  + " — the wake must route to the OWNER remote queue)")

    # issue #138 precedent (t34_two_phase_aot.mojo), applied here for #176:
    # `scp` (this Scene's backing cell) and `tcb` must NOT be freed.
    # t34c_worker0/t34c_wakerA/t34c_wakerB (spawned above) NEVER return —
    # `_park_forever` spins on `sc[].progress[me]` FOREVER after each
    # serve loop quiesces, because a foreign pthread that actually returns
    # crashes the b2 1.0.0b2 runtime (the exact constraint `_park_forever`
    # itself documents).  Freeing `scp` here raced these still-spinning
    # zombie threads against `c_malloc`/`c_free` reusing the same address
    # for a later allocation — reproduced as a SIGSEGV in a still-running
    # zombie thread, standalone, with NO host contention required (3/100
    # in one local run) — the same failure class #138 already root-caused
    # and fixed in the sibling t34 driver, just never carried over here.
    # `cells` (the Scene's field-backing data block, allocated above) was
    # ALREADY never freed for the identical reason; `scp`/`tcb` now match
    # it — every allocation a zombie thread can still reach is
    # intentionally leaked for the remaining life of the process, which
    # `main()`'s final `_iso_exit` reclaims in one shot.


def main() raises:
    var failures = Int(0)
    var failuresp = UnsafePointer[Int, MutAnyOrigin](to=failures)
    run_scenario(failuresp)
    if failures == 0:
        print("T34c duplicate wake (H2, PR #109): PASS")
        _iso_exit(0)
    print("T34c duplicate wake (H2, PR #109): FAIL (" + String(failures) + ")")
    _iso_exit(1)