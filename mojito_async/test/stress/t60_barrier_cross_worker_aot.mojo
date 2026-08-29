# mojito_async/test/stress/t60_barrier_cross_worker_aot.mojo
#
# RED driver for issue #148 — select, Condvar and Barrier never got the
# two-phase park and the guarded wait set.
#
# The tree's own history establishes that single-phase `park_current` plus an
# unguarded waiter deque is a lost wakeup AND a data race on M:N — that is
# why #55 (mutex), #122 (rwlock) and #137 (channels) exist.  Three primitives
# were skipped:
#
#     channel/select.mojo:691-694   registers in N channel FIFOs, then
#                                   park_current
#     sync/condvar.mojo:173-177     _w_tcb/_w_id are plain Deques, NO
#                                   SpinLock in the struct at all
#     sync/condvar.mojo:215         park_current
#     sync/barrier.mojo:153         park_current, same unguarded wait set
#
# `sync/condvar.mojo`'s own module header says why, and it is the honest kind
# of comment: "publish+park is atomic within a dispatcher slice".  That was
# true on the A1 single cooperative worker.  It stopped being true when the
# A2 M:N scheduler landed, and neither module was revisited.
#
# TWO DISTINCT FAILURES, both reachable from one scene.
#
# 1. DATA RACE ON THE WAIT SET.  `Barrier.wait` appends `(tcb, id)` to two
#    plain `Deque[Int]`s, and the releasing arrival `popleft`s from the same
#    two, with no guard anywhere.  On two real worker threads those are
#    concurrent mutations of the same storage: memory corruption, not merely
#    a lost wake.
#
# 2. LOST WAKEUP.  A release landing while the arriving task is still
#    RUNNING/PARKING latches `_early` via `unpark_current`, and
#    `park_current` (park.mojo:70-76) transitions PARKING -> WAITING WITHOUT
#    EVER CONSULTING THE LATCH.  The wake is gone, and the stale `_early`
#    then fabricates a phantom early wake for that task's NEXT two-phase
#    park.
#
# Barrier is the vehicle because it needs no mutex to reach the same
# `park_current` and the same unguarded FIFO shape `Condvar` and `select`
# use — condvar.mojo's header says the FIFO is "reused verbatim" by
# barrier.mojo, in that direction, so a race here is a race there.
#
# FAILURE ORACLE, not a hang.  The rendezvous runs in a FORKED CHILD, for the
# same reason t58's registry scene does: corrupting a Deque from two threads
# can abort the process inside the allocator before any in-process assertion
# is reached.  The parent decodes what killed the child.  Inside the child
# every drive loop is bounded, and a party that never completes is
# fingerprinted rather than waited on:
#
#     state WAITING and early_readiness() TRUE
#         -> park_current committed to WAITING with a wake already latched:
#            the lost-wakeup half, unambiguously
#     state WAITING, latch clear, and the wait set still non-empty after the
#     phase counter advanced
#         -> a release DID run and left a waiter queued: the unguarded-wait-set
#            half
#
# OBSERVED, 12 runs of 12: the unguarded-wait-set half, every time.  The
# latch half did NOT fire on this host, and neither did an allocator abort —
# the race manifests as lost and duplicated FIFO entries rather than as
# corruption libmalloc notices.  Both are recorded rather than assumed.
#
# BUILD LEVEL: `-O 0`, same as t38/t47/t50/t52 — the driver's own round
# handshake is plain Int cells and issue #143's LICM hoist eats them at
# default `-O`.
#
# Verdict: exit 0 + "PASS"; anything else prints RED and exits 1.
# AOT-only (pthread externs; modular/modular#6971).
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.sync.barrier import Barrier
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


@extern("fork")
def _c_fork() abi("C") -> Int32: ...


@extern("waitpid")
def _c_waitpid(
    pid: Int32, status: UnsafePointer[Int32, MutAnyOrigin], options: Int32
) abi("C") -> Int32: ...


comptime TB = TaskControlBlock[IntResult]

comptime ROUNDS = Int(3000)
comptime ROUND_SPIN_BUDGET = Int(200000)
comptime HANDSHAKE_BUDGET = Int(50000000)

comptime W0_ID = Int(1)
comptime W1_ID = Int(2)

comptime C_ERR0 = Int(0)
comptime C_ERR1 = Int(1)
comptime C_ABORT = Int(2)
comptime C_DONE0 = Int(3)
comptime C_DONE1 = Int(4)
comptime C_LOST_LANE = Int(5)     # 1 or 2 when a party never completed
comptime C_FP_STATE = Int(6)
comptime C_FP_EARLY = Int(7)
comptime C_FP_PHASE = Int(8)
comptime C_FP_WAITERS = Int(9)
comptime N_CELLS = Int(16)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var bar: UnsafePointer[Barrier, MutAnyOrigin]
    var c: UnsafePointer[Int, MutAnyOrigin]
    var cause0: UnsafePointer[Int, MutAnyOrigin]
    var cause1: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.bar = UnsafePointer[Barrier, MutAnyOrigin](unsafe_from_address=1)
        self.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.cause0 = self.c
        self.cause1 = self.c


def _dispatch(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr, lane: Int
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    var cause = sc[].cause0 if lane == 0 else sc[].cause1
    if sc[].bar[].wait[IntResult](rt, h, cause):
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def dispatch0(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    return _dispatch(rt, tcb_addr, tid, ud, 0)


def dispatch1(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    return _dispatch(rt, tcb_addr, tid, ud, 1)


def _drive_round[
    F: def(mut Runtime, Int, Int, BytePtr) raises -> Int
](
    scp: UnsafePointer[Scene, MutAnyOrigin],
    dispatcher: F,
    worker: UnsafePointer[Worker, MutAnyOrigin],
    worker_id: Int,
    lane: Int,
    rnd: Int,
) raises -> Bool:
    var sc = scp[]
    var rt = worker[].runtime()
    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    tcb.transition(TaskControlBlock.RUNNABLE)
    var tid = rnd + 1
    rt[].enqueue_local(Int(tcbp), tid)
    var h = JoinHandle[IntResult](tcbp, tid)
    if lane == 0:
        sc.cause0[] = 0
    else:
        sc.cause1[] = 0

    var spins = 0
    while not h.is_completed():
        _ = scheduler_loop(rt[], dispatcher, scp.bitcast[Byte](), worker_id=worker_id)
        spins += 1
        if spins > ROUND_SPIN_BUDGET:
            sc.c[C_LOST_LANE] = lane + 1
            sc.c[C_FP_STATE] = h.state()
            sc.c[C_FP_EARLY] = 1 if tcb.early_readiness() else 0
            sc.c[C_FP_PHASE] = sc.bar[].phase()
            sc.c[C_FP_WAITERS] = sc.bar[].waiter_count()
            return False
    return True


def serve0(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    for r in range(ROUNDS):
        if sc.c[C_ABORT] != 0:
            return
        if not _drive_round(scp, dispatch0, sc.w0, W0_ID, 0, r):
            sc.c[C_ABORT] = 1
            return
        sc.c[C_DONE0] = r + 1


def serve1(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    for r in range(ROUNDS):
        if sc.c[C_ABORT] != 0:
            return
        if not _drive_round(scp, dispatch1, sc.w1, W1_ID, 1, r):
            sc.c[C_ABORT] = 1
            return
        sc.c[C_DONE1] = r + 1


@export("t60_lane0")
def t60_lane0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve0(sc)
    except e:
        sc[].c[C_ERR0] = 1
        sc[].c[C_ABORT] = 1
        print("  lane 0 raised: " + String(e))


@export("t60_lane1")
def t60_lane1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve1(sc)
    except e:
        sc[].c[C_ERR1] = 1
        sc[].c[C_ABORT] = 1
        print("  lane 1 raised: " + String(e))


def _state_name(s: Int) -> String:
    if s == TaskControlBlock.RUNNABLE:
        return "RUNNABLE"
    if s == TaskControlBlock.RUNNING:
        return "RUNNING"
    if s == TaskControlBlock.PARKING:
        return "PARKING"
    if s == TaskControlBlock.WAITING:
        return "WAITING"
    if s == TaskControlBlock.COMPLETED:
        return "COMPLETED"
    return "?(" + String(s) + ")"


def _rendezvous(cells: Int) raises -> Int:
    """The scene itself. Runs in the forked child."""
    var w0 = make_worker()
    var w1 = make_worker()
    var bar = Barrier(2)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    sc[].w1 = UnsafePointer[Worker, MutAnyOrigin](to=w1)
    sc[].bar = UnsafePointer[Barrier, MutAnyOrigin](to=bar)
    sc[].c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    sc[].cause0 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 10 * 8)
    sc[].cause1 = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 11 * 8)
    for i in range(N_CELLS):
        sc[].c[i] = 0

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["t60_lane0"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["t60_lane1"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    var c = sc[].c
    print("  rendezvous: lane0=" + String(c[C_DONE0]) + "/" + String(ROUNDS)
          + " lane1=" + String(c[C_DONE1]) + "/" + String(ROUNDS)
          + " phase=" + String(bar.phase())
          + " waiters_left=" + String(bar.waiter_count()))

    if c[C_ERR0] != 0 or c[C_ERR1] != 0:
        return 1
    if c[C_LOST_LANE] != 0:
        print("  - LOST WAKEUP — lane " + String(c[C_LOST_LANE] - 1)
              + " never completed its round.")
        print("      task state       = " + _state_name(c[C_FP_STATE]))
        print("      early_readiness  = " + String(c[C_FP_EARLY] == 1))
        print("      barrier phase    = " + String(c[C_FP_PHASE]))
        print("      waiters queued   = " + String(c[C_FP_WAITERS])
              + " (target is 2)")
        if (c[C_FP_STATE] == TaskControlBlock.WAITING and c[C_FP_EARLY] == 1):
            print("    => issue #148, the LOST-WAKEUP half. park_current")
            print("       transitioned PARKING -> WAITING with a wake ALREADY")
            print("       LATCHED, because it never consults the early-wake")
            print("       latch (park.mojo:70-76). The release is gone, and the")
            print("       stale latch will fabricate a phantom early wake for")
            print("       this task's NEXT two-phase park.")
        elif (c[C_FP_STATE] == TaskControlBlock.WAITING
              and c[C_FP_WAITERS] >= 1):
            print("    => issue #148, the UNGUARDED-WAIT-SET half. The phase")
            print("       counter advanced, so a release DID run, and the wait")
            print("       set still holds " + String(c[C_FP_WAITERS])
                  + " entr(y|ies); a completed phase must drain the FIFO to 0.")
            print("       Barrier.wait appends to two plain Deque[Int]s while")
            print("       the releasing arrival popleft()s from the same two,")
            print("       with no guard anywhere (barrier.mojo:153;")
            print("       condvar.mojo:173-177 has NO SpinLock in the struct at")
            print("       all). The release read a length that an append on the")
            print("       other worker had already changed, so a waiter was")
            print("       left queued and will never be notified.")
        return 1
    if c[C_DONE0] != ROUNDS or c[C_DONE1] != ROUNDS:
        print("  - a lane stopped short without reporting a lost round")
        return 1
    return 0


def main() raises:
    print("T60 barrier cross-worker (issue #148)")
    var cells = Int(c_malloc(N_CELLS * 8 + 128))

    var pid = _c_fork()
    if Int(pid) < 0:
        print("T60 barrier cross-worker: RED (fork failed; scene not run)")
        _iso_exit(1)
    if Int(pid) == 0:
        var rc = 0
        try:
            rc = _rendezvous(cells)
        except e:
            print("  child raised: " + String(e))
            rc = 1
        _iso_exit(Int32(rc))

    var status: Int32 = 0
    _ = _c_waitpid(pid, UnsafePointer[Int32, MutAnyOrigin](to=status), 0)
    var st = Int(status)
    var sig = st & 0x7F
    var code = (st >> 8) & 0xFF

    if sig != 0:
        print("  - WAIT-SET DATA RACE — the rendezvous died on signal "
              + String(sig) + ". Barrier.wait appends to two plain"
              + " Deque[Int]s and the releasing arrival popleft()s from the"
              + " same two, with no guard anywhere (barrier.mojo:153,"
              + " condvar.mojo:173-177 — 'NO SpinLock in the struct at"
              + " all'). Two real worker threads mutating that storage"
              + " concurrently is memory corruption, not merely a lost wake.")
        print("T60 barrier cross-worker: RED (1)")
        _iso_exit(1)
    if code != 0:
        print("T60 barrier cross-worker: RED (see the child's report above)")
        _iso_exit(1)
    print("T60 barrier cross-worker: PASS")
