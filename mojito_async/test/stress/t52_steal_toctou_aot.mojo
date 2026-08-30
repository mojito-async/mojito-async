# mojito_async/test/stress/t52_steal_toctou_aot.mojo
#
# RED driver for issue #144 — a raise on the worker loop kills the OS thread
# and silently strands every task that thread owned.
#
# `runtime/scheduler.mojo:178` (and :372, :436) does check-then-pop across
# TWO separate guard acquisitions:
#
#     if rt.has_local():          # LocalDeque._guard taken, read, RELEASED
#         rec = rt.pop_local()    # LocalDeque._guard taken AGAIN
#
# and `runtime/queue.mojo:163-172` raises on an empty deque:
#
#     raise Error("LocalDeque.pop_back: pop from an empty deque")
#
# So a thief that takes the last record between those two acquisitions makes
# the OWNER raise.  Stealing has been live since #112 wired `set_peers`, and
# the thief only has to win one spinlock acquisition inside a gap the owner
# opens on every single dispatch.
#
# WHY A RAISE HERE IS NOT AN ERROR, IT IS A DEAD THREAD.  The error
# propagates out of `fair_scheduler_loop`, out of `pool_worker_loop_scheduled`
# and into the embedder's `abi("C")` trampoline, which per this project's own
# documented embedding pattern can only set `loop_ok = False` and return.
# The worker's OS thread exits while the pool runs on.  Every STARTED task it
# owned is then permanently unrunnable — started tasks are owner-affine and
# never stealable, and the dead worker's remote queue is never popped again.
# Wakes routed to it enqueue successfully into a queue nobody will drain.
# Nothing detects this before `join_all` at teardown: no log, no counter, no
# signal.
#
# THE SCENE.  Two real Workers wired as peers exactly the way the pool wires
# them (`_index`/`_peers`/`_n_workers`, the t33 idiom).  The victim owns a
# local deque and drives `scheduler_loop` over it; the thief hammers the
# production `Worker.request_steal` against the victim's deque.  One record
# is in flight at a time, which is the shape the issue describes: "Worker A's
# deque holds exactly one record."
#
# The record is a single never-started TCB, re-enqueued each round and left
# RUNNABLE by a dispatcher that only counts.  That keeps it permanently
# stealable (the steal guard rejects STARTED records) and means the driver
# allocates nothing per round.
#
# FAILURE ORACLE, not a hang.  Every loop is bounded.  The victim's drive
# loop is wrapped the way the embedder's trampoline is, so a raise is caught,
# its message recorded, and the thread stops — which is precisely what
# happens in production, and the driver can then report:
#
#   - the exact error that escaped the loop
#   - that the victim's loop_ok went False
#   - what was still sitting in the victim's queues when it died, which
#     nothing will ever run now
#
# Green means the two workers between them consume exactly what was produced
# and no loop ever raises.
#
# BUILD LEVEL: `-O 0`, for the same reason t38/t47/t50 are pinned — the
# driver's own cross-thread counters are plain Int cells and issue #143's
# LICM hoist eats them at default `-O`.
#
# Verdict: exit 0 + "PASS"; a raise or a lost record prints RED and exits 1.
# AOT-only (pthread externs; modular/modular#6971).
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker
from mojito_async.runtime.park import park_current, unpark_current
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

comptime ROUNDS = Int(200000)
comptime W0_ID = Int(1)

comptime C_PRODUCED = Int(0)
comptime C_SERVED = Int(1)       # records the victim dispatched
comptime C_STOLEN = Int(2)       # records the thief took
comptime C_VICTIM_DIED = Int(3)  # the embedder's loop_ok going False
comptime C_THIEF_DIED = Int(4)
comptime C_DONE = Int(5)         # victim finished all rounds
comptime C_LEFT_LOCAL = Int(6)   # queue contents at the moment of death
comptime C_LEFT_REMOTE = Int(7)
comptime C_THIEF_RAISES = Int(8)  # request_steal's OWN check-then-pop firing
comptime C_ORPHAN_QUEUED = Int(9)   # wakes parked in the dead worker's remote queue
comptime C_ORPHAN_STATE = Int(10)
comptime N_CELLS = Int(16)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]   # victim
    var w1: UnsafePointer[Worker, MutAnyOrigin]   # thief
    var tcb: UnsafePointer[TB, MutAnyOrigin]
    var c: UnsafePointer[Int, MutAnyOrigin]
    var err: UnsafePointer[Byte, MutAnyOrigin]    # unused placeholder

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.tcb = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=1)
        self.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.err = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)


# The victim's dispatcher.  It COUNTS and returns; it never claims RUNNING,
# so the record stays never-started and therefore stays stealable, and it
# never completes, so one TCB serves every round with no allocation.
def dispatch_victim(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    sc[].c[C_SERVED] += 1
    return 1


def dispatch_park_orphan(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """Setup only: run one task far enough that it is STARTED and WAITING,
    with its owner_runtime stamped to the victim's worker.  A started task
    is owner-affine — no thief may ever take it — so once the victim dies it
    can only ever be run by a thread that no longer exists."""
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    park_current(rt, h)
    return 1


def serve_victim(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """The owner side of the window.  Push exactly one record, then drive
    the loop.  `scheduler_loop`'s has_local()/pop_local() pair opens the
    gap; the thief is spinning on the same guard."""
    var sc = scp[]
    var rt = sc.w0[].runtime()
    for r in range(ROUNDS):
        rt[].enqueue_local(Int(sc.tcb), 1)
        sc.c[C_PRODUCED] += 1
        _ = scheduler_loop(rt[], dispatch_victim, scp.bitcast[Byte](), worker_id=W0_ID)
    sc.c[C_DONE] = 1


def serve_thief(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    """The production steal probe, hammered at the victim's deque.

    `request_steal` carries the SAME check-then-pop shape internally
    (worker.mojo:184-186: is_empty() then steal_front(), two guard
    acquisitions), so the OWNER popping between them makes the THIEF raise.
    In production that raise propagates the same way and kills the thief's
    worker thread.  Here it is caught and counted rather than fatal, so one
    run can reach BOTH sides of the defect instead of stopping at whichever
    fires first."""
    var sc = scp[]
    while sc.c[C_DONE] == 0 and sc.c[C_VICTIM_DIED] == 0:
        try:
            var got = sc.w1[].request_steal[IntResult](0)
            if got:
                sc.c[C_STOLEN] += 1
        except e:
            sc.c[C_THIEF_RAISES] += 1


@export("t52_victim")
def t52_victim(arg: BytePtr) abi("C"):
    """Stands in for the embedder's abi("C") trampoline, which per the
    documented embedding pattern can only set loop_ok = False and return."""
    var sc = arg.bitcast[Scene]()
    try:
        serve_victim(sc)
    except e:
        # loop_ok = False.  In production the OS thread now exits and the
        # pool keeps running without it.
        sc[].c[C_VICTIM_DIED] = 1
        var rt = sc[].w0[].runtime()
        try:
            sc[].c[C_LEFT_LOCAL] = rt[].local_queue()[].count()
            sc[].c[C_LEFT_REMOTE] = rt[].remote_queue()[].count()
        except e2:
            pass
        print("  victim worker loop RAISED and the thread is now gone:")
        print("    " + String(e))


@export("t52_thief")
def t52_thief(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_thief(sc)
    except e:
        sc[].c[C_THIEF_DIED] = 1
        print("  thief worker loop RAISED and the thread is now gone:")
        print("    " + String(e))


def run_scenario() raises -> Int:
    # Two workers wired as peers exactly the way worker_pool.set_peers does
    # (t33's idiom; Workers are not movable in b2, so they live on main's
    # frame, which outlives every thread here).
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

    var tcb = TB.create()
    tcb.transition(TaskControlBlock.RUNNABLE)

    # The orphan: a STARTED task owned by the victim worker, parked WAITING.
    # It is here to make "silently strands its tasks" observable rather than
    # asserted — after the victim dies, a wake for this task still enqueues
    # perfectly happily onto a queue nothing will ever pop.
    var orphan = TB.create()
    var orphanp = UnsafePointer[TB, MutAnyOrigin](to=orphan)
    orphan.transition(TaskControlBlock.RUNNABLE)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = w0p
    sc[].w1 = w1p
    sc[].tcb = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    sc[].c = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_CELLS * 8))
    )
    for i in range(N_CELLS):
        sc[].c[i] = 0

    var rt0 = w0.runtime()
    rt0[].enqueue_local(Int(orphanp), 99)
    _ = scheduler_loop(rt0[], dispatch_park_orphan, sc.bitcast[Byte](), worker_id=W0_ID)
    var h_orphan = JoinHandle[IntResult](orphanp, 99)

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["t52_victim"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["t52_thief"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    var c = sc[].c

    # Post-mortem: deliver a wake to the orphan now that the victim worker
    # is gone.  unpark_current routes it to the OWNER's remote-ready queue,
    # which is the correct thing to do and is exactly the problem.
    if c[C_VICTIM_DIED] != 0 and h_orphan.state() == TaskControlBlock.WAITING:
        unpark_current(rt0[], h_orphan)
        c[C_ORPHAN_QUEUED] = rt0[].remote_queue()[].count()
        c[C_ORPHAN_STATE] = h_orphan.state()

    print("T52 steal TOCTOU: produced=" + String(c[C_PRODUCED])
          + " served=" + String(c[C_SERVED])
          + " stolen=" + String(c[C_STOLEN])
          + " (of " + String(ROUNDS) + " rounds)")

    var bad = 0
    if c[C_VICTIM_DIED] != 0:
        print("  - ISSUE #144. The victim's scheduler_loop raised and its"
              + " worker thread is gone.")
        print("      loop_ok            = False")
        print("      rounds reached     = " + String(c[C_PRODUCED])
              + "/" + String(ROUNDS))
        print("      left in local deque= " + String(c[C_LEFT_LOCAL]))
        print("      left in remote q   = " + String(c[C_LEFT_REMOTE]))
        print("    Then, with the worker already gone, a wake was delivered to")
        print("    a STARTED task that worker owned:")
        print("      wake accepted, orphan state = "
              + ("RUNNABLE" if c[C_ORPHAN_STATE] == TaskControlBlock.RUNNABLE else String(c[C_ORPHAN_STATE])))
        print("      records now in the dead worker's remote queue = "
              + String(c[C_ORPHAN_QUEUED]))
        print("    That enqueue succeeded and nothing will ever pop it. The")
        print("    task is owner-affine, so no thief may take it either. No")
        print("    log, no counter, nothing detects this before join_all.")
        bad += 1
    if c[C_THIEF_RAISES] != 0:
        print("  - request_steal raised " + String(c[C_THIEF_RAISES])
              + " time(s) on the THIEF side: worker.mojo:184-186's own")
        print("    is_empty()/steal_front() pair, the same check-then-pop.")
        print("    Each one would have killed the thief's worker thread in")
        print("    production; this driver catches them so the run can reach")
        print("    the owner-side raise too.")
        bad += 1
    if c[C_THIEF_DIED] != 0:
        print("  - the thief thread itself died on an unexpected error.")
        bad += 1
    if bad == 0:
        var consumed = c[C_SERVED] + c[C_STOLEN]
        if consumed != c[C_PRODUCED]:
            print("  - accounting mismatch: produced=" + String(c[C_PRODUCED])
                  + " but served+stolen=" + String(consumed)
                  + " (a record was dropped or double-consumed)")
            bad += 1
        if c[C_STOLEN] == 0:
            print("  - the thief never stole anything, so the"
                  + " has_local()/pop_local() window was never contended."
                  + " Inconclusive, not a pass.")
            bad += 1
    return bad


def main() raises:
    var bad = 0
    try:
        bad = run_scenario()
    except e:
        print("T52 steal TOCTOU: RED (exception " + String(e) + ")")
        _iso_exit(1)
    if bad == 0:
        print("T52 steal TOCTOU: PASS")
    else:
        print("T52 steal TOCTOU: RED (" + String(bad) + " failure(s))")
        _iso_exit(1)
