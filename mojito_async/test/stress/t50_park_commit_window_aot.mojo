# mojito_async/test/stress/t50_park_commit_window_aot.mojo
#
# RED driver for issue #142 — `park_commit` computes whether it parked and
# throws the answer away, so a wake landing in the VALIDATE -> COMMIT window
# strands the task.
#
# `runtime/park.mojo:286` declares `def park_commit[R: ResultValue](` with no
# return type, and no call site in the tree captures a result.  Internally it
# DOES decide: if a wake latched early readiness while the task sat in
# PARKING, it consumes the latch and unwinds PARKING -> RUNNABLE without ever
# entering WAITING and without enqueueing (the Q6 rule: the record was
# already dequeued, so the CALLER keeps running the task).  Then it tells
# nobody.
#
# `Mutex.lock` (sync/mutex.mojo:232-233) closes with:
#
#     park_commit(h)
#     return False
#
# and False is the caller's "I parked, go run something else".  So when the
# unwind happens, the task is RUNNABLE, sitting in NO queue, holding the
# GRANT marker `unlock()` stamped on it, and `_locked` stays True forever.
# Every later waiter piles up behind a lock owned by a task nobody will run.
#
# THE WINDOW.  Two real OS threads, every step legal under the owner guard:
#
#   contender (w0)                        releaser (w1)
#   ------------------------------        ------------------------------
#   lock(): _locked True, append          holds the lock
#     to the FIFO, release mtx guard
#   park_prepare  -> PARKING              spins on waiter_count() == 1
#   park_validate -> takes the OWNER
#     remote-queue guard, reads
#     _early (False), RELEASES it
#                       <=== HERE ===>    unlock(): pops the waiter, stamps
#                                           WAITER_GRANTED, unpark_current
#                                           takes the SAME owner guard, sees
#                                           PARKING, LATCHES _early, does
#                                           NOT enqueue (correct: the
#                                           parker's COMMIT owns the unwind)
#   park_commit   -> takes the guard,
#     finds the latch, consumes it,
#     unwinds PARKING -> RUNNABLE,
#     does not enqueue
#   return False  -> "I parked"           <- the lie
#
# Both threads take the SAME owner remote-queue guard, and the contender
# takes it TWICE with a gap between (VALIDATE then COMMIT).  The releaser
# only has to win one spinlock acquisition inside that gap.  A third thread
# (`widener`) opens and closes the same guard in a loop so the contender's
# COMMIT frequently has to queue for it, which is what makes the window wide
# enough to hit reliably instead of once in a very long while.  The widener
# adds contention only; it touches no production state.
#
# FAILURE ORACLE, not a hang.  Every drive loop is bounded.  When a round
# fails to complete, the driver waits for the releaser to finish its unlock
# entirely, then fingerprints the mutex and the runtime:
#
#   state == RUNNABLE, grant held, _locked True, 0 waiters, task in NO queue
#       -> issue #142: park_commit unwound and lock() reported a park
#   state == WAITING,  task in NO queue
#       -> a genuine lost wakeup (a DIFFERENT bug; the two-phase kernel is
#          supposed to have closed this, so it would be its own finding)
#
# so the driver reports WHICH defect stranded the task rather than hanging.
#
# BUILD LEVEL.  This driver is in `AOT_O0_DRIVERS`, and the reason is issue
# #143 rather than a compiler crash: the round handshake between the three
# threads is a plain non-atomic Int cell read in a spin loop, and at the
# default optimization level the compiler hoists that load straight out of
# the loop — the first build of this driver "finished" 4000 rounds in 6ms
# having never observed the other thread at all.  That is the same LICM
# hoist #143 is about, met from the driver side.  When #143 lands and the
# TCB reads become atomic, this driver's own cells should follow and the
# `-O 0` pin can come off.
#
# Verdict: exit 0 + "PASS"; a stranded task prints RED and forces exit 1.
# AOT-only (pthread externs; modular/modular#6971).
from std.time import sleep
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.sync import Mutex
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

comptime ROUNDS = Int(4000)
# Per-round watchdog: how many scheduler_loop turns the contender gives a
# round before declaring the task lost.  A healthy round takes 1-3.
comptime ROUND_SPIN_BUDGET = Int(20000)
# How long the releaser waits for a waiter to appear, and for the round ack.
comptime HANDSHAKE_BUDGET = Int(200000000)

comptime W0_ID = Int(1)
comptime W1_ID = Int(2)

# Cell indices into the shared Int array.
comptime C_FAILURES = Int(0)
comptime C_ERR0 = Int(1)
comptime C_ERR1 = Int(2)
comptime C_ERR2 = Int(3)
comptime C_READY = Int(4)        # releaser holds the lock for round N
comptime C_RELEASED = Int(5)     # releaser's unlock() has fully returned
comptime C_ACK = Int(6)          # contender finished round N
comptime C_ABORT = Int(7)
comptime C_ROUNDS_DONE = Int(8)
comptime C_N_COMMITTED = Int(9)  # lock() returned False (committed WAITING)
comptime C_N_EARLY = Int(10)     # lock() returned True on FIRST entry
comptime C_N_REGRANT = Int(11)   # lock() returned True claiming a marker
comptime C_FP_STATE = Int(12)
comptime C_FP_GRANT = Int(13)
comptime C_FP_LOCKED = Int(14)
comptime C_FP_WAITERS = Int(15)
comptime C_FP_HASLOCAL = Int(16)
comptime C_FP_HASREMOTE = Int(17)
comptime C_LOST = Int(18)
# Which handshake wait gave up, when a run ends without a verdict:
#   11 contender waiting for the releaser to take the lock
#   12 a round was declared lost (the real finding)
#   21 releaser could not take the lock
#   22 releaser saw no waiter publish itself
#   23 releaser never got the round ack
comptime C_WHY = Int(19)
comptime N_CELLS = Int(24)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var w0: UnsafePointer[Worker, MutAnyOrigin]
    var w1: UnsafePointer[Worker, MutAnyOrigin]
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var c: UnsafePointer[Int, MutAnyOrigin]   # N_CELLS Int cells

    def __init__(out self):
        self.w0 = UnsafePointer[Worker, MutAnyOrigin](unsafe_from_address=1)
        self.w1 = self.w0
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ---------------------------------------------------------------------------
# contender: one fresh task per round, contending for a lock the releaser
# already holds, so lock() ALWAYS takes the slow park path.
# ---------------------------------------------------------------------------

def dispatch_contender(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    var sc = ud.bitcast[Scene]()
    var h = _handle(tcb_addr, tid)
    var first_entry = not sc[].mtx[].holds_grant(h)
    claim_running(h)
    if sc[].mtx[].lock(rt, h):
        if first_entry:
            # True on a first entry against a held lock is the VALIDATE-True
            # unwind: the grant landed BEFORE the PARKING transition, which
            # mutex.lock does handle correctly.  Counted, not a failure.
            sc[].c[C_N_EARLY] += 1
        else:
            sc[].c[C_N_REGRANT] += 1
        sc[].mtx[].value()[0] += 1
        _ = sc[].mtx[].unlock[IntResult](rt)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
    else:
        # lock() says: "I parked, drive something else."  Whether that is
        # true is exactly what this driver is testing.
        sc[].c[C_N_COMMITTED] += 1
    return 1


def _fingerprint(scp: UnsafePointer[Scene, MutAnyOrigin], h: JoinHandle[IntResult]) raises:
    """Record what the runtime and the mutex actually look like once the
    releaser has completely finished its unlock, so the report can name the
    defect instead of guessing."""
    var sc = scp[]
    var rt = sc.w0[].runtime()
    sc.c[C_FP_STATE] = h.state()
    sc.c[C_FP_GRANT] = 1 if sc.mtx[].holds_grant(h) else 0
    sc.c[C_FP_LOCKED] = 1 if sc.mtx[].is_locked() else 0
    sc.c[C_FP_WAITERS] = sc.mtx[].waiter_count()
    sc.c[C_FP_HASLOCAL] = 1 if rt[].has_local() else 0
    sc.c[C_FP_HASREMOTE] = 1 if rt[].has_remote() else 0


def _drive_round(scp: UnsafePointer[Scene, MutAnyOrigin], rnd: Int) raises -> Bool:
    var sc = scp[]
    var rt = sc.w0[].runtime()
    var tcb = TB.create()
    var tcbp = UnsafePointer[TB, MutAnyOrigin](to=tcb)
    tcb.transition(TaskControlBlock.RUNNABLE)
    var tid = rnd + 1
    rt[].enqueue_local(Int(tcbp), tid)
    var h = JoinHandle[IntResult](tcbp, tid)

    var spins = 0
    while not h.is_completed():
        _ = scheduler_loop(rt[], dispatch_contender, scp.bitcast[Byte](), worker_id=W0_ID)
        spins += 1
        if spins > ROUND_SPIN_BUDGET:
            # The task never came back.  Wait for the releaser to be
            # completely done so the fingerprint is not a torn read of a
            # handoff still in flight, then record it.
            var w = 0
            while sc.c[C_RELEASED] < rnd + 1 and w < HANDSHAKE_BUDGET:
                w += 1
            _fingerprint(scp, h)
            sc.c[C_LOST] = rnd + 1
            return False
    return True


def serve_contender(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    for r in range(ROUNDS):
        var w = 0
        while sc.c[C_READY] < r + 1:
            w += 1
            if w > HANDSHAKE_BUDGET or sc.c[C_ABORT] != 0:
                if sc.c[C_WHY] == 0: sc.c[C_WHY] = 11
                sc.c[C_ABORT] = 1
                return
        if not _drive_round(scp, r):
            if sc.c[C_WHY] == 0: sc.c[C_WHY] = 12
            sc.c[C_ABORT] = 1
            sc.c[C_ACK] = r + 1
            return
        sc.c[C_ROUNDS_DONE] = r + 1
        sc.c[C_ACK] = r + 1


# ---------------------------------------------------------------------------
# releaser: holds the lock, waits for the contender to publish itself as a
# waiter, then releases straight into the contender's PARKING window.
# ---------------------------------------------------------------------------

def serve_releaser(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w1[].runtime()
    for r in range(ROUNDS):
        var w = 0
        while not sc.mtx[].try_lock():
            w += 1
            if w > HANDSHAKE_BUDGET or sc.c[C_ABORT] != 0:
                if sc.c[C_WHY] == 0: sc.c[C_WHY] = 21
                sc.c[C_ABORT] = 1
                return
        sc.c[C_READY] = r + 1
        w = 0
        while sc.mtx[].waiter_count() == 0:
            w += 1
            if w > HANDSHAKE_BUDGET or sc.c[C_ABORT] != 0:
                if sc.c[C_WHY] == 0: sc.c[C_WHY] = 22
                sc.c[C_ABORT] = 1
                return
        # The waiter is published.  It is now somewhere between publishing
        # and its WAITING commit; this release is aimed at that gap.
        _ = sc.mtx[].unlock[IntResult](rt[])
        sc.c[C_RELEASED] = r + 1
        w = 0
        while sc.c[C_ACK] < r + 1:
            w += 1
            if w > HANDSHAKE_BUDGET or sc.c[C_ABORT] != 0:
                if sc.c[C_WHY] == 0: sc.c[C_WHY] = 23
                return


# ---------------------------------------------------------------------------
# widener: contends for the contender-owner's remote-ready queue guard, so
# park_commit has to queue for the lock it just released in park_validate.
# Pure contention — no production state is read or written.
# ---------------------------------------------------------------------------

def serve_widener(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    var sc = scp[]
    var rt = sc.w0[].runtime()
    while sc.c[C_ABORT] == 0 and sc.c[C_ROUNDS_DONE] < ROUNDS:
        rt[].remote_queue()[]._guard.lock()
        rt[].remote_queue()[]._guard.unlock()


@export("t50_contender")
def t50_contender(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_contender(sc)
    except e:
        sc[].c[C_ERR0] = 1
        sc[].c[C_ABORT] = 1


@export("t50_releaser")
def t50_releaser(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_releaser(sc)
    except e:
        sc[].c[C_ERR1] = 1
        sc[].c[C_ABORT] = 1


@export("t50_widener")
def t50_widener(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_widener(sc)
    except e:
        sc[].c[C_ERR2] = 1


def _state_name(s: Int) -> String:
    if s == TaskControlBlock.NEW:
        return "NEW"
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
    return "CANCELLED"


def run_scenario() raises -> Int:
    var w0 = make_worker()
    var w1 = make_worker()
    var mtx = Mutex[Int](0)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].w0 = UnsafePointer[Worker, MutAnyOrigin](to=w0)
    sc[].w1 = UnsafePointer[Worker, MutAnyOrigin](to=w1)
    sc[].mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    var cells = Int(c_malloc(N_CELLS * 8))
    sc[].c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(N_CELLS):
        sc[].c[i] = 0

    var t0: Int = 0
    var t1: Int = 0
    var t2: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["t50_contender"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["t50_releaser"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t2), 0,
        entry_pointer["t50_widener"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)
    _ = _pthread_join(UInt(t2), 0)

    var c = sc[].c
    print("T50 park_commit window: rounds=" + String(c[C_ROUNDS_DONE])
          + "/" + String(ROUNDS)
          + " committed-WAITING=" + String(c[C_N_COMMITTED])
          + " early-unwind=" + String(c[C_N_EARLY])
          + " re-grant=" + String(c[C_N_REGRANT]))

    if c[C_ERR0] != 0 or c[C_ERR1] != 0 or c[C_ERR2] != 0:
        print("  - a worker thread raised (contender=" + String(c[C_ERR0])
              + " releaser=" + String(c[C_ERR1]) + " widener=" + String(c[C_ERR2]) + ")")
        return 1

    if c[C_LOST] == 0:
        if c[C_ROUNDS_DONE] != ROUNDS:
            print("  - only " + String(c[C_ROUNDS_DONE]) + "/" + String(ROUNDS)
                  + " rounds ran and no task was reported lost; the round"
                  + " handshake gave up first (code " + String(c[C_WHY])
                  + "). Inconclusive, not a pass.")
            return 1
        return 0

    # A task was stranded.  Name which defect did it.
    print("  - round " + String(c[C_LOST]) + ": the task never came back."
          + " Fingerprint after the releaser's unlock() fully returned:")
    print("      task state      = " + _state_name(c[C_FP_STATE]))
    print("      holds GRANT     = " + String(c[C_FP_GRANT] == 1))
    print("      mutex _locked   = " + String(c[C_FP_LOCKED] == 1))
    print("      mutex waiters   = " + String(c[C_FP_WAITERS]))
    print("      owner has_local = " + String(c[C_FP_HASLOCAL] == 1))
    print("      owner has_remote= " + String(c[C_FP_HASREMOTE] == 1))
    if (c[C_FP_STATE] == TaskControlBlock.RUNNABLE
            and c[C_FP_GRANT] == 1
            and c[C_FP_LOCKED] == 1
            and c[C_FP_HASLOCAL] == 0
            and c[C_FP_HASREMOTE] == 0):
        print("    => ISSUE #142. park_commit unwound PARKING -> RUNNABLE,")
        print("       consumed the early-wake latch, and returned nothing;")
        print("       Mutex.lock then returned False, which its caller reads")
        print("       as 'parked'. The task is RUNNABLE, in NO queue, holding")
        print("       the grant, and _locked stays True with no owner.")
    elif c[C_FP_STATE] == TaskControlBlock.WAITING:
        print("    => NOT #142: the task committed to WAITING and the wake")
        print("       was never delivered — a lost wakeup, which the two-phase")
        print("       kernel is supposed to have closed. Its own finding.")
    else:
        print("    => stranded in an unclassified shape; see the fingerprint.")
    return 1


def main() raises:
    var failures = 0
    try:
        failures = run_scenario()
    except e:
        print("T50 park_commit window: RED (exception " + String(e) + ")")
        _iso_exit(1)
    if failures == 0:
        print("T50 park_commit window: PASS")
    else:
        print("T50 park_commit window: RED (" + String(failures) + " failure(s))")
        _iso_exit(1)
