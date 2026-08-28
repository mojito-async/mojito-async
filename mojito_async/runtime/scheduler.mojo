# §60 (park via runtime.park).
#
# On ONE worker with no fibers and no dynamic function values (b2), there is
# no invisible preemption: a task yields or parks ONLY at points where it
# calls these primitives (work-first, spec §88).  This module is extern-free:
#
#   - `yield_now(mut rt, h)`  — the RUNNING task is put back on the runnable
#     queue as RUNNABLE (spec §27) WITHOUT blocking; it MUST NOT sleep the
#     worker or register a wait.  `rt` re-queues onto THIS worker's LOCAL
#     deque (A2.2, issue #68 — owner push_back, the same runnable set the
#     scheduler drains); enqueue-once is guarded (an already-RUNNABLE task
#     is never re-enqueued).
#   - the cooperative PARK/WAKE primitives live in runtime/park.mojo now
#     (issue #39 single source): park_current (RUNNING -> PARKING -> WAITING,
#     generation-bumped epoch, spec §25, wait reason stamped) and
#     unpark_current (readiness delivered ONCE, WAITING -> RUNNABLE +
#     re-enqueue onto the worker's REMOTE-ready queue — issue #68: a wake
#     may come from any worker, so the wake lands on the per-worker remote
#     queue; E5 routes it to the OWNER worker, spec §19.2).  The A1.1
#     `_suspend_current` / `resume_current` spellings were deleted; every
#     consumer and lane driver imports park.mojo.
#     A2.5 (issue #71) — STARTED-FIBER AFFINITY: the loop stamps
#     owner_worker AND the owner Runtime address at FIRST RUN (when
#     `worker_id` is nonzero), and asserts the no-off-owner invariant
#     (a STARTED record is never popped by a non-owner worker — spec §19.2;
#     the wake routing in park.mojo + E4's steal guard make it unreachable,
#     this is the debug assertion path).  OWNERSHIP SPLIT (#73 fairness:
#     yield_now's `rt.note_yield()` line + the fair_scheduler_loop append
#     are the sibling lane's; this lane edits only scheduler_loop's body).
#
#     # E3-OWNED: injection intake (issue #69) — #69's bounded injection poll
#     # (optional `inject`/`inject_budget` params, default None) drops in at
#     # the seam below, BEFORE pop_local, keeping the A1 call form.
#
# A1.5 (issue #53) — the FIBER-BACKED drive.  This module stays EXTERN-FREE
# and UNCHANGED in its mechanics: the per-worker loop pops a RUNNABLE
# record and hands it to the statically-known dispatcher.  The frame
# migration lives one module over, in runtime/fiber_seam.mojo, and the
# fiber handle is THREADED THROUGH THE DRIVER VALUE (b2 design decision #4,
# never dynamic dispatch): an *_aot driver's dispatcher drives each record's
# fiber via the seam — first entry makes the fresh context (ms_ctx_make),
# a park is the body's seam_park_switch (fiber -> caller; the frame leaves
# the worker's native context), the park/wake state commit is
# fiber_suspend_current / fiber_yield_now / fiber_resume_current (#39 kernel
# spellings), and the next slice re-enters the fiber at its exact saved
# frame.  Non-parking tasks never touch a fiber: the cheap path is this
# loop + plain execute() on the worker's native context, and the Runtime
# fiber-path toggle (fiber_drives/fiber_switches) stays flat — the fast-path
# regression guard.  Keep this module import-free of fibers so the JIT unit
# drivers (t11..t18/t20..t22) keep linking without the dylib (#6971).
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.queue import TaskRecord
from mojito_async.runtime.runtime import Nil, Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.runtime.inject_queue import InjectQueue
from mojito_async.runtime.join_handle import JoinHandle


# ---------------------------------------------------------------------------
# yield_now — cooperative reschedule without blocking (spec §27)
# ---------------------------------------------------------------------------

def yield_now[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Reschedule the currently RUNNING task without blocking.

    RUNNING -> PARKING -> RUNNABLE is taken through the machine's early-wake
    edge (spec A0.5): the task is never WAITING, claims no wait epoch, and
    re-enters THIS worker's runnable set via enqueue_local (A2.2, issue #68
    — owner push_back onto the local deque; the scheduler drains LOCAL
    before REMOTE).  A real WAITING suspend would use runtime.park's
    `park_current`.  Doesn't sleep the worker or register a wait; no
    allocation beyond the deque's amortized growth.  Enqueue-once: if the
    task is ALREADY RUNNABLE (its record still queued), this is a no-op -
    never double-enqueued."""
    if h.state() == TaskControlBlock.RUNNABLE:
        return  # already reschedulable; do not double-enqueue
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue_local(Int(h.tcb()), h.id())


# ---------------------------------------------------------------------------
# scheduler_loop — the per-worker cooperative drive loop (spec §21 / C7)
# ---------------------------------------------------------------------------

# Dispatcher slot: given the worker's Runtime plus a RUNNABLE record, execute
# that record's task up to its next state.  The dispatcher KNOWS the task
# bodies (b2 cannot store heterogeneous thunks); the loop is generic over
# it.  Passing `rt` lets the dispatcher park (via `park_current`), yield
# (via `yield_now`), or wake (via `unpark_current`) as it drives.
def scheduler_loop[F: def(mut Runtime, Int, Int, BytePtr) raises -> Int, R: ResultValue = Nil](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    worker_id: Int = 0,
) raises -> Int:
    """Drive ONE worker until its run queues are quiet (A2.2, issue #68).

    Per-worker queues (spec §21): the worker's LOCAL deque is drained
    FIRST, then its REMOTE-ready queue — locally-resident spawns never
    starve on their own worker, and a remote wake is observed as soon as
    local work is served.  For each popped record:

      - SKIP it (counted via rt.skipped()) when its TCB is not RUNNABLE —
        a stale duplicate is NEVER dispatched (the A1 enqueue-once
        invariant survives the queue split; it is only logical cross-
        worker), else
      - stamp owner_worker at FIRST RUN when `worker_id` is nonzero (E5
        surface, issue #68; 0 = unpinned/not-started, matching
        wake_target_worker), then hand (rt, tcb_addr, task_id, ud) to
        `dispatcher`, which executes that task to its next state.
    Returns the number of records SERVED (observable progress); skipped
    records are observable via `rt.skipped()`.

    worker_id — explicit worker identity threaded by value (b2 has no TLS);
    the E1 worker pool passes its worker index.  Existing single-runtime
    callers keep the 3-argument form (default 0 = no pin stamp).

    A2.3 (issue #69): the GLOBAL-INJECTION poll is driver-drained through
    the concrete InjectQueue seam (try_pop/pending) at the driver call
    site — the `# E3-OWNED: injection intake` seam below marks where the
    bounded injection poll drops in.  The poll must live where the
    dispatcher is statically known (b2: cross-module generic instantiation
    of the multi-param loop is miscompiled, verified by probe); this plain
    loop keeps the A1 signature EXACTLY — so every existing callsite is
    untouched and injection-free.
    """
    var slices = 0
    while True:
        var have = False
        var rec = TaskRecord(0, 0)
        # # E3-OWNED: injection intake — #69 polls its inject queue HERE.
        if rt.has_local():
            rec = rt.pop_local()
            have = True
        elif rt.has_remote():
            rec = rt.pop_remote()
            have = True
        if not have:
            break
        var checker = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=rec.tcb_addr
        )
        if checker[].state() != TaskControlBlock.RUNNABLE:
            rt.note_skipped()
            continue
        var own = checker[].owner_worker()
        # no-off-owner invariant (issue #71): a STARTED record is worker-
        # affine — the wake routing (park.mojo's owner-remote push) and E4's
        # steal guard keep it off every non-owner queue, so popping one here
        # is a migration bug.  Assert it (debug builds; the A1 unpooled
        # sentinel worker_id == 0 skips the check).
        if own != 0 and worker_id != 0 and own != worker_id:
            raise Error(
                "scheduler_loop: STARTED task "
                + String(rec.task_id)
                + " popped off-owner (owner "
                + String(own)
                + ", worker "
                + String(worker_id)
                + ") — a started fiber must never migrate (issue #71)"
            )
        if worker_id != 0 and own == 0:
            # FIRST RUN: stamp the worker affinity — the owner worker id
            # (E5 surface, issue #68) AND the owner Runtime address (issue
            # #71: the cross-worker wake route target, so unpark_current
            # needs no global worker registry).
            checker[].set_owner_worker(worker_id)
            checker[].set_owner_runtime(
                Int(UnsafePointer[Runtime, MutAnyOrigin](to=rt))
            )
        slices += 1
        _ = dispatcher(rt, rec.tcb_addr, rec.task_id, ud)
    return slices


# ---------------------------------------------------------------------------
# A2.3 (issue #69) — the bounded GLOBAL-INJECTION poll
#
# The poll's LOOP lives at the call site that statistically knows the task
# bodies (this module's stated doctrine: "Executing an unstarted record is
# a GENERIC operation performed at a call site that statically knows the
# task body ... never dynamic dispatch through the record"; b2 additionally
# miscompiles cross-module generic instantiations of this multi-param loop
# shape, verified by probe).  The library therefore exposes the poll as the
# CONCRETE InjectQueue seam — try_pop (by-ref TaskRecord), pending,
# try_push/push — that a worker loop drives:
#
#     while True:
#         polled = 0
#         while polled < INJECT_BUDGET:        # bounded: global intake cannot
#             rec = TaskRecord(0, 0)           # starve under a busy local
#             if not inject[].try_pop(rec):    # queue; budget = fairness (E7
#                 break                        # sharpens it)
#             ... skip-or-dispatch(rec) ...
#             polled += 1
#         if rt has local work: dispatch ONE local record (continue)
#         if inject[].pending() > 0: continue  # injection is the fallthrough
#         break                                # when the worker is quiet
#
# Non-blocking discipline (ADR-009): try_pop never blocks; a FULL injection
# queue never wedges a worker — the worker spins past injection and
# services its LOCAL deques, retrying injection on the next loop iteration
# (issue step 3).  Spec §18/§21 topology: every worker drains the shared
# intake between its local/remote deques, so injected work is dequeued
# identically on every worker — WITHOUT a global lock on the local hot path.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# A1.5 fiber seam (issue #53): the fiber-backed DRIVE lives in
# runtime/fiber_seam.mojo — seam_drive returns the frame-reported
# DriveVerdict (Parked | Completed; T3), seam_park_switch stamps the frame,
# and seam_destroy_slot raises on a parked/suspended (live) frame.  The
# runtime fiber-path counters are comptime-gated (FIBER_TOGGLE).
#
# A1.3 affinity seam (issue #51) — worker-affine started fibers (ADR-006/007)
# ---------------------------------------------------------------------------
#
# spec §19.2: STARTED tasks are NOT stealable; a started fiber's wake is
# routed to its OWNER worker's run queue ("remote-ready routing"), NEVER the
# general stealable set.  On the single worker the owner IS the sole worker,
# so this always resolves to the local FIFO (spec §88 — today's behavior,
# preserved unchanged).  The decision surface below is what EPIC #2's M:N
# worker pool snaps to (E5 started-fiber remote-ready): the pool calls
# wake_target_worker(f.owner_worker(), this_worker_id) at wake time and
# enqueues onto that worker's (remote-ready) queue when the returned target
# differs from the waker; on one worker the target is always the sole queue.
# A2.2 (issue #68) has already landed the queues: the E5 seam pushes onto
# the target worker's RemoteReadyQueue (push_remote).
def wake_target_worker(owner_worker: Int, local_worker: Int) -> Int:
    """Resolve the enqueue target for a woken (started) task/fiber.

    owner_worker — the woken fiber's pinned owner (from Fiber.owner_worker();
                   0 = not started / not pinned: no affinity yet).
    local_worker — the worker performing the wake (explicit identity; b2 has
                   no TLS, so worker identity is threaded by value).

    Returns the worker whose run queue must receive the wake:
      - owner == local_worker (intra-worker wake, spec §88) -> local_worker,
        the wake lands on this worker's own runnable set (today's
        behavior);
      - owner == 0 (unstarted/unpinned) -> local_worker, the general
        runnable-set fallback (nothing to be affine to yet);
      - otherwise (foreign wake) -> owner_worker: the wake lands on the
        OWNER worker's remote-ready queue (spec §19.2), never the stealable
        set.  EPIC #2 enqueues there in the E5 seam (push_remote).
    """
    if owner_worker == 0 or owner_worker == local_worker:
        return local_worker
    return owner_worker

# ===========================================================================
# E4-OWNED (issue #70): A2.4 unstarted-task stealing — §21 loop seam
# ===========================================================================
#
# The steal probe in the M:N worker loop (spec §21), IN ORDER:
#
#     if var task = worker.pop_local():          # local deque
#         worker.run_task(task^); continue
#     if var task = worker.pop_remote_ready():   # E5 started-fiber wakes
#         worker.run_task(task^); continue
#     if var task = worker.runtime.inject_queue.pop():   # E3 injection
#         worker.run_task(task^); continue
#     if var task = worker.try_steal_unstarted():    # <-- E4 probe (issue #70)
#         worker.run_task(task^); continue
#     worker.process_timers()
#     worker.poll_reactor_nonblocking()
#     if worker.has_no_immediate_work():
#         worker.park_os_thread_until_event()      # <-- E6 idle sleep handoff
#
# The loop restructure is #68's; E4's part is the probe call + counters:
#   - the probe is `Worker.try_steal_unstarted()` (runtime/worker.mojo):
#     round-robin peers from own_index+1, STARTED guard (spec §19.1/§19.2),
#     ONE capped round, then hands off — the empty-round outcome feeds the
#     E6 sleep decision directly below (an idle worker does NOT spin on
#     empty deques; issue #70 step 3/4);
#   - each successful steal bumps the worker runtime's task_steals_total
#     exactly once (spec §71; issue #70 step 5 — failed probes bump nothing).
#
# ADR-006/ADR-007 are upheld structurally: only never-run records are ever
# removed from a deque, so a started fiber's live stack is never migrated.