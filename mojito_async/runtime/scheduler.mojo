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
#     A2.5 (issue #71) — STARTED-FIBER AFFINITY / OWNERSHIP SPLIT: the
#     BASE scheduler_loop stamps owner_worker AND the owner Runtime
#     address at FIRST RUN (when `worker_id` is nonzero) and asserts the
#     no-off-owner invariant (a STARTED record is never popped by a
#     non-owner worker — spec §19.2; the wake routing in park.mojo + E4's
#     steal guard make it unreachable, this is the debug assertion path).
#     H1 fold (#73/#71 coordinate): fair_scheduler_loop stamps BOTH at
#     FIRST RUN (owner_worker + owner Runtime address) and asserts the
#     same no-off-owner invariant on its REMOTE-ready pops (budget-drain
#     + main pick) — sound on this base because every remote record
#     carries either no owner yet or this worker's own id, and t36 is the
#     only fair-loop driver.
#
#     # E3-OWNED: injection intake (issue #69) — #69's bounded injection poll
#     # (optional `inject`/`inject_budget` params, default None) drops in at
#     # the seam below, BEFORE pop_local, keeping the A1 call form.
#
# A2.7 (issue #73) — the FAIRNESS BUDGET (spec §21/§67/§71).  The
# worker-loop fairness drive is `fair_scheduler_loop` below: after K
# CONSECUTIVELY LOCALLY-SOURCED task slices it services remote-ready, the
# injection intake, and the caller's timer/reactor sweep before resuming
# local work (spec §21 "run at most K ready tasks then service
# reactor/timers").  Work-class slice accounting (local vs remote vs
# injection) and the kill-0 starvation watch live on the runtime.
#
# COOPERATIVE YIELD — documented limitation (spec §67/§68): scheduling is
# COOPERATIVE.  `yield_now` (below) remains the escape hatch, and a task
# that parks through runtime.park also hands the worker back.  But ARBITRARY
# CPU-BOUND USER CODE THAT NEVER YIELDS MAY MONOPOLIZE ITS WORKER: the
# scheduler has NO async stack preemption in the MVP (spec §68 forbids
# it), so a task's own slice runs to its next checkpoint unconditionally.
# The fairness budget bounds the DEFERRAL of timers/reactor/remote/inject
# to K slices (they still run between the hog's slices), and the kill-0
# watch MEASURES a never-yielding task via starvation_events — the
# accepted, documented cooperative limitation (spec §67 says exactly this;
# §68 lists safepoint/time-budget preemption as later options, none in the
# MVP).  E8 (bench, issue #74) reads starvation_events.
#
#   REVIEW CHECKLIST (A2.7 drivers review):
#     [ ] fair_scheduler_loop counts CONSECUTIVE LOCAL slices vs Budget.K
#     [ ] hitting K services timers/reactor BEFORE more local work
#     [ ] remote-ready + injection drain to quiet in the same budget pass
#     [ ] a never-yielding task: timers still fire (t36 test 1), remote/
#         inject run within the window (t36 test 2), starvation_events
#         bumps once per >K streak (t36 tests 1+3)
#     [ ] a yielding/parking task NEVER bumps starvation_events (t36 test 3)
#     [ ] yield_now still reschedules cooperatively (A1 parity, t36 test 3)
#     [ ] K=0 degenerates to plain scheduler_loop semantics
#     [ ] the loop returns only after a quiet final service pass (E6
#         idle-sleep handoff: park the OS thread only after this return)
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
    # A2.7 (issue #73): one cooperative handoff observed — the starve-watch
    # (fair_scheduler_loop) resets its consecutive-slices counter on this.
    rt.note_yield()


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
        # issue #144: atomic check-and-pop eliminates the TOCTOU between a
        # separate has_local() probe and pop_local() — a thief stealing the
        # last record in that window made the owner raise.
        var local_opt = rt.try_pop_local()
        if local_opt:
            rec = local_opt.value()
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
            rt._complete_dispatched()
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
        rt._complete_dispatched()
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
# A2.7 (issue #73) — FAIRNESS BUDGET DRIVE (spec §21 "run at most K ready
# tasks then service reactor/timers"; §67 no class starves; §71 observe).
#
# fair_scheduler_loop is the WORKER-LOOP fairness budget over the same
# per-worker queues as scheduler_loop:
#
#   BUDGET (spec §21): the loop counts CONSECUTIVE LOCALLY-SOURCED slices
#   against Budget.K; the instant that count reaches K it (1) notes the
#   budget reset, (2) runs the caller's timer/reactor service callback
#   (a time/timer_service sweep — the A1 timer lane — and/or a reactor
#   nonblocking poll), and (3) drains the REMOTE-ready queue and the
#   INJECTION intake to quiet BEFORE resuming local work.  Endless local
#   CPU work therefore never defers timers/reactor/remote/inject beyond K
#   slices: the acceptance "a timer inserted at T+small fires while a local
#   task never yields".
#
#   WORK-CLASS ACCOUNTING (§71): every served slice is classified and
#   counted on the runtime — slices_local (this worker's deque: spawn,
#   yield), slices_remote (STARTED-task wakes, spec §19.2), slices_inject
#   (the E3 intake seam; #69's inject_queue polls through this drains).
#   The class counters let the budget guarantee reactor/timer progress
#   under CPU saturation and surface exactly what deferred what to the
#   benchmark lane (E8, issue #74).
#
#   KILL-0 STARVATION WATCH (§67/§71): the loop tracks the CURRENT task's
#   consecutive locally-sourced slices since ITS OWN cooperative handoff —
#   a yield_now (observed via the runtime yields counter) or a park/exit
#   (observed via the post-dispatch TCB state — WAITING/COMPLETED).  When a
#   SAME-task streak exceeds Budget.K (the K+1th slice), the runtime
#   starvation_events counter bumps ONCE per streak.  Note carefully: the
#   LOOP-forced service passes are what prevent actual starvation (timers
#   fire, remote/inject drain), but a never-yielding task's OWN streak is
#   NOT reset by them — that streak is the documented §67 cooperative
#   limitation ("CPU-bound user code that never yields may monopolize its
#   worker"; NO preemption in MVP, §68), measured rather than hidden and
#   surfaced to the benchmarks.  A yielding/parking task NEVER bumps it.
#
#   E6 IDLE-SLEEP HANDOFF (banner for the sibling #72 lane): this function
#   returns ONLY after a quiet final service pass — the fair drain is empty
#   (timers serviced, reactor polled, remote/inject drained) — so an idle
#   worker that parks its OS thread (NativeEvent park) does so strictly
#   AFTER the fair drain, composing with the #72 idle path.  The pool's
#   worker loop (thread_entry) parks only post-return.
#
#   K=0 disables the budget, the service passes and the watch entirely:
#   the loop degenerates to plain scheduler_loop semantics (A1 parity).
#
#   Generic over the SAME statically-known record dispatcher as
#   scheduler_loop (b2: no function-typed struct fields) PLUS the caller's
#   service callback `service(rt, ud)` — the timer/reactor sweep the budget
#   runs between drains (the A1 timer lane's service_timers wrapped by the
#   caller, or a reactor poll).  The budget default (4) mirrors
#   config.DEFAULT_FAIR_BUDGET_K — the literal is duplicated here because
#   scheduler.mojo stays EXTERN-FREE (config.mojo imports cpu_logical_count
#   from mojito-sys; modular/modular#6971 JIT drivers cannot resolve that).
# ---------------------------------------------------------------------------

# Starve-watch + budget state of one fair drive (loop-local; b2 has no
# function-typed or global mutable state, so the loop owns it explicitly).
struct FairLoopState(ImplicitlyCopyable, ImplicitlyDeletable):
    """Per-drive fairness state (budget counter + kill-0 watch)."""

    var consecutive_budget: Int   # consecutive locally-sourced slices served
    var streak_task: Int          # task id owning the current watch streak (0 = none)
    var streak_len: Int           # consecutive same-task local slices since ITS yield/park

    def __init__(out self):
        self.consecutive_budget = 0
        self.streak_task = 0
        self.streak_len = 0


def fair_scheduler_loop[
    F: def(mut Runtime, Int, Int, BytePtr) raises -> Int,
    S: def(mut Runtime, BytePtr) raises,
    R: ResultValue = Nil,
](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    service: S,
    budget_k: Int = 4,
    worker_id: Int = 0,
) raises -> Int:
    """Drive ONE worker under the A2.7 fairness budget (spec §21/§67/§71).

    Same record discipline as scheduler_loop (local deque FIRST, then the
    remote-ready queue, then the E3 injection intake; stale non-RUNNABLE
    records are skipped via rt.skipped(); owner_worker is stamped at first
    run when worker_id is nonzero) — PLUS, whenever K consecutively
    locally-sourced slices have been served, a budget pass runs BEFORE more
    local work: rt.budget_resets(), the `service` callback (the caller's
    timer/reactor sweep), then a FULL drain of the remote-ready queue and
    the injection intake (counted slices_remote / slices_inject).  Endless
    local CPU work defers timers/reactor/remote/inject by at most K slices.

    The kill-0 starve watch (rt.starvation_events) bumps ONCE per streak
    when the SAME never-yielding task reaches Budget.K+1 consecutive local
    slices with no yield_now (rt.yields delta) and no park/exit (TCB no
    longer RUNNABLE after its slice).  A yield_now or park resets the
    streak; a loop-forced service pass does NOT (it is the loop's doing,
    not the task's cooperation — the §67 documented limitation measured).

    Returns the number of records SERVED (dispatched; skipped records are
    observable via rt.skipped()).  When it returns, the fair drain is quiet
    — timers/reactor serviced (the E6 idle-sleep handoff banner: park the
    OS thread only after this return)."""
    comptime CLS_LOCAL = Int(1)
    comptime CLS_REMOTE = Int(2)
    comptime CLS_INJECT = Int(3)
    var slices = 0
    var st = FairLoopState()
    var fair_drained = False
    while True:
        # ---- budget gate: K consecutive local slices -> service first ----
        if (
            budget_k > 0
            and rt.has_local()
            and st.consecutive_budget >= budget_k
        ):
            st.consecutive_budget = 0
            rt.note_budget_reset()
            rt.note_service_sweep()
            service(rt, ud)
            # drain remote-ready fully, then the injection intake fully
            while rt.has_remote():
                var rrec = rt.pop_remote()
                var rcheck = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
                    unsafe_from_address=rrec.tcb_addr
                )
                if rcheck[].state() != TaskControlBlock.RUNNABLE:
                    rt.note_skipped()
                    rt._complete_dispatched()
                    continue
                # H1 no-off-owner assertion (issue #73/#71 coordinate): a
                # STARTED record must NEVER pop off its owner worker (spec
                # §19.2; the #71 lane's owner-routed park makes it
                # unreachable once merged — this is the debug assertion
                # path; the A1 unpooled sentinel worker_id == 0 skips it).
                var rown = rcheck[].owner_worker()
                if rown != 0 and worker_id != 0 and rown != worker_id:
                    raise Error(
                        "fair_scheduler_loop: STARTED task "
                        + String(rrec.task_id)
                        + " popped remote off-owner (owner "
                        + String(rown)
                        + ", worker "
                        + String(worker_id)
                        + ") — a started fiber must never migrate (issue #71)"
                    )
                if worker_id != 0 and rown == 0:
                    # FIRST RUN (fair drain): stamp the worker affinity —
                    # owner worker id (E5, issue #68) AND the owner Runtime
                    # address (H1; issue #71 wake route target).
                    rcheck[].set_owner_worker(worker_id)
                    rcheck[].set_owner_runtime(
                        Int(UnsafePointer[Runtime, MutAnyOrigin](to=rt))
                    )
                rt.note_slice_remote()
                slices += 1
                _ = dispatcher(rt, rrec.tcb_addr, rrec.task_id, ud)
                rt._complete_dispatched()
            while rt.has_inject():
                var irec = rt.pop_inject()
                var icheck = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
                    unsafe_from_address=irec.tcb_addr
                )
                if icheck[].state() != TaskControlBlock.RUNNABLE:
                    rt.note_skipped()
                    rt._complete_dispatched()
                    continue
                if worker_id != 0 and icheck[].owner_worker() == 0:
                    icheck[].set_owner_worker(worker_id)
                    icheck[].set_owner_runtime(
                        Int(UnsafePointer[Runtime, MutAnyOrigin](to=rt))
                    )
                rt.note_slice_inject()
                slices += 1
                _ = dispatcher(rt, irec.tcb_addr, irec.task_id, ud)
                rt._complete_dispatched()

        # ---- pick the next record (spec §21 order: local, remote, inject) --
        var have = False
        var rec = TaskRecord(0, 0)
        var cls = 0
        # issue #144: atomic check-and-pop (see scheduler_loop equivalent above).
        var local_opt = rt.try_pop_local()
        if local_opt:
            rec = local_opt.value()
            have = True
            cls = CLS_LOCAL
        elif rt.has_remote():
            rec = rt.pop_remote()
            have = True
            cls = CLS_REMOTE
        elif rt.has_inject():
            rec = rt.pop_inject()
            have = True
            cls = CLS_INJECT
        if not have:
            # quiet fair-drain: one final service pass before declaring the
            # worker idle (E6 handoff: sleep only after this return; a wake
            # produced by the pass re-enters the loop).
            if fair_drained:
                break
            rt.note_service_sweep()
            service(rt, ud)
            fair_drained = True
            continue
        fair_drained = False

        # ---- serve the record (stale-skip + first-run owner stamp) --------
        var checker = UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
            unsafe_from_address=rec.tcb_addr
        )
        if checker[].state() != TaskControlBlock.RUNNABLE:
            rt.note_skipped()
            rt._complete_dispatched()
            continue
        # H1 no-off-owner assertion (issue #73/#71 coordinate): a STARTED
        # record picked from the REMOTE-ready queue must belong to this
        # worker (spec §19.2 — started fibers never migrate).  Local pops
        # are this worker's own spawns/yields (owner stamped here at first
        # run); inject records are never started when first popped.
        if cls == CLS_REMOTE:
            var mown = checker[].owner_worker()
            if mown != 0 and worker_id != 0 and mown != worker_id:
                raise Error(
                    "fair_scheduler_loop: STARTED task "
                    + String(rec.task_id)
                    + " popped remote off-owner (owner "
                    + String(mown)
                    + ", worker "
                    + String(worker_id)
                    + ") — a started fiber must never migrate (issue #71)"
                )
        if worker_id != 0 and checker[].owner_worker() == 0:
            # FIRST RUN: stamp the worker affinity — owner worker id (E5,
            # issue #68) AND the owner Runtime address (H1; issue #71 wake
            # route target).
            checker[].set_owner_worker(worker_id)
            checker[].set_owner_runtime(
                Int(UnsafePointer[Runtime, MutAnyOrigin](to=rt))
            )
        if cls == CLS_LOCAL:
            rt.note_slice_local()
            st.consecutive_budget += 1
        elif cls == CLS_REMOTE:
            rt.note_slice_remote()
        else:
            rt.note_slice_inject()
        slices += 1
        var yields_before = rt.yields()
        _ = dispatcher(rt, rec.tcb_addr, rec.task_id, ud)
        rt._complete_dispatched()

        # ---- kill-0 starve watch (locally-sourced slices only) ------------
        if cls == CLS_LOCAL:
            if checker[].state() != TaskControlBlock.RUNNABLE or rt.yields() != yields_before:
                # cooperative handoff: parked (WAITING), completed, or
                # yield_now — the streak is over (fresh watch window).
                st.streak_task = 0
                st.streak_len = 0
            elif st.streak_task == rec.task_id:
                st.streak_len += 1
                if budget_k > 0 and st.streak_len == budget_k + 1:
                    rt.note_starvation()
            else:
                st.streak_task = rec.task_id
                st.streak_len = 1
    return slices

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