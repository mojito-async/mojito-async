# mojito_async/test/stress/t12_stress.mojo
#
# A1.5 stress (issue #37) — exit criterion 2: LOST-WAKEUP SUITES under
# FORCED SCHEDULES (race_hooks-style determinism), over the REAL A1.1
# park/wake machinery (TaskControlBlock transitions + Runtime enqueue).
#
# The lost-wakeup race (spec §23): a task checks its condition (false),
# another makes the condition true and wakes nobody, and the first parks
# forever.  On the A1.1 single cooperative worker the defense is the
# protocol: before committing to WAITING a task re-checks cancellation and
# its condition, and readiness is delivered AT MOST ONCE per wait epoch
# (resume_current + enqueue-once), while a wake that lands on a
# not-yet-parked task is a counted no-op that the waiter observes through
# its condition.
#
# PART A — real-runtime interleavings (scheduler_loop + a dispatcher that
# FORCES the dangerous orders) over 2-task and 200-task populations:
#   A1 wake-before-park: the notifier runs BEFORE the waiter ever parks; the
#                        waiter must complete WITHOUT sleeping and without
#                        losing readiness (no extra enqueue, no raise).
#   A2 park-then-wake:   the waiter parks (WAITING), the notifier wakes it,
#                        it re-checks and completes; exactly one wake
#                        accepted, one re-enqueue.
#   A3 double-wake:      two wake attempts in one epoch; the second is an
#                        enqueue-once no-op; exactly one delivery.
#   A4 wake storm:       200 tasks park, are woken in REVERSE order, all
#                        complete; zero skipped/stale records.
#
# PART B — race_hooks-style pipeline model bound to REAL spawned TCBs and a
# REAL Runtime: scripted boundaries (PREPARE/VALIDATE/COMMIT/WAKE) fire
# actions (SET_READY / REQUEST_CANCEL / WAKE); every delivery is an actual
# resume_current enqueue, so attempts/accepted/enqueues are ground truth.
#   B1 baseline           full pipeline, parks, explicit wake accepted once.
#   B2 wake-before-park   SET_READY at VALIDATE: never sleeps, one winner.
#   B3 double-wake        sleeps, 2 attempts -> 1 accepted (per-epoch guard).
#   B4 cancel-vs-ready    cancel fires before the condition: CANCEL wins
#                         deterministically (identical script, two runs).
#
# Pure Mojo (`mojo run -I repo`), extern-free, def-only, deterministic.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async.cancellation import CancelFlag, make_cancel_flag
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import (
    _suspend_current,
    resume_current,
    scheduler_loop,
)
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.task import JoinHandle, claim_running, execute, spawn


def red(what: String) raises -> None:
    print("T12 stress (lost-wakeup): RED (" + what + ")")
    raise Error(what)


comptime TB = TaskControlBlock[IntResult]


def _handle(tcb_addr: Int, tid: Int) -> JoinHandle[IntResult]:
    return JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )


# ===========================================================================
# PART A — real-runtime interleavings under forced schedules
# ===========================================================================

struct SceneA(ImplicitlyCopyable, ImplicitlyDeletable):
    """2-task scene: a_ready@0, a_tcb@1, a_tid@2, b_tid@3, waiting_seen@4,
    ran@5, slice_a@6."""

    var a_ready: UnsafePointer[Int, MutAnyOrigin]
    var a_tcb: UnsafePointer[Int, MutAnyOrigin]
    var a_tid: UnsafePointer[Int, MutAnyOrigin]
    var b_tid: UnsafePointer[Int, MutAnyOrigin]
    var waiting_seen: UnsafePointer[Int, MutAnyOrigin]
    var ran: UnsafePointer[Int, MutAnyOrigin]
    var slice_a: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.a_ready = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.a_tcb = self.a_ready
        self.a_tid = self.a_ready
        self.b_tid = self.a_ready
        self.waiting_seen = self.a_ready
        self.ran = self.a_ready
        self.slice_a = self.a_ready


def a_body_see(ud: BytePtr) raises -> IntResult:
    """Waiter slice for the wake-before-park order: readiness must already
    be visible through the condition; completes without ever parking."""
    var sc = ud.bitcast[SceneA]()
    if sc[].a_ready[] != 1:
        red("wake-before-park: A ran without readiness")
    sc[].ran[] = sc[].ran[] + 1
    return IntResult(11)


def a_body_finish(ud: BytePtr) raises -> IntResult:
    """Waiter post-resume slice: re-checks the condition, completes."""
    var sc = ud.bitcast[SceneA]()
    if sc[].a_ready[] != 1:
        red("resumed A without readiness (lost wakeup)")
    sc[].ran[] = sc[].ran[] + 1
    return IntResult(22)


def b_body_notify(ud: BytePtr) raises -> IntResult:
    """Notifier body: flips the condition true."""
    var sc = ud.bitcast[SceneA]()
    sc[].a_ready[] = 1
    sc[].ran[] = sc[].ran[] + 1
    return IntResult(99)


def dispatch_a1(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """Forced wake-before-park: B is enqueued FIRST; B sets the condition
    and wakes A, which is still RUNNABLE (enqueue-once no-op — legal, never
    raising, never lost: A re-checks the condition on its own slice)."""
    var sc = ud.bitcast[SceneA]()
    if tid == sc[].b_tid[]:
        var hb = _handle(tcb_addr, tid)
        _ = execute(hb, b_body_notify, ud)
        var ha = _handle(sc[].a_tcb[], sc[].a_tid[])
        resume_current(rt, ha)
        return 1
    var ha = _handle(tcb_addr, tid)
    _ = execute(ha, a_body_see, ud)
    return 1


def dispatch_a2(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """A2 schedule: A parks on slice 0; B notifies and wakes; A resumes and
    completes."""
    var sc = ud.bitcast[SceneA]()
    if tid == sc[].a_tid[]:
        var ha = _handle(tcb_addr, tid)
        if sc[].slice_a[] == 0:
            claim_running(ha)
            _suspend_current(rt, ha)
            sc[].slice_a[] = 1
            sc[].waiting_seen[] = 1
        else:
            _ = execute(ha, a_body_finish, ud)
        return 1
    var hb = _handle(tcb_addr, tid)
    _ = execute(hb, b_body_notify, ud)
    var ha = _handle(sc[].a_tcb[], sc[].a_tid[])
    resume_current(rt, ha)
    return 1


def dispatch_a3(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    """A3 double-wake: B notifies and wakes A TWICE from its slice."""
    var sc = ud.bitcast[SceneA]()
    if tid == sc[].a_tid[]:
        var ha = _handle(tcb_addr, tid)
        if sc[].slice_a[] == 0:
            claim_running(ha)
            _suspend_current(rt, ha)
            sc[].slice_a[] = 1
            sc[].waiting_seen[] = 1
        else:
            _ = execute(ha, a_body_finish, ud)
        return 1
    var hb = _handle(tcb_addr, tid)
    _ = execute(hb, b_body_notify, ud)
    var ha = _handle(sc[].a_tcb[], sc[].a_tid[])
    resume_current(rt, ha)
    resume_current(rt, ha)  # duplicate: RUNNABLE -> counted no-op
    return 1


# --- A4: N-task wake storm --------------------------------------------------

struct SceneStorm(ImplicitlyCopyable, ImplicitlyDeletable):
    """Storm scene: n@0, parked@1, done@2, counts@3 (per-slot slice
    counters, S slots), slot_map@(3+S) (tid -> slot, S entries)."""

    var n: UnsafePointer[Int, MutAnyOrigin]
    var parked: UnsafePointer[Int, MutAnyOrigin]
    var done: UnsafePointer[Int, MutAnyOrigin]
    var counts: UnsafePointer[Int, MutAnyOrigin]
    var slot_map: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.parked = self.n
        self.done = self.n
        self.counts = self.n
        self.slot_map = self.n


def storm_finish(ud: BytePtr) raises -> IntResult:
    var sc = ud.bitcast[SceneStorm]()
    sc[].done[] = sc[].done[] + 1
    return IntResult(1)


def dispatch_storm(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var sc = ud.bitcast[SceneStorm]()
    var slot = sc[].slot_map[tid]
    var h = _handle(tcb_addr, tid)
    if sc[].counts[slot] == 0:
        claim_running(h)
        _suspend_current(rt, h)
        sc[].counts[slot] = 1
        sc[].parked[] = sc[].parked[] + 1
    else:
        _ = execute(h, storm_finish, ud)
    return 1


# ===========================================================================
# PART B — race_hooks-style model over REAL TCBs
# ===========================================================================

struct HookPoint:
    comptime PREPARE = Int(0)
    comptime VALIDATE = Int(1)
    comptime COMMIT = Int(2)
    comptime WAKE = Int(3)
    comptime COUNT = Int(4)


struct HookAction:
    comptime NOOP = Int(0)
    comptime SET_READY = Int(1)
    comptime REQUEST_CANCEL = Int(2)
    comptime WAKE = Int(3)


struct ActionList(ImplicitlyCopyable, ImplicitlyDeletable):
    """Bounded action list (capacity 4) for one boundary."""
    comptime CAPACITY = Int(4)

    var _n: Int
    var _a0: Int
    var _a1: Int
    var _a2: Int
    var _a3: Int

    def __init__(out self):
        self._n = 0
        self._a0 = HookAction.NOOP
        self._a1 = HookAction.NOOP
        self._a2 = HookAction.NOOP
        self._a3 = HookAction.NOOP

    def push(mut self, action: Int) raises:
        if self._n >= Self.CAPACITY:
            raise Error("ActionList: capacity exceeded")
        if self._n == 0:
            self._a0 = action
        elif self._n == 1:
            self._a1 = action
        elif self._n == 2:
            self._a2 = action
        else:
            self._a3 = action
        self._n += 1

    def size(self) -> Int:
        return self._n

    def at(self, i: Int) -> Int:
        if i == 0:
            return self._a0
        if i == 1:
            return self._a1
        if i == 2:
            return self._a2
        if i == 3:
            return self._a3
        return HookAction.NOOP


struct HookScript(ImplicitlyCopyable, ImplicitlyDeletable):
    var prepare: ActionList
    var validate: ActionList
    var commit: ActionList
    var wake: ActionList

    def __init__(out self):
        self.prepare = ActionList()
        self.validate = ActionList()
        self.commit = ActionList()
        self.wake = ActionList()


def make_hook_script() -> HookScript:
    return HookScript()


def script_add(mut s: HookScript, point: Int, action: Int) raises:
    if point == HookPoint.PREPARE:
        s.prepare.push(action)
    elif point == HookPoint.VALIDATE:
        s.validate.push(action)
    elif point == HookPoint.COMMIT:
        s.commit.push(action)
    elif point == HookPoint.WAKE:
        s.wake.push(action)
    else:
        raise Error("script_add: unknown HookPoint")


def _list_at(s: HookScript, point: Int) -> ActionList:
    if point == HookPoint.PREPARE:
        return s.prepare
    if point == HookPoint.VALIDATE:
        return s.validate
    if point == HookPoint.COMMIT:
        return s.commit
    return s.wake


struct ModelResult(ImplicitlyCopyable, ImplicitlyDeletable):
    """Outcome + live state of one modeled park epoch (pointer-backed so b2
    by-value parameters share it with the caller)."""

    comptime NONE = Int(0)
    comptime READY = Int(1)
    comptime CANCEL = Int(2)

    var winner: Int
    var slept: Bool
    var attempts: Int
    var accepted: Int
    var enqueued_base: Int
    var ready: Bool
    var fired: InlineArray[Int, 4]

    def __init__(out self):
        self.winner = Self.NONE
        self.slept = False
        self.attempts = 0
        self.accepted = 0
        self.enqueued_base = 0
        self.ready = False
        self.fired = InlineArray[Int, 4](fill=0)


struct ModelCtx(ImplicitlyCopyable, ImplicitlyDeletable):
    """A modeled wait over REAL machinery: a CancelFlag cell + a
    ModelResult cell, both caller-owned and referenced by pointer (b2
    copies the pointers, not the pointees)."""

    var flag: UnsafePointer[CancelFlag, MutAnyOrigin]
    var state: UnsafePointer[ModelResult, MutAnyOrigin]

    def __init__(
        out self,
        flag: UnsafePointer[CancelFlag, MutAnyOrigin],
        state: UnsafePointer[ModelResult, MutAnyOrigin],
    ):
        self.flag = flag
        self.state = state


def _fire(
    mut ctx: ModelCtx,
    mut rt: Runtime,
    h: JoinHandle[IntResult],
    script: HookScript,
    point: Int,
) raises:
    var st = ctx.state
    st[].fired[point] = st[].fired[point] + 1
    var actions = _list_at(script, point)
    var i = 0
    while i < actions.size():
        var a = actions.at(i)
        if a == HookAction.SET_READY:
            st[].ready = True
        elif a == HookAction.REQUEST_CANCEL:
            ctx.flag[].request()
        elif a == HookAction.WAKE:
            _attempt_wake(ctx, rt, h)
        elif a != HookAction.NOOP:
            raise Error("_fire: unknown action")
        i += 1


def _attempt_wake(mut ctx: ModelCtx, mut rt: Runtime, h: JoinHandle[IntResult]) raises:
    """A real wake attempt: accepted iff the task is WAITING (delivers
    readiness once); a RUNNABLE task is a counted no-op (enqueue-once).  An
    ACCEPTED wake claims the READY winner slot if the race is undecided."""
    var st = ctx.state
    st[].attempts += 1
    if h.state() == TaskControlBlock.WAITING:
        resume_current(rt, h)
        st[].accepted += 1
        if st[].winner == ModelResult.NONE:
            st[].winner = ModelResult.READY


def _settle(mut ctx: ModelCtx, mut rt: Runtime, h: JoinHandle[IntResult]) raises:
    """Decide pending races at a boundary: cancellation first (spec), then
    readiness.  Exactly one winner; the winner claims the READY delivery."""
    var st = ctx.state
    if st[].winner != ModelResult.NONE:
        return
    if ctx.flag[].is_requested():
        st[].winner = ModelResult.CANCEL
        return
    if st[].ready:
        st[].winner = ModelResult.READY
        # Deliver readiness only when the task is already sleeping; a
        # not-yet-parked task observes readiness through its condition
        # re-check (the wake would be an enqueue-once no-op anyway).
        if h.state() == TaskControlBlock.WAITING:
            _attempt_wake(ctx, rt, h)


def run_park_pipeline(
    mut ctx: ModelCtx,
    mut rt: Runtime,
    h: JoinHandle[IntResult],
    script: HookScript,
) raises:
    """The canonical wait loop against a REAL spawned task:
    PREPARE -> settle -> VALIDATE -> settle -> COMMIT (real PARKING ->
    WAITING) -> settle -> WAKE -> settle -> final delivery.

    The task starts RUNNABLE; a task that never sleeps keeps RUNNABLE and
    is completed by the caller's drive with a re-checking body."""
    var st = ctx.state
    if h.state() != TaskControlBlock.RUNNABLE:
        raise Error("run_park_pipeline: task must start RUNNABLE")
    st[].enqueued_base = rt.enqueued()
    _fire(ctx, rt, h, script, HookPoint.PREPARE)
    _settle(ctx, rt, h)
    if st[].winner != ModelResult.NONE:
        return
    _fire(ctx, rt, h, script, HookPoint.VALIDATE)
    _settle(ctx, rt, h)
    if st[].winner != ModelResult.NONE:
        return
    # COMMIT: claim RUNNING, then the REAL park on the TCB state machine
    # (RUNNING -> PARKING -> WAITING).
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.WAITING)
    st[].slept = True
    _fire(ctx, rt, h, script, HookPoint.COMMIT)
    _settle(ctx, rt, h)
    if st[].winner != ModelResult.NONE:
        return
    _fire(ctx, rt, h, script, HookPoint.WAKE)
    _settle(ctx, rt, h)
    # Final delivery: an undecided wait is woken by the caller (baseline);
    # a CANCEL winner leaves the task parked (the cancel path owns it).
    if st[].winner == ModelResult.NONE and h.state() == TaskControlBlock.WAITING:
        _attempt_wake(ctx, rt, h)


def b_drive_body(ud: BytePtr) raises -> IntResult:
    """Trivial completion body for model tasks that never really waited."""
    return IntResult(7)


def b_drive(mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr) raises -> Int:
    var h = _handle(tcb_addr, tid)
    _ = execute(h, b_drive_body, ud)
    return 1


def main() raises:
    var rt = create()

    # ---------------- A1: wake-before-park ----------------------------------
    var buf = List[Int]()
    for _ in range(16):
        buf.append(0)
    var sca = SceneA()
    var bptr = buf.unsafe_ptr()
    sca.a_ready = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(bptr) + 0 * 8
    )
    sca.a_tcb = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(bptr) + 1 * 8)
    sca.a_tid = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(bptr) + 2 * 8)
    sca.b_tid = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(bptr) + 3 * 8)
    sca.waiting_seen = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(bptr) + 4 * 8
    )
    sca.ran = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(bptr) + 5 * 8)
    sca.slice_a = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(bptr) + 6 * 8
    )
    var sap = UnsafePointer[SceneA, MutAnyOrigin](to=sca)
    var ud_a = sap.bitcast[Byte]()

    # B is spawned FIRST (FIFO head): its slice is served before A's first
    # slice — the wake-before-park forcing.
    var tcb_b = TB.create()
    var h_b = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b), 0)
    var tcb_a = TB.create()
    var h_a = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a), 0)
    buf[3] = h_b.id()
    buf[2] = h_a.id()
    buf[1] = Int(UnsafePointer[TB, MutAnyOrigin](to=tcb_a))
    var served1 = scheduler_loop(rt, dispatch_a1, ud_a)
    if served1 != 2:
        red("A1 served " + String(served1) + " != 2")
    if rt.enqueued() != 2:
        red("A1 enqueued " + String(rt.enqueued())
            + " != 2 (the early wake must NOT enqueue)")
    if not h_a.is_completed():
        red("A1: A did not complete (wake-before-park lost readiness)")
    if not h_b.is_completed():
        red("A1: B did not complete")
    if buf[4] != 0:
        red("A1: A slept despite wake-before-park")
    if buf[5] != 2:
        red("A1: bodies did not run exactly once each")

    # ---------------- A2: park-then-wake ------------------------------------
    var tcb_a2 = TB.create()
    var h_a2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a2), 0)
    var tcb_b2 = TB.create()
    var h_b2 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b2), 0)
    buf[3] = h_b2.id()
    buf[2] = h_a2.id()
    buf[1] = Int(UnsafePointer[TB, MutAnyOrigin](to=tcb_a2))
    buf[6] = 0
    var served2 = scheduler_loop(rt, dispatch_a2, ud_a)
    # records: A(park slice), B(notify+wake slice), A(resume slice) = 3
    if served2 != 3:
        red("A2 served " + String(served2) + " != 3")
    # enqueues: 2 spawns + 1 accepted wake = 3
    if rt.enqueued() != 5:  # 2 (A1) + 2 spawns + 1 wake
        red("A2 enqueued " + String(rt.enqueued()) + " != 5")
    if not h_a2.is_completed():
        red("A2: woken A did not complete")
    if buf[4] != 1:
        red("A2: A did not park before the wake")
    if rt.skipped() != 0:
        red("A2: stale records skipped")

    # ---------------- A3: double-wake ---------------------------------------
    var tcb_a3 = TB.create()
    var h_a3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_a3), 0)
    var tcb_b3 = TB.create()
    var h_b3 = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_b3), 0)
    buf[3] = h_b3.id()
    buf[2] = h_a3.id()
    buf[1] = Int(UnsafePointer[TB, MutAnyOrigin](to=tcb_a3))
    buf[6] = 0
    var served3 = scheduler_loop(rt, dispatch_a3, ud_a)
    if served3 != 3:
        red("A3 served " + String(served3) + " != 3")
    if rt.enqueued() != 8:  # 5 (A1+A2) + 2 spawns + 1 wake
        red("A3 enqueued " + String(rt.enqueued())
            + " != 8 (the second wake must be a no-op)")
    if not h_a3.is_completed():
        red("A3: A did not complete after the double-wake")
    if rt.skipped() != 0:
        red("A3: stale records skipped")

    # ---------------- A4: 200-task wake storm --------------------------------
    comptime S = Int(200)
    var sbuf = List[Int]()
    for _ in range(3 + 2 * S + 8):
        sbuf.append(0)
    var bp = sbuf.unsafe_ptr()
    var storm = SceneStorm()
    storm.n = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(bp) + 0 * 8)
    storm.parked = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(bp) + 1 * 8
    )
    storm.done = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(bp) + 2 * 8)
    storm.counts = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(bp) + 3 * 8
    )
    storm.slot_map = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(bp) + (3 + S) * 8
    )
    sbuf[0] = S
    var spray = UnsafePointer[SceneStorm, MutAnyOrigin](to=storm)
    var ud_s = spray.bitcast[Byte]()

    var tcb_pool = List[TB]()
    for _ in range(S):
        tcb_pool.append(TB.create())
    var handles = List[JoinHandle[IntResult]]()
    for i in range(S):
        var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb_pool[i]), 0)
        handles.append(h)
        sbuf[3 + S + h.id()] = i  # tid -> slot (ids are 7..206 here)
    if rt.pending() != S:
        red("A4: expected " + String(S) + " queued records before the drive")

    var served4a = scheduler_loop(rt, dispatch_storm, ud_s)
    if served4a != S:
        red("A4 wave1 served " + String(served4a) + " != " + String(S))
    if sbuf[1] != S:
        red("A4: not all tasks parked (" + String(sbuf[1]) + ")")
    for i in range(S):
        if not handles[i].tcb()[].is_waiting():
            red("A4: task " + String(i) + " not WAITING after wave 1")

    # Reverse-order wake storm (slot S-1 down to 0).
    for i in range(S):
        var slot = S - 1 - i
        resume_current(rt, handles[slot])

    var served4b = scheduler_loop(rt, dispatch_storm, ud_s)
    if served4b != S:
        red("A4 wave2 served " + String(served4b) + " != " + String(S))
    if sbuf[2] != S:
        red("A4: not all tasks completed (" + String(sbuf[2]) + ")")
    for i in range(S):
        if not handles[i].is_completed():
            red("A4: task " + String(i) + " not COMPLETED")
    if rt.pending() != 0:
        red("A4: runnable queue not quiet after the storm")
    if rt.skipped() != 0:
        red("A4: stale records skipped during the storm")
    # Enqueue total across ALL Part A scenarios on `rt`:
    #   A1: 2 spawns; A2: 2 spawns + 1 wake; A3: 2 spawns + 1 wake;
    #   A4: S spawns + S wakes  ->  8 + 2*S.
    var expected = 8 + 2 * S
    if rt.enqueued() != expected:
        red("enqueued " + String(rt.enqueued()) + " != " + String(expected))

    # ================= PART B: race-hooks model over real TCBs ==============

    # --- B1: baseline — full pipeline, parks, wake accepted exactly once ----
    var rt2 = create()
    var f1 = make_cancel_flag()
    var st1 = ModelResult()
    var ctx1 = ModelCtx(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=f1),
        UnsafePointer[ModelResult, MutAnyOrigin](to=st1),
    )
    var t1 = TB.create()
    var h1 = spawn(rt2, UnsafePointer[TB, MutAnyOrigin](to=t1), 0)
    var script1 = make_hook_script()
    run_park_pipeline(ctx1, rt2, h1, script1)
    if not st1.slept:
        red("B1: baseline did not sleep")
    if st1.winner != ModelResult.READY:
        red("B1: the caller's final wake must claim readiness")
    if st1.attempts != 1 or st1.accepted != 1:
        red("B1: final delivery not accepted exactly once")
    if h1.state() != TaskControlBlock.RUNNABLE:
        red("B1: woken task must be RUNNABLE")
    # drive the woken task to completion (single record)
    var scratch1: Int = 0
    var ud1 = UnsafePointer[Int, MutAnyOrigin](to=scratch1).bitcast[Byte]()
    if scheduler_loop(rt2, b_drive, ud1) != 1:
        red("B1: woken task did not complete")
    if not h1.is_completed():
        red("B1: task not COMPLETED after drive")
    for p in range(HookPoint.COUNT):
        if st1.fired[p] != 1:
            red("B1: boundary " + String(p) + " fired " + String(st1.fired[p]))

    # --- B2: wake-before-park — readiness at VALIDATE, never sleeps ---------
    var rt3 = create()
    var f2 = make_cancel_flag()
    var st2 = ModelResult()
    var ctx2 = ModelCtx(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=f2),
        UnsafePointer[ModelResult, MutAnyOrigin](to=st2),
    )
    var t2 = TB.create()
    var h2 = spawn(rt3, UnsafePointer[TB, MutAnyOrigin](to=t2), 0)
    var script2 = make_hook_script()
    script_add(script2, HookPoint.VALIDATE, HookAction.SET_READY)
    run_park_pipeline(ctx2, rt3, h2, script2)
    if st2.winner != ModelResult.READY:
        red("B2: early readiness must win")
    if st2.slept:
        red("B2: pipeline must NOT sleep (wake-before-park)")
    if st2.attempts != 0 or st2.accepted != 0:
        red("B2: no wake attempt may be needed")
    if st2.fired[HookPoint.COMMIT] != 0:
        red("B2: COMMIT must never be reached")
    # the task stayed RUNNABLE; drive it to completion.
    var scratch2: Int = 0
    var ud2 = UnsafePointer[Int, MutAnyOrigin](to=scratch2).bitcast[Byte]()
    if scheduler_loop(rt3, b_drive, ud2) != 1:
        red("B2: awakened task did not complete")
    if not h2.is_completed():
        red("B2: awakened task not COMPLETED")

    # --- B3: double-wake — sleeps, 2 attempts, 1 accepted -------------------
    var rt4 = create()
    var f3 = make_cancel_flag()
    var st3 = ModelResult()
    var ctx3 = ModelCtx(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=f3),
        UnsafePointer[ModelResult, MutAnyOrigin](to=st3),
    )
    var t3 = TB.create()
    var h3 = spawn(rt4, UnsafePointer[TB, MutAnyOrigin](to=t3), 0)
    var script3 = make_hook_script()
    script_add(script3, HookPoint.COMMIT, HookAction.WAKE)
    script_add(script3, HookPoint.COMMIT, HookAction.WAKE)
    run_park_pipeline(ctx3, rt4, h3, script3)
    if not st3.slept:
        red("B3: must sleep before the wakes")
    if st3.attempts != 2:
        red("B3: two wake attempts expected")
    if st3.accepted != 1:
        red("B3: the second wake must be generation/enqueue-guarded")
    if st3.winner != ModelResult.READY:
        red("B3: readiness must win the race")
    if h3.state() != TaskControlBlock.RUNNABLE:
        red("B3: task must be RUNNABLE after one accepted wake")

    # --- B4: cancel-vs-ready — cancel wins, deterministically ---------------
    var script4 = make_hook_script()
    script_add(script4, HookPoint.PREPARE, HookAction.REQUEST_CANCEL)
    script_add(script4, HookPoint.PREPARE, HookAction.SET_READY)

    var rt5 = create()
    var f4 = make_cancel_flag()
    var st4 = ModelResult()
    var ctx4 = ModelCtx(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=f4),
        UnsafePointer[ModelResult, MutAnyOrigin](to=st4),
    )
    var t4 = TB.create()
    var h4 = spawn(rt5, UnsafePointer[TB, MutAnyOrigin](to=t4), 0)
    run_park_pipeline(ctx4, rt5, h4, script4)

    var rt6 = create()
    var f5 = make_cancel_flag()
    var st5 = ModelResult()
    var ctx5 = ModelCtx(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=f5),
        UnsafePointer[ModelResult, MutAnyOrigin](to=st5),
    )
    var t5 = TB.create()
    var h5 = spawn(rt6, UnsafePointer[TB, MutAnyOrigin](to=t5), 0)
    run_park_pipeline(ctx5, rt6, h5, script4)

    if st4.winner != ModelResult.CANCEL:
        red("B4: cancellation must win")
    if st5.winner != ModelResult.CANCEL:
        red("B4: identical script must decide identically")
    if st4.slept or st5.slept:
        red("B4: cancel-before-park must never sleep")
    if st4.accepted != 0 or st5.accepted != 0:
        red("B4: a cancelled wait must not be delivered readiness")
    if not f4.is_requested() or not f5.is_requested():
        red("B4: cancellation flag not latched")

    print("T12 stress (lost-wakeup): PASS")