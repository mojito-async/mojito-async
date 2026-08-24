# spike/colorless_runtime/spawn.mojo
#
# A0.6 (issue #15) — spawn + one-shot JoinHandle + the park/wake/join
# protocol over the runtime's runnable queue.
#
# WHAT IS PROVEN HERE (spike-honest, per Main's lane brief)
# ---------------------------------------------------------
# b2 toolchain bug modular/modular#6971: extern calls lowered inside an
# IMPORTED module (JIT and AOT) miscompile — so ms_ctx_make/ms_ctx_switch/
# ms_stack_alloc can NOT live in any call path of this module.  This module
# is deliberately EXTERN-FREE pure Mojo; it compiles and runs correctly when
# imported by `mojo run` drivers and `mojo build` AOT drivers alike.
#
# Proven IN-library (no externs needed):
#   - spawn: TCB registration NEW -> RUNNABLE + FIFO enqueue (work-first
#     bookkeeping, spec §88);
#   - execute: a child's first entry WITHOUT a fiber — the generic hook
#     receives the task body as `[F: def(BytePtr) raises -> R]` plus its
#     userdata pointer (the S0 pattern), runs it on the current stack, and
#     settles COMPLETED with either a stored result or a preserved error;
#   - park protocol: RUNNING -> PARKING -> WAITING (generation bump via the
#     A0.5 TCB) and wake WAITING -> RUNNABLE + re-enqueue;
#   - JoinHandle one-shot join: double-join rejected, consume-once result
#     take, child-error re-raise with the message preserved, deterministic
#     single-destruction teardown for abandoned results.
#
# Proven DRIVER-side (exact-resume fibers; see tests/t2_worker_reuse_aot.mojo
# mirroring tests/t4_fiber.mojo):
#   - mid-frame suspension of a started task on a synthetic stack and exact
#     resumption at the suspension point (ms_ctx_switch choreography inline
#     in the driver module — the only b2-safe place for those externs);
#   - worker reuse while parked: another task executes on the SAME OS thread
#     between a task's park and its wake.
#
# COOPERATIVE POLICY (spec §88): run()/execute are work-first — a task's
# first entry happens eagerly on the current worker; a task that must wait
# commits to PARKING/WAITING through the protocol below and lets the driver
# loop dequeue-and-execute other RUNNABLE records (recursion depth =
# scheduling nesting).  INV: no OS-thread creation; no hidden blocking.
#
# Move-only handles: b2 has no linear types, so JoinHandle is implicitly
# copyable at the language level; SINGLE-OWNER semantics are enforced BY
# CONVENTION (docstring + one-shot flags): exactly one handle per spawned
# task drives join/abandon.  Copying a handle and consuming through both
# copies is a programming error the flags cannot catch across copies.
from task import TaskControlBlock, ResultValue
from runtime import Runtime
from mojito_spike import BytePtr


# ---------------------------------------------------------------------------
# JoinHandle[R]
# ---------------------------------------------------------------------------

struct JoinHandle[R: ResultValue](ImplicitlyCopyable, ImplicitlyDeletable):
    """One-shot handle to a spawned task's outcome (spec §9.1, INV-4).

    Holds the address of the task's caller-allocated TaskControlBlock cell
    plus the consumption bookkeeping:
      _joined    — join() consumed (or was attempted); second join raises.
      _abandoned — abandon() released the result; later join raises.
      _failed    — the child raised; _err carries the PRESERVED message that
                   join() re-raises.
    """

    var _tcb: UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin]
    var _id: Int
    var _joined: Bool
    var _abandoned: Bool
    var _failed: Bool
    var _err: String

    def __init__(out self, tcb: UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin], id: Int):
        self._tcb = tcb
        self._id = id
        self._joined = False
        self._abandoned = False
        self._failed = False
        self._err = ""

    # --- queries -------------------------------------------------------------

    def id(self) -> Int:
        return self._id

    def tcb(self) -> UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin]:
        """Address of the underlying TCB cell (driver/fiber choreography)."""
        return self._tcb

    def state(self) -> Int:
        return self._tcb[].state()

    def is_completed(self) -> Bool:
        return self._tcb[].is_completed()

    def is_cancelled(self) -> Bool:
        return self._tcb[].is_cancelled()

    def is_failed(self) -> Bool:
        return self._failed

    def error(self) -> String:
        return self._err

    # --- consumption ---------------------------------------------------------

    def begin_join(mut self) raises:
        """One-shot gate (A0-T6).  Raises on ANY second consumption attempt:
        double join, or join after abandon."""
        if self._joined:
            raise Error(
                "JoinHandle.join: double join rejected (one-shot, spec INV-4)"
            )
        if self._abandoned:
            raise Error(
                "JoinHandle.join: result already abandoned; cannot join"
            )
        self._joined = True

    def finish_join(mut self) raises -> Self.R:
        """Consume-once fast path: move the settled result out (or re-raise
        the preserved child error).  A pending (not-yet-COMPLETED) child has
        no in-library drive: mid-frame resumption needs the embedding
        driver's fiber loop, so this path raises descriptively instead of
        blocking the worker invisibly."""
        if not self.is_completed():
            raise Error(
                "JoinHandle.finish_join: child not COMPLETED yet — pending "
                "join requires the driver's fiber/schedule loop (see module "
                "header); poll or drive before finishing"
            )
        if self._failed:
            raise Error("child task failed: " + self._err)
        return self._tcb[].take_result()

    def join(mut self) raises -> Self.R:
        """join(): begin + finish.  Fast paths (completed child, error
        propagation, double-join rejection) run entirely in-library; waiting
        for an unfinished child is expressed through the park/wake protocol
        by the embedding scheduler loop."""
        self.begin_join()
        return self.finish_join()

    def fail(mut self, msg: String):
        """Record a preserved child error (called by execute()'s handler)."""
        self._failed = True
        self._err = msg

    def mark_abandoned(mut self) raises:
        if self._abandoned or self._joined:
            raise Error("JoinHandle.abandon: result already consumed")
        self._abandoned = True


# ---------------------------------------------------------------------------
# spawn / execute / park-wake protocol / abandon
# ---------------------------------------------------------------------------

def spawn[R: ResultValue](
    mut rt: Runtime,
    tcb: UnsafePointer[TaskControlBlock[R], MutAnyOrigin],
    parent_id: Int,
) raises -> JoinHandle[R]:
    """Register a NEW task: link its parent, walk NEW -> RUNNABLE, enqueue.

    The CALLER allocates the TCB cell (stack_allocation pattern — mirrors
    fiber.create's caller-allocates convention; keeps this module
    allocation-mechanism-free and extern-free).

    Returns the single owner handle.  Work-first note (spec §88): spawn only
    REGISTERS the child as independently schedulable; its first entry happens
    when the scheduler/driver reaches it (execute() here, or a fiber trampoline).
    """
    if rt.is_shutdown():
        raise Error("spawn: runtime is shut down")
    tcb[].set_parent_id(parent_id)
    tcb[].transition(TaskControlBlock.RUNNABLE)
    var id = rt.next_id()
    rt.enqueue(Int(tcb), id)
    return JoinHandle[R](tcb, id)


def execute[R: ResultValue, F: def(BytePtr) raises -> R](
    rt: Runtime,
    mut h: JoinHandle[R],
    thunk: F,
    ud: BytePtr,
) raises:
    """Work-first FIRST ENTRY of a spawned child, fiber-free form.

    The generic hook statically knows the task body (b2 has no dynamic fn
    values): claim RUNNABLE -> RUNNING, invoke thunk(userdata) on the current
    stack, settle RUNNING -> COMPLETED and store the result — or, on a raise,
    still reach COMPLETED and preserve the message for join() to re-raise
    (A0-T8).  The userdata pointer is the S0 context channel (module-level
    bodies cannot capture enclosing locals in b2).

    The exact-resume FIBER equivalent of this primitive — a body suspending
    MID-FRAME and being resumed at the same point on the same thread — lives
    in the driver (tests/t2_worker_reuse_aot.mojo, t4_fiber.mojo shape).
    """
    _ = rt
    if h.is_completed():
        raise Error("execute: task already completed")
    h.tcb()[].transition(TaskControlBlock.RUNNING)
    try:
        var res = thunk(ud)
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(res)
    except e:
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.fail(String(e))


def claim_running[R: ResultValue](h: JoinHandle[R]) raises:
    """Owner-worker claim of a dequeued record: RUNNABLE -> RUNNING.
    Called by the scheduler/driver immediately BEFORE entering (first entry)
    or resuming (fiber switch) the task."""
    h.tcb()[].transition(TaskControlBlock.RUNNING)


def park_prepare[R: ResultValue](h: JoinHandle[R]) raises:
    """Park pipeline step 1: RUNNING -> PARKING (the running task commits to
    stop using the worker after its current checkpoint)."""
    h.tcb()[].transition(TaskControlBlock.PARKING)


def park_commit[R: ResultValue](h: JoinHandle[R]) raises:
    """Park pipeline step 2: PARKING -> WAITING.  Claims a fresh wait epoch
    (TCB generation bump — stale wakes from the previous epoch are rejected
    by construction of the A0.5 machine)."""
    h.tcb()[].transition(TaskControlBlock.WAITING)

def wake[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Deliver readiness: WAITING -> RUNNABLE and re-enqueue (FIFO).
    Waking keeps the task's original scheduler id."""
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue(Int(h.tcb()), h.id())


def abandon[R: ResultValue](mut h: JoinHandle[R]) raises:
    """Deterministic single destruction of an UNCONSUMED completed result
    (A0-T7).  Replaces the TCB cell contents with a fresh TCB: the old
    value's destructor chain fires exactly once, destroying the stored
    result; a second abandon (or join-after-abandon) raises.  Consumed
    results (take_result already moved the value out, clearing the slot
    flag) are untouched — no double destruction."""
    h.mark_abandoned()
    if not h.is_completed():
        raise Error("JoinHandle.abandon: child not COMPLETED")
    # Replace the cell contents with a fresh TCB: the assignment destroys
    # the old value, whose destructor chain fires exactly once and takes the
    # stored result (its slot flag) with it.  A consumed slot (take_result
    # already cleared _has_result) is untouched by this teardown.
    h.tcb()[] = TaskControlBlock[R]()
