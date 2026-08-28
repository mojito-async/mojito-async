# mojito_async/task.mojo
#
# A1.1 runtime (issue #33) — spawn + one-shot JoinHandle + the
# park/wake/join protocol over the runtime's runnable queue.
#
# Productionized from spike/colorless_runtime/spawn.mojo (A0.6, issue #15);
# semantics carried forward VERBATIM.  Proven IN-library (extern-free):
#   - spawn: TCB registration NEW -> RUNNABLE + FIFO enqueue (work-first
#     bookkeeping, spec §88);
#   - execute: a child's first entry WITHOUT a fiber — the generic hook
#     receives the task body as `[F: def(BytePtr) raises -> R]` plus its
#     userdata pointer (the S0 pattern), runs it on the current stack, and
#     settles COMPLETED with either a stored result or a preserved error;
#   - park protocol: RUNNING -> PARKING -> WAITING (generation bump via the
#     A0.5 TCB) and wake WAITING -> RUNNABLE + re-enqueue (once per epoch);
#   - JoinHandle one-shot join: double-join rejected, consume-once result
#     take, child-error re-raise with the message preserved, deterministic
#     single-destruction teardown for abandoned results.
#
# COOPERATIVE POLICY (spec §88): run()/execute are work-first — a task's
# first entry happens eagerly on the current worker; a task that must wait
# commits to PARKING/WAITING through the protocol and lets the scheduler
# dequeue-and-execute other RUNNABLE records.  INV: no OS-thread creation,
# no hidden blocking.
#
# Move-only handles: b2 has no linear types, so JoinHandle is implicitly
# copyable; SINGLE-OWNER semantics are enforced BY CONVENTION (docstring +
# one-shot flags): exactly one handle drives join/abandon.
#
# Mojo 1.0.0b2 (def-only) constraints honored (see runtime.mojo header).
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


# ---------------------------------------------------------------------------
# SuspendReason / park reason codes (shared by scheduler + sync)
# ---------------------------------------------------------------------------

struct SuspendReason:
    """Why a task waits — stamped on the embedded WaitNode.  Open set."""

    comptime NONE = Int(0)
    comptime YIELD = Int(1)
    comptime JOIN = Int(2)
    comptime TIMER = Int(5)
    comptime PARK = Int(3)
    comptime CANCEL = Int(4)


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

    def __init__(
        out self,
        tcb: UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin],
        id: Int,
    ):
        self._tcb = tcb
        self._id = id
        self._joined = False
        self._abandoned = False
        self._failed = False
        self._err = ""

    # --- queries ------------------------------------------------------------

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
        """One-shot gate.  Raises on ANY second consumption attempt: double
        join, or join after abandon.  ALSO refuses a child that has not yet
        reached COMPLETED — WITHOUT marking the handle joined — so a join
        attempted early does not burn the handle: once the child completes, a
        later join still works (the pending state is not consumed)."""
        if self._joined:
            raise Error(
                "JoinHandle.join: double join rejected (one-shot, spec INV-4)"
            )
        if self._abandoned:
            raise Error(
                "JoinHandle.join: result already abandoned; cannot join"
            )
        if not self.is_completed():
            raise Error(
                "JoinHandle.join: child not COMPLETED yet — drive the "
                "scheduler first; the pending join was NOT consumed"
            )
        self._joined = True

    def finish_join(mut self) raises -> Self.R:
        """Consume-once fast path: move the settled result out (or re-raise
        the preserved child error).  A pending (not-yet-COMPLETED) child has
        no in-library drive: mid-frame resumption needs the embedding
        scheduler loop, so this path raises descriptively instead of blocking
        the worker invisibly."""
        if not self.is_completed():
            raise Error(
                "JoinHandle.finish_join: child not COMPLETED yet — pending "
                "join requires the A1 scheduler loop (see module header); "
                "schedule/drive before finishing"
            )
        if self._failed:
            raise Error("child task failed: " + self._err)
        return self._tcb[].take_result()

    def finish_join_preserve(mut self) raises -> Self.R:
        """Synonym to keep spike-spawn compatibility naming."""
        return self.finish_join()

    def join(mut self) raises -> Self.R:
        """join(): begin + finish.  Fast paths (completed child, error
        propagation, double-join rejection) run entirely in-library."""
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

    The CALLER allocates the TCB cell (stack_allocation pattern).  Returns
    the single owner handle.  Work-first: spawn only REGISTERS the child as
    independently schedulable; its first entry happens when execute() (or a
    scheduler trampoline) reaches it.
    """
    if rt.is_shutdown():
        raise Error("mojito_async.spawn: runtime is shut down")
    tcb[].set_parent_id(parent_id)
    tcb[].transition(TaskControlBlock.RUNNABLE)
    var id = rt.next_id()
    rt.enqueue(Int(tcb), id)
    return JoinHandle[R](tcb, id)


def execute[R: ResultValue, F: def(BytePtr) raises -> R](
    mut h: JoinHandle[R],
    thunk: F,
    ud: BytePtr,
) raises:
    """Work-first FIRST ENTRY of a spawned child, fiber-free form.

    The generic hook statically knows the task body (b2 has no dynamic fn
    values): claim RUNNABLE -> RUNNING, invoke thunk(userdata) on the current
    stack, settle RUNNING -> COMPLETED and store the result — or, on a raise,
    still reach COMPLETED and preserve the message for join() to re-raise.
    """
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


def claim_running[R: ResultValue](
    h: JoinHandle[R],
) raises:
    """Owner-worker claim of a dequeued record: RUNNABLE -> RUNNING.  Called
    by the scheduler immediately BEFORE entering or resuming the task."""
    h.tcb()[].transition(TaskControlBlock.RUNNING)


def park_prepare[R: ResultValue](h: JoinHandle[R]) raises:
    """Park pipeline step 1: RUNNING -> PARKING (commit to stop using the
    worker after the current checkpoint)."""
    h.tcb()[].transition(TaskControlBlock.PARKING)


def park_commit[R: ResultValue](h: JoinHandle[R]) raises:
    """Park pipeline step 2: PARKING -> WAITING.  Claims a fresh wait epoch
    (generation bump — stale wakes from a previous epoch are rejected)."""
    h.tcb()[].transition(TaskControlBlock.WAITING)


def suspend_commit[R: ResultValue](h: JoinHandle[R]) raises:
    """A1 `_suspend_current` commit: RUNNING -> PARKING -> WAITING, atomically
    as far as the worker allows (a single suspension).  Equivalent to
    park_prepare(h); park_commit(h)."""
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.WAITING)


def wake[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Deliver readiness: WAITING -> RUNNABLE and re-enqueue (FIFO).  Waking
    keeps the task's original scheduler id; a wake to a non-WAITING task is
    an illegal transition and raises (never silently enqueued twice)."""
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue(Int(h.tcb()), h.id())


def abandon[R: ResultValue](mut h: JoinHandle[R]) raises:
    """Deterministic single destruction of an UNCONSUMED completed result.
    Validates the child is COMPLETED BEFORE marking the handle abandoned, so
    a failed validation leaves the handle fully usable (not burned).  Then
    replaces the TCB cell with a fresh TCB: the old value's destructor fires
    exactly once; a second abandon (or join-after-abandon) raises."""
    if not h.is_completed():
        raise Error("JoinHandle.abandon: child not COMPLETED")
    h.mark_abandoned()
    h.tcb()[] = TaskControlBlock[R]()