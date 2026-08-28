# mojito_async/task.mojo
#
# A1.1 runtime (issue #33) — spawn + execute + claim_running + abandon over
# the runtime's runnable queue.
#
# Productionized from spike/colorless_runtime/spawn.mojo (A0.6, issue #15);
# semantics carried forward VERBATIM.  Proven IN-library (extern-free):
#   - spawn: TCB registration NEW -> RUNNABLE + FIFO enqueue (work-first
#     bookkeeping, spec §88);
#   - execute: a child's first entry WITHOUT a fiber — the generic hook
#     receives the task body as `[F: def(BytePtr) raises -> R]` plus its
#     userdata pointer (the S0 pattern), runs it on the current stack, and
#     settles COMPLETED with either a stored result or a preserved error;
#   - claim_running: the owner-worker claim of a dequeued record
#     (RUNNABLE -> RUNNING) before entering/resuming the task;
#   - abandon: deterministic single destruction of an unconsumed result.
#
# A1 follow-up (issue #40): spawn is SCOPE-AWARE.  The scoped spelling is
# now the SCOPE METHOD `scope.spawn[T](rt, tcb, parent_id) -> JoinHandle[T]`
# on the #42 non-generic Scope (mojito_async/scope.mojo, issue #61): it
# AUTO-REGISTERS the child inside spawn (INV-3 becomes structure, not
# convention) and returns the typed handle.  The module-level SCOPED
# overload is DELETED with the generic Scope (scope.mojo header carries the
# #42 decision summary).  The un-scoped overload remains (b2 has no TLS and
# the runtime carries only a Integer scope handle, not the registry pointer,
# so the scope must be threaded explicitly; un-scoped callers own their
# scoping by convention).
#
# execute() (A3.2, issue #61/#42) ALSO stamps the child's R-free TCB_Prefix
# failure record (mark_failed) in parallel with the JoinHandle-level fail(),
# so the non-generic Scope's erased registry (and the #63/#64 grouped-join
# lanes) can see per-child failure without knowing the child's type.
#
# COOPERATIVE POLICY (spec §88): run()/execute are work-first — a task's
# first entry happens eagerly on the current worker; a task that must wait
# commits to PARKING/WAITING through runtime.park and lets the scheduler
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
from mojito_async.runtime.join_handle import JoinHandle, SuspendReason


def spawn[R: ResultValue](
    mut rt: Runtime,
    tcb: UnsafePointer[TaskControlBlock[R], MutAnyOrigin],
    parent_id: Int,
) raises -> JoinHandle[R]:
    """Register a NEW task: link its parent, walk NEW -> RUNNABLE, enqueue.

    The CALLER allocates the TCB cell (stack_allocation pattern) and owns
    the SCOPE BOOKKEEPING by convention (the INV-3-scoped spelling is the
    #42 Scope method `scope.spawn[T](rt, tcb, parent_id)`, which
    auto-registers and returns the typed JoinHandle[T]).  Returns the
    single owner handle.  Work-first: spawn only REGISTERS the child as
    runnable; its first entry happens when execute() or a scheduler
    trampoline reaches it.
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
    still reach COMPLETED, preserve the message for join() to re-raise, AND
    stamp the R-free TCB_Prefix failure record (mark_failed) so the erased
    registry sees the failure without knowing T (A3.2, #42).
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
        h.tcb()[].mark_failed(String(e))


def claim_running[R: ResultValue](
    h: JoinHandle[R],
) raises:
    """Owner-worker claim of a dequeued record: RUNNABLE -> RUNNING.  Called
    by the scheduler immediately BEFORE entering or resuming the task."""
    h.tcb()[].transition(TaskControlBlock.RUNNING)


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