# mojito_async/runtime/join_handle.mojo
#
# A1 follow-up (issue #39) — the ONE JoinHandle + SuspendReason definition,
# relocated from mojito_async/task.mojo into the runtime package so the
# park/wake protocol and its result handle share one home (the 4-lens A1.1
# panel: the runtime kernel must not depend on the top-level public task
# module).  `mojito_async.task` re-exports both names, so every existing
# import path (`from mojito_async.task import JoinHandle, SuspendReason`)
# keeps working unchanged; the ROOT package re-exports JoinHandle too.
#
# Semantics are carried forward VERBATIM from the A1.1 task.mojo (issue #33):
# a one-shot, implicitly-copyable-but-single-owner handle to a spawned task's
# outcome (spec §9.1, INV-4), plus the open-set wait-reason codes stamped on
# the TCB's embedded WaitNode (spec §24/§25).
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock


# ---------------------------------------------------------------------------
# SuspendReason / park reason codes (shared by scheduler + sync + channel +
# timer)
# ---------------------------------------------------------------------------

struct SuspendReason:
    """Why a task waits — stamped on the embedded WaitNode.  Open set."""

    comptime NONE = Int(0)
    comptime YIELD = Int(1)
    comptime JOIN = Int(2)
    comptime PARK = Int(3)
    comptime CANCEL = Int(4)
    comptime TIMER = Int(5)


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
