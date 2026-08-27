# mojito_async/runtime/parking_lot.mojo
#
# A1.1 runtime (issue #33) — the parking lot: park/unpark vocabulary over the
# TCB parked-state machinery (spec §24 "ParkingLot / cancellation"; see
# task_control_block.mojo's embedded WaitNode for the allocation-free wait
# cell).  On the single worker this is the mapping between "the running task
# wants to wait" and "readiness delivered once per epoch".
#
# Execution discipline: park_current commits the task (RUNNING -> PARKING ->
# WAITING); unpark delivers readiness once (WAITING -> RUNNABLE + re-enqueue).
# Both require a caller-allocated JoinHandle whose TCB is in the right state:
# park raises IllegalTransitionError if the task is not RUNNING, unpark if it
# is not WAITING.  No hidden allocation, no OS-thread synchronization.
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.task import JoinHandle, SuspendReason


struct ParkingLot:
    """Owns the parking discipline of one worker's caller-supplied tasks."""

    def park[R: ResultValue](mut self, mut rt: Runtime, h: JoinHandle[R]) raises:
        """Commission: RUNNING -> PARKING -> WAITING (fresh epoch)."""
        h.tcb()[].transition(TaskControlBlock.PARKING)
        h.tcb()[].transition(TaskControlBlock.WAITING)

    def unpark_once[R: ResultValue](
        mut self, mut rt: Runtime, h: JoinHandle[R]
    ) raises:
        """Deliver readiness exactly once: WAITING -> RUNNABLE + re-enqueue."""
        h.tcb()[].transition(TaskControlBlock.RUNNABLE)
        rt.enqueue(Int(h.tcb()), h.id())


def park_current[R: ResultValue](mut rt : Runtime, h: JoinHandle[R]) raises:
    """Module-level park: cooperative suspend of the current task."""
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].transition(TaskControlBlock.WAITING)


def unpark_current[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Module-level unpark: wake one parked task (once per epoch)."""
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)
    rt.enqueue(Int(h.tcb()), h.id())