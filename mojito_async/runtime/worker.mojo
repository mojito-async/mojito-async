# mojito_async/runtime/worker.mojo
#
# A1.1 runtime (issue #33) — the single worker and its drive surface.
#
# `Worker` names the "calling thread IS the worker" invariant (spec §22:
# scheduler re-entry; b2 has no TLS, so the worker is an explicit value the
# caller drives rather than thread-local state).  On ONE worker everything
# is cooperative: a Worker owns a Runtime and offers the two motions the
# lanes/drivers need — run the root task, and drive the runnable queue with
# a statically-known dispatcher (scheduler_loop).  It deliberately does NOT
# own task bodies (b2 cannot store them); callers pass dispatchers where
# bodies are known, exactly like the spike's drivers.
#
# Extern-free, allocation-discipline kept: the Worker holds no task storage;
# every TaskControlBlock cell is caller-allocated.
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.integration.sys import BytePtr


struct Worker:
    """One cooperative worker = one Runtime + run/drive entry points."""

    var _runtime: Runtime

    def __init__(out self):
        self._runtime = create()

    # --- access ----------------------------------------------------------

    def runtime(mut self) -> ref Runtime:
        return self._runtime

    def handle(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    # --- entry points -------------------------------------------------------

    def run_root[T: def() -> None](mut self, task: T) raises:
        """Execute the ROOT task synchronously on this worker (runtime.run)."""
        self._runtime.run(task)

    def drive[F: def(Int, Int, BytePtr) raises -> Int](
        mut self, dispatcher: F, ud: BytePtr
    ) raises -> Int:
        """Single-worker scheduler loop: runs the runnable queue to quiet.
        Returns the number of records driven."""
        return scheduler_loop(self._runtime, dispatcher, ud)

    def shutdown(mut self) -> None:
        self._runtime.shutdown()


# Module-level factory (b2 has no static methods).
def make_worker() -> Worker:
    return Worker()