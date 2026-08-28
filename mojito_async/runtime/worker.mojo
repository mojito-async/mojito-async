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
#
# A2.2 (issue #68) — per-worker run-queue accessors (E2-owned).  The queue
# STATE lives on this worker's Runtime (_local LocalDeque + _remote
# RemoteReadyQueue); Worker exposes thin refs so callers can probe/drive
# them without reaching into Runtime internals.
#
# # E1-OWNED: the WORKER POOL, thread_entry, NativeThread and TlsKey
# bindings are the sibling lane's (issue #67).  The entry points below
# (run_root/drive/shutdown) stay untouched by this lane.
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.queue import LocalDeque, RemoteReadyQueue
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop


struct Worker:
    """One cooperative worker = one Runtime + run/drive entry points."""

    var _runtime: Runtime

    def __init__(out self):
        self._runtime = create()

    # --- access ----------------------------------------------------------

    def runtime(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        """This worker's Runtime (b2 pointer-return idiom; deref at the call
        site — `w.runtime()[].enqueue_local(...)`)."""
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    def handle(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    # --- A2.2 per-worker run queues (issue #68) — E2-owned accessors ------

    def local_queue(mut self) -> UnsafePointer[LocalDeque, MutAnyOrigin]:
        """This worker's LOCAL runnable deque (owner push/pop; the scheduler
        drains it before the remote queue, spec §21)."""
        return self._runtime.local_queue()

    def remote_queue(mut self) -> UnsafePointer[RemoteReadyQueue, MutAnyOrigin]:
        """This worker's REMOTE-ready queue (any worker pushes a wake, the
        owner pops — spec §19.2)."""
        return self._runtime.remote_queue()

    # --- entry points -------------------------------------------------------

    def run_root[T: def() raises -> None](mut self, task: T) raises:
        """Execute the ROOT task synchronously on this worker (runtime.run):
        full TCB lifecycle on the calling thread, errors preserved and
        re-raised."""
        self._runtime.run(task)

    def drive[F: def(mut Runtime, Int, Int, BytePtr) raises -> Int](
        mut self, dispatcher: F, ud: BytePtr
    ) raises -> Int:
        """Single-worker scheduler loop: drives the runnable queue to quiet
        with the given statically-known dispatcher (same bound as
        scheduler_loop).  Returns the number of records served."""
        return scheduler_loop(self._runtime, dispatcher, ud)

    def shutdown(mut self) -> None:
        self._runtime.shutdown()


# Module-level factory (b2 has no static methods).
def make_worker() -> Worker:
    return Worker()