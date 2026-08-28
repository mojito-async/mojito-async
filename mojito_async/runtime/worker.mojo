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
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.vendor.mojito_sys import (
    NativeThread,
    NativeTlsKey,
    make_native_thread,
    tls_get,
)


struct Worker:
    """One worker = one Runtime + (A1) run/drive entry points + (A2.1)
    per-OS-thread identity: worker id, the owned NativeThread handle once
    the pool spawns it, and the current_worker TLS key this worker's thread
    writes at entry (issue #67).  The A1 motions are UNCHANGED: on ONE
    worker everything is cooperative — run the root task, or drive the
    runnable queue with a statically-known dispatcher.  A2.1 adds the
    OS-thread carrier; the scheduler_loop integration on the worker thread
    is the queue lane's (#68) E2-OWNED seam fill.
    """

    var _runtime: Runtime
    var _id: Int
    var _thread: NativeThread
    var _started: Bool
    var _tls_current_worker: NativeTlsKey

    def __init__(out self):
        self._runtime = create()
        self._id = 0
        self._thread = make_native_thread()
        self._started = False
        self._tls_current_worker = NativeTlsKey()

    def __init__(out self, id: Int):
        self._runtime = create()
        self._id = id
        self._thread = make_native_thread()
        self._started = False
        self._tls_current_worker = NativeTlsKey()

    # --- access ----------------------------------------------------------

    def runtime(mut self) -> ref Runtime:
        return self._runtime

    def handle(mut self) -> UnsafePointer[Runtime, MutAnyOrigin]:
        return UnsafePointer[Runtime, MutAnyOrigin](to=self._runtime)

    def id(mut self) -> Int:
        """Distinct per-worker id (0 for an unpooled A1 worker; the pool
        numbers its workers 0..N-1)."""
        return self._id

    def thread(mut self) -> NativeThread:
        """The OS thread this worker runs on once the pool spawned it."""
        return self._thread

    def started(mut self) -> Bool:
        """True once the pool spawned this worker's OS thread."""
        return self._started

    def tls_key(mut self) -> NativeTlsKey:
        return self._tls_current_worker

    def mark_started(mut self, t: NativeThread, key: NativeTlsKey):
        """Pool-owned: bind the spawned thread + the current_worker TLS key."""
        self._thread = t
        self._tls_current_worker = key
        self._started = True

    def tls_worker_ptr(mut self) -> BytePtr:
        """Read THIS OS thread's current_worker slot (coarse entry-only
        value; address 0 when unset).  Only the worker thread itself reads
        its own slot — the pool side never calls this."""
        return tls_get(self._tls_current_worker)

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
