# spike/colorless_runtime/runtime.mojo
#
# A0.2 — minimal Runtime skeleton (issue #11).
#
# Deliberately NOT a real runtime yet.  This file carries the A0.6 compile
# surface the new drivers need (runnable-queue services, shutdown flag,
# counters); every behavioral entry point raises "not implemented", so the
# drivers print RED rather than FAIL.  The real one-worker scheduler lands
# in this same PR (issue #15).
#
# Mojo 1.0.0b2 (def-only) constraints honored here:
#   - no `fn`, no `async`/`await`/`Future`, no module-level mutable globals;
#   - first-class `def` values are nominal, so the task argument is taken as
#     a generic constrained to the `def()` callable trait;
#   - b2 has no static methods inside structs: module-level `create()` is
#     the constructor surface.
#
# Safety invariant: run() must NEVER silently accept work it cannot schedule.
from task import ResultValue

# --- compile-surface types (real definitions land with the A0.6 impl) ------

struct Nil(ResultValue):
    var _tag: Int
    def __init__(out self): self._tag = 0


struct TaskRecord(ImplicitlyCopyable, ImplicitlyDeletable):
    var tcb_addr: Int
    var task_id: Int
    def __init__(out self, tcb_addr: Int, task_id: Int):
        self.tcb_addr = tcb_addr
        self.task_id = task_id


struct Runtime:
    comptime NO_SCOPE = Int(0)

    var _shutdown: Bool

    def __init__(out self):
        self._shutdown = False

    # --- root-task execution -------------------------------------------------
    def run[T: def() -> None](self, task: T) raises:
        _ = task  # deliberately unused while run() is a stub
        raise Error("runtime.run: not implemented yet (A0.4/A0.6)")

    # --- runnable-queue services (A0.6 surface) ------------------------------
    def next_id(...)
        raise Error("runtime.next_id: not implemented (A0.6)")

    def enqueue(mut self, tcb_addr: Int, task_id: Int) raises:
        raise Error("runtime.enqueue: not implemented (A0.6)")

    def pop_ready(mut self) raises -> TaskRecord:
        raise Error("runtime.pop_ready: not implemented (A0.6)")

    def has_ready(self) raises -> Bool:
        raise Error("runtime.has_ready: not implemented (A0.6)")

    def pending(self) raises -> Int:
        raise Error("runtime.pending: not implemented (A0.6)")

    def tasks_started(self) raises -> Int:
        raise Error("runtime.tasks_started: not implemented (A0.6)")

    def tasks_completed(self) raises -> Int:
        raise Error("runtime.tasks_completed: not implemented (A0.6)")

    def enqueued(self) raises -> Int:
        raise Error("runtime.enqueued: not implemented (A0.6)")

    def scope_handle(self) raises -> Int:
        raise Error("runtime.scope_handle: not implemented (A0.6)")

    # --- lifecycle -----------------------------------------------------------
    def shutdown(mut self) -> None:
        self._shutdown = True

    def is_shutdown(self) -> Bool:
        return self._shutdown


# Module-level factory (b2 has no static methods). Mirrors the mojito-sys
# convention of exposing module-level constructors.
def create() -> Runtime:
    return Runtime()