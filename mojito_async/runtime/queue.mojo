# mojito_async/runtime/queue.mojo
#
# A1.1 runtime (issue #33) — FIFO runnable queue + task record payload.
#
# Productionized from spike/colorless_runtime/queue.mojo (A0.5, issue #14)
# and spike/colorless_runtime/runtime.mojo's TaskRecord (A0.6, issue #15),
# with module paths moved from the spike top-level into mojito_async.runtime.
# Semantics are carried forward VERBATIM; this is a rename/relayout, not a
# redesign:
#
#   - struct FifoQueue[T] with push/pop/len/is_empty/clear.  This is the
#     runnable-task queue for the colorless single-worker scheduler: ONE
#     worker enqueues/dequeues on its own thread, so there is deliberately
#     no mutex, no condvar, and (per lane A1.2 non-goals) no lock-free
#     structure and no work stealing.  std.collections.Deque provides an
#     allocation-free popleft() on a ring buffer with amortized copy-free
#     growth on push.
#   - pop() raises on an empty queue; clear() drains and leaves the queue
#     reusable; drained capacity is retained (high-water mark).
#
# TaskRecord is the type-erased runnable-task payload (DATA only — inject
# schedules, spec §21 P0).  b2 forbids heterogeneous thunks behind one queue
# element type (function-typed fields are rejected), so the record carries
# the ADDRESS of the task's caller-allocated TaskControlBlock cell plus its
# scheduler id.  Executing an unstarted record is a GENERIC operation
# performed at a call site that statically knows the task body (runtime.run
# for roots, task.execute for children, or a driver's dispatch) — never
# dynamic dispatch through this record.
#
# Mojo 1.0.0b2 (def-only) constraints honored here: `def` only; no `fn`, no
# static methods (module-level factories instead); parameters pass by
# immutable value (so the payload is Movable & ImplicitlyCopyable at the
# call boundary, while the storage path is move-based).
from std.collections import Deque


struct TaskRecord(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Type-erased runnable-task record (data only).

    tcb_addr — address of the task's caller-allocated TaskControlBlock cell.
    task_id  — the scheduler id that enqueued it (parent or wake).
    """

    var tcb_addr: Int
    var task_id: Int

    def __init__(out self, tcb_addr: Int, task_id: Int):
        self.tcb_addr = tcb_addr
        self.task_id = task_id


struct FifoQueue[T: Movable & ImplicitlyDeletable & ImplicitlyCopyable](Sized):
    """FIFO runnable-task queue.  Sized so `len(self)` works."""

    var data: Deque[Self.T]

    def __init__(out self):
        self.data = Deque[Self.T]()

    def __len__(self) -> Int:
        return len(self.data)

    def is_empty(self) -> Bool:
        return len(self.data) == 0

    def push(mut self, t: Self.T):
        self.data.append(t)

    def pop(mut self) raises -> Self.T:
        if self.is_empty():
            raise Error("FifoQueue: pop from an empty queue")
        return self.data.popleft()

    def clear(mut self) raises:
        while not self.is_empty():
            _ = self.data.popleft()