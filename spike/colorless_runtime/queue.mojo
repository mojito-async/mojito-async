# A0.5 (issue #14) — pure-Mojo FIFO runnable queue (single worker).
#
# Spec §100 A0.5: struct FifoQueue[T] with push/pop/len/is_empty/clear.
# This is the runnable-task queue for the colorless scheduler: ONE worker
# enqueues/dequeues on its own thread, so there is deliberately no mutex,
# no condvar, and (per lane non-goals) NO lock-free structure and NO work
# stealing. The plan fronts an "allocation-free pop path" for scheduling;
# std.collections.Deque gives us an allocation-free popleft() on a ring
# buffer, with amortized copy-free growth on push.
#
# Dup-enqueue protection: REVIEW-REMOVED — value-equality scanning cannot
# express task-identity enqueue-once (A0.6 WAKE); identity dedup belongs to
# the scheduler/Event generation claim (A0.7). FifoQueue is a payload-neutral
# FIFO (Movable only). pop() raises on an empty queue; clear() drains and
# leaves the queue reusable (subsequent pop re-raises); drained capacity is
# retained (high-water mark) — deliberate for the spike, noted for A0.10.
#
#
# ImplicitlyCopyable is required only because b2 `def` parameters are
# passed by value (copy) at the call boundary; the push/pop storage path
# itself is move-based (Deque append moves, popleft moves out). No element
# is ever compared or copied inside the queue.
from std.collections import Deque

struct FifoQueue[T: Movable & ImplicitlyDeletable & ImplicitlyCopyable](Sized):
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