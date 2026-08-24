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
# Dup-enqueue protection: push(t) raises if an element equal to t is already
# queued. The guard is a linear O(n) contains() scan over the live elements —
# documented as spike-appropriate (a correct, boring guard for a small
# runnable/scheduling queue; O(1) dedup needs a hash structure that a spike
# does not justify). pop() raises on an empty queue; clear() drops the queue
# and leaves it reusable (a subsequent pop re-raises).
#
# Type parameters (b2-callable, monotone-increasing):
#   Movable             - Deque stores its elements; pop transfers one out.
#   ImplicitlyDeletable - Deque droppped on clear/scope exit.
#   Equatable           - the dup-enqueue == scan.
#   ImplicitlyCopyable  - the contains() probe compares by value.
# SizedRaising         - enables the builtin len().
from std.collections import Deque

struct FifoQueue[
    T: Movable & ImplicitlyDeletable & Equatable & ImplicitlyCopyable
](SizedRaising):
    var data: Deque[Self.T]

    def __init__(out self):
        self.data = Deque[Self.T]()

    def __len__(self) -> Int:
        return len(self.data)

    def is_empty(self) -> Bool:
        return len(self.data) == 0

    # Linear dup-enqueue guard (spike-appropriate; see module docstring).
    def _contains(self, t: Self.T) -> Bool:
        var n = len(self.data)
        var i = 0
        while i < n:
            if self.data[i] == t:
                return True
            i += 1
        return False

    def push(mut self, t: Self.T) raises:
        if self._contains(t):
            raise Error("FifoQueue: duplicate enqueue rejected (element already queued)")
        self.data.append(t)

    def pop(mut self) raises -> Self.T:
        if self.is_empty():
            raise Error("FifoQueue: pop from an empty queue")
        return self.data.popleft()

    def clear(mut self) raises:
        while not self.is_empty():
            _ = self.data.popleft()