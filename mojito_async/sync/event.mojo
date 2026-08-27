# mojito_async/sync/event.mojo
#
# A1.1 runtime (issue #33) — internal park/wake Event (readiness cell).
#
# Spec §6 sync/event.mojo.  For A1.1 this is the INTERNAL park/wake seam the
# scheduler/parking_lot reference: a minimal waitable readiness cell with a
# sticky latch.  It is deliberately NOT a public synchronization primitive —
# Mutex/Semaphore/Channel/timer heap belong to sibling lanes (A1.2-A1.4) and
# are NOT built here.  No hidden allocation; the cell is caller-owned.
#
# Semantics kept simple and deterministic (single worker, no contention):
#   publish()  — a waiter arms the cell under a gen token.
#   set()      — latches readiness (sticky until consume).
#   consume()  — wait-side consumes the latch.
#   is_set()   — observes readiness.
# This mirrors the spike's Event (A0.7, issue #16) readiness half without the
# partner race-pipeline: A1.1 keeps scheduling bookkeeping in scheduler.mojo /
# task.mojo over the TaskControlBlock machine, and this cell is the parking
# target for the (few) A1.1 waits.
struct WaitEvent(ImplicitlyCopyable, ImplicitlyDeletable):
    """Readiness cell with a sticky latch."""

    var _latched: Bool
    var _waiter_id: Int

    def __init__(out self):
        self._latched = False
        self._waiter_id = -1

    def arm(mut self, waiter_id: Int):
        """Waiter publishes its identity (data, not code)."""
        self._waiter_id = waiter_id

    def set(mut self):
        """Deliver readiness (latches)."""
        self._latched = True

    def clear(mut self):
        """Wait-side consume of a pending readiness."""
        self._latched = False

    def is_set(self) -> Bool:
        return self._latched

    def waiter_id(self) -> Int:
        return self._waiter_id