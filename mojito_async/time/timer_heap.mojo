# mojito_async/time/timer_heap.mojo
#
# A1.4 timer lane (issue #36) — monotonic min-heap of timer deadlines.
#
# A binary min-heap keyed by absolute monotonic deadline ticks (UInt64 ns,
# the unit the A1.4 sleep parks on, spec §30-31).  This module is the PURE
# data structure deliverable: insert/arm, cancel (whole-id and exact-token),
# min-peek / expired-pop in deadline order, and generation-token stale-timer
# suppression (spec §31: "stale timer suppression via generation tokens").
#
# Design (b2, def-only, no module globals):
#   - The heap is a caller-owned struct holding two std List fields:
#       _entries — array-backed min-heap of TimerEntry (the actual timers);
#       _live    — dense (id-indexed) registry of the most-recently-armed
#                  generation per id (0 = none).  Grow-on-demand keeps
#                  arm/live_gen O(1) amortized so the registry never
#                  dominates the O(log n) sift cost (the timer-scale bench
#                  arms dense task ids 1..N).
#   - `arm(id, tcbaddr, deadline)` grants a FRESH generation token, stamps
#     the entry, and records it as the live generation for `id`.  A STALE
#     timer is an entry that remains physically in the heap (e.g. re-armed
#     under the same id) but whose stamped generation is no longer the live
#     one; expiry pops it but a generation-aware waker skips it (the token
#     no longer matches).  This is exactly the classic stale-suppression
#     mechanism: never resurrect a cancelled/superseded deadline.
#   - cancel(id) removes every pending entry for `id` (the whole timer) and
#     clears its live registration. cancel_token(id, gen) removes ONLY the
#     entry whose (id, gen) match exactly — a stale token from a previous
#     arm cannot cancel a NEWER timer, and removing the live arm clears the
#     registry so later stale pops are suppressed.
#   - The heap is extern-free.  On the wake hot path pop_min() is O(log n)
#     with no allocation beyond the List's amortized storage; only
#     cancel()/cancel_token() rebuild (control-plane, never hot).
#
# The expiry DRIVE (popping due timers and waking the parked tasks they
# reference) lives in timer_service.mojo / sleep.mojo — the canonical
# park/wake integration.  This module stays a pure structure so it is
# independently testable and reusable by later wheel lanes.
from std.collections import List


# Sentinel deadline: "no timer scheduled".  UInt64 max.
comptime NO_DEADLINE = UInt64(0xFFFF_FFFF_FFFF_FFFF)


struct TimerEntry(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """One heap node.  `deadline` is the monotonic expiry tick (ns).  `id`
    is the timer/task id (the parked task's scheduler id); `gen` is the
    generation token granted when this arm happened; `tcbaddr` is the
    caller-allocated TaskControlBlock address to wake on expiry."""

    var deadline: UInt64
    var id: Int
    var gen: Int
    var tcbaddr: Int

    def __init__(out self):
        self.deadline = 0
        self.id = 0
        self.gen = 0
        self.tcbaddr = 0

    def __init__(out self, deadline: UInt64, id: Int, gen: Int, tcbaddr: Int):
        self.deadline = deadline
        self.id = id
        self.gen = gen
        self.tcbaddr = tcbaddr

    def is_stale_against(self, live_gen: Int) -> Bool:
        """True when a popped entry no longer corresponds to the live arm of
        its id (a superseded or cancelled generation)."""
        return live_gen != 0 and live_gen != self.gen


struct TimerHeap(Movable, ImplicitlyDeletable):
    """Binary min-heap of monotonic timer deadlines with generation tokens.

    Movable-only (not ImplicitlyCopyable): the owned List storage cannot be
    implicitly copied under b2; callers pass `mut heap` or move it.

    INV: for every live timer, _entries[min] is the earliest deadline; the
    _live registry maps each armed id to its most-recently-granted gen.
    """

    var _entries: List[TimerEntry]
    var _live: List[Int]
    var _gen_seq: Int

    def __init__(out self):
        self._entries = List[TimerEntry]()
        self._live = List[Int]()
        self._gen_seq = 0

    # --- capacity / peaks ---------------------------------------------------

    def is_empty(self) -> Bool:
        return len(self._entries) == 0

    def size(self) -> Int:
        return len(self._entries)

    def min_deadline(self) -> UInt64:
        """The earliest pending deadline, or NO_DEADLINE when empty."""
        if self.is_empty():
            return NO_DEADLINE
        return self._entries[0].deadline

    def has_due(self, now: UInt64) -> Bool:
        """True when the earliest timer is expired at `now`."""
        return (not self.is_empty()) and self.min_deadline() <= now

    # --- generation registry ------------------------------------------------
    #
    # Dense-id layout: _live[id] = most recently granted generation for
    # `id` (0 = never armed / cleared).  Grow-on-demand append keeps
    # arm()/live_gen() O(1) amortized.

    def live_gen(self, id: Int) -> Int:
        """The generation granted to `id`'s MOST RECENT arm; 0 if never."""
        if id >= 0 and id < len(self._live):
            return self._live[id]
        return 0

    def _set_live(mut self, id: Int, gen: Int):
        while len(self._live) <= id:
            self._live.append(0)
        self._live[id] = gen

    def _clear_live(mut self, id: Int):
        if id >= 0 and id < len(self._live):
            self._live[id] = 0

    # --- heap operations (sift) ---------------------------------------------

    def _sift_up(mut self, start: Int):
        var i = start
        while i > 0:
            var parent = (i - 1) // 2
            if self._entries[parent].deadline <= self._entries[i].deadline:
                break
            var tmp = self._entries[i]
            self._entries[i] = self._entries[parent]
            self._entries[parent] = tmp
            i = parent

    def _sift_down(mut self, start: Int):
        var i = start
        var n = len(self._entries)
        while True:
            var l = 2 * i + 1
            var r = 2 * i + 2
            var smallest = i
            if l < n and self._entries[l].deadline < self._entries[smallest].deadline:
                smallest = l
            if r < n and self._entries[r].deadline < self._entries[smallest].deadline:
                smallest = r
            if smallest == i:
                break
            var tmp = self._entries[i]
            self._entries[i] = self._entries[smallest]
            self._entries[smallest] = tmp
            i = smallest

    # --- arm / cancel / pop --------------------------------------------------

    def arm(mut self, id: Int, tcbaddr: Int, deadline: UInt64) raises -> Int:
        """Register a timer for `id` expiring at `deadline`.  Grants a fresh
        generation token (bumped per arm) and returns it.  The prior arm of
        the same `id` (if any) stays physically in the heap but is now
        STALE: expiry pops it, and a generation-aware waker skips it."""
        self._gen_seq += 1
        var gen = self._gen_seq
        self._entries.append(TimerEntry(deadline, id, gen, tcbaddr))
        var idx = len(self._entries) - 1
        self._sift_up(idx)
        self._set_live(id, gen)
        return gen

    def pop_min(mut self) raises -> TimerEntry:
        """Remove and return the earliest timer.  Raises on an empty heap."""
        if self.is_empty():
            raise Error("TimerHeap.pop_min: empty heap")
        var root = self._entries[0]
        var last = len(self._entries) - 1
        self._entries[0] = self._entries[last]
        _ = self._entries.pop(last)
        if not self.is_empty():
            self._sift_down(0)
        return root

    def cancel(mut self, id: Int) -> Bool:
        """Cancel the whole timer for `id`: remove every armed entry of that
        id and clear its live registration.  Returns True if anything was
        removed.  A cancelled timer never fires.

        Rebuild is O(n) — cancellation is a control-plane operation, not on
        the expiry hot path; correctness and determinism win."""
        self._clear_live(id)
        var kept = List[TimerEntry]()
        var removed = False
        for e in range(len(self._entries)):
            if self._entries[e].id == id:
                removed = True
            else:
                kept.append(self._entries[e])
        self._entries = kept^
        return removed

    def cancel_token(mut self, id: Int, gen: Int) -> Bool:
        """Remove the entry whose (id, gen) match exactly.  A stale token
        (from an older arm) does NOT cancel a newer timer.  Returns True iff
        the exact entry was removed.  If the removed entry WAS the live arm,
        the live registration for `id` is cleared too (no timer pending)."""
        var i = 0
        while i < len(self._entries):
            if (
                self._entries[i].id == id and self._entries[i].gen == gen
            ):
                _ = self._entries.pop(i)
                if i < len(self._entries):
                    self._sift_down(i)
                    self._sift_up(i)
                if self.live_gen(id) == gen:
                    self._clear_live(id)
                return True

            i += 1
        return False

    # --- expire pass ----------------------------------------------------------

    def collect_due(mut self, now: UInt64) raises -> List[TimerEntry]:
        """Pop every timer expired at `now` in deadline order.  Returns the
        ordered list so the caller can filter stale generations and wake the
        matching tasks."""
        var out = List[TimerEntry]()
        while not self.is_empty() and self.min_deadline() <= now:
            var e = self.pop_min()
            out.append(e)
        return out^