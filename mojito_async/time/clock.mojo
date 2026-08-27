# mojito_async/time/clock.mojo
#
# A1.4 timer lane (issue #36) — monotonic clock surface.
#
# The spec (§30, §76.5) mandates ABSOLUTE MONOTONIC deadlines and a
# deterministic (virtual) clock for timer/deadline tests so a long timeout
# test runs as a deterministic state transition instead of a wall-clock wait.
# This module provides a caller-owned VIRTUAL monotonic clock: a UInt64
# ticks cell (ns) that the driver/auot harness seeds and advances.  Reading
# is extern-free and allocation-free (one pointer deref).  When real time is
# needed, an embedding *_aot driver reads clock monotonic via an extern and
# `set()`s the cell — the externs stay in the AOT drivers (modular/modular#
# 6971: b2 JIT cannot lower imported-module externs).
#
# A1.4 notes: no module globals, so the clock is an explicit cell the runtime
# shelf owns and threads into the timer service; the service never blocks.


struct MonotonicClock(ImplicitlyCopyable, ImplicitlyDeletable):
    """Virtual monotonic nanosecond clock wrapping a caller-owned UInt64
    ticks cell.  `now()` never goes backwards within the caller's sequence
    of `advance`/`set` calls.

    Invariant: the referenced cell monotonically increases (the embedding
    driver only advances or seeds it forward)."""

    var _cell: UnsafePointer[UInt64, MutAnyOrigin]

    def __init__(out self, cell: UnsafePointer[UInt64, MutAnyOrigin]):
        self._cell = cell

    def now(self) -> UInt64:
        """Current monotonic reading (ticks, ns)."""
        return self._cell[]

    def advance(mut self, ticks: UInt64):
        """Advance the clock forward by `ticks` ns (virtual-time driver)."""
        self._cell[] = self._cell[] + ticks

    def advance_ns(mut self, ns: UInt64):
        self._cell[] = self._cell[] + ns

    def set(mut self, ticks: UInt64):
        """Seed/reset the clock reading to `ticks`."""
        self._cell[] = ticks
