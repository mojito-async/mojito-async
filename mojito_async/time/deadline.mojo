# mojito_async/time/deadline.mojo
#
# A1.1 runtime (issue #33) — Deadline API surface (spec §7.1).
#
# A1.1 owns the *data* surface only: a Deadline is a monotonic point in
# milliseconds.  The A1.4 timer lane implements the timer heap and any
# blocking sleep/until mechanics; this module deliberately has NO heap.
# Keeping Deadline name-identical to the spec root surface so A1.4 fills
# semantics in place.
#
# A7.1 reactor lane (issue #75): explicit trait conformance
# (ImplicitlyCopyable/ImplicitlyDeletable/Movable) added so `Duration` can
# be carried inside `Optional[Duration]` (the reactor's `Reactor.poll`
# timeout parameter, reactor/poller.mojo's NativePoller.wait) — b2's
# `Optional[T: Movable]` bound requires the conformance to be declared
# explicitly on the struct, it is not inferred from an all-scalar field
# layout.  Every prior consumer of `Duration` used it by plain value, so
# this is additive and does not change any existing call site.


struct Duration(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """A monotonic duration: UInt64 ticks in nanoseconds (the unit the A1.4
    timer lane will park on).  Provides from_millis/to_millis consistent with
    the existing Deadline (which is expressed in milliseconds)."""

    var _ticks: UInt64

    def __init__(out self):
        self._ticks = 0

    def __init__(out self, ticks: UInt64):
        self._ticks = ticks

    def ticks(self) -> UInt64:
        return self._ticks

    def to_millis(self) -> Int:
        return Int(self._ticks // 1000000)

    def is_zero(self) -> Bool:
        return self._ticks == 0



# Module-level factory (b2 has no static methods): milliseconds -> Duration
# (1 ms = 1_000_000 ns ticks).
def from_millis(ms: Int) -> Duration:
    return Duration(UInt64(ms) * 1000000)


struct Deadline:
    """A monotonic deadline expressed in milliseconds."""

    var _at_ms: Int

    def __init__(out self):
        self._at_ms = 0

    def __init__(out self, at_ms: Int):
        self._at_ms = at_ms

    def at_ms(self) -> Int:
        return self._at_ms

    def is_expired(self, now_ms: Int) -> Bool:
        return now_ms >= self._at_ms

    def remaining(self, now_ms: Int) -> Int:
        """Remaining milliseconds at `now_ms` (clamped >= 0)."""
        var r = self._at_ms - now_ms
        if r < 0:
            return 0
        return r


def from_now(now_ms: Int, ms: Int) -> Deadline:
    """A deadline `ms` milliseconds from the given monotonic clock reading."""
    return Deadline(now_ms + ms)