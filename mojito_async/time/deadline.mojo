# mojito_async/time/deadline.mojo
#
# A1.1 runtime (issue #33) — Deadline API surface (spec §7.1).
#
# A1.1 owns the *data* surface only: a Deadline is a monotonic point in
# milliseconds.  The A1.4 timer lane implements the timer heap and any
# blocking sleep/until mechanics; this module deliberately has NO heap.
# Keeping Deadline name-identical to the spec root surface so A1.4 fills
# semantics in place.


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