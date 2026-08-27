# mojito_async/time/sleep.mojo
#
# A1.1 runtime (issue #33) — `sleep` API surface (spec §7.1).
#
# The A1.4 timer lane is out of scope for A1.1 (the assignment explicitly
# defers the timer heap to A1.4), yet the root surface re-exports `sleep` per
# spec §7.1 so sibling lanes import it.  This is an honest, documented STUB:
# `sleep(Duration)` is present and typed exactly as A1.4 will ship it and
# raises a precise error when invoked, rather than pretending to time out.
# No hidden blocking, no allocation.
#
# A1.4 will replace this body with a real deadline-based park on the timer
# wheel; the signature is the stable A1.4 surface.
from mojito_async.time.deadline import Duration


def sleep(duration: Duration) raises:
    """Sleep the CURRENT task for `duration`.

    NOT IMPLEMENTED in A1.1 — landed by the A1.4 timer lane.  Raises a
    descriptive error (never blocks or spins).  Keep callers compiling until
    the timer heap exists."""
    raise Error("sleep: A1.4 timer lane not yet implemented in A1.1")