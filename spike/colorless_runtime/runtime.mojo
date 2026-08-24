# A0.2 — minimal Runtime skeleton (issue #11).
#
# Deliberately NOT a real runtime.  The one-worker scheduler, fiber,
# task-control-block, queue, and spawn land in A0.3–A0.6 landing; this file
# exists so the spike harness and the A0-T1 colorlessness test can compile
# against the intended surface while those lanes are still red.
#
# Mojo 1.0.0b2 (def-only) constraints honored here:
#   - no `fn`, no `async`/`await`/`Future`, no module-level mutable globals;
#   - first-class `def` values are nominal (a bare `def work()` cannot be
#     converted to a trait-typed value), so the task argument is taken as a
#     generic constrained to the `def()` callable trait — the b2-legal way to
#     accept an ordinary def/closure.  See S0 mojito_spike.mojo notes.
#   - Mojo 1.0.0b2 has no static methods inside structs; the module-level
#     `create()` factory below is the constructor surface (same style as
#     mojito-sys ms_* externs, which are also module-level defs).
#
# Safety invariant: run() must NEVER silently accept work it cannot schedule.
# Until A0.4/A0.6 implement the real scheduler it raises immediately, so no
# user task is silently swallowed.

struct Runtime:
    # No mutable state yet: the skeleton owns no scheduler.  A0.4 installs
    # worker/fiber/queue state here.

    def __init__(out self):
        pass

    # Run a colorless task (any ordinary def with no arguments, no result).
    # Stub: raises until the scheduler exists. Never invokes `task`.
    def run[T: def() -> None](self, task: T) raises:
        _ = task  # deliberately unused while run() is a stub
        raise Error("runtime.run: not implemented yet (A0.4/A0.6)")

    # Shut down the runtime.  Skeleton has nothing to tear down; the real
    # owner (A0.4) reclaims worker stack/context here.
    def shutdown(self) -> None:
        pass


# Module-level factory (b2 has no static methods). Mirrors the mojito-sys
# convention of exposing module-level constructors.
def create() -> Runtime:
    return Runtime()