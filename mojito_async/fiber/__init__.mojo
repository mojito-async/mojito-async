# mojito_async/fiber/__init__.mojo
#
# A1.2 (issue #50) + siblings — the fiber package: the one-shot continuation
# semantic layer (continuation.mojo, this lane), the Fiber binding
# (fiber.mojo, issue #49 / FiberBind) and the per-worker stack cache
# (stack_pool.mojo, issue #52 / StackPool).  Each sibling lane adds its own
# module re-export here (disjoint imports).

from mojito_async.fiber.continuation import (
    FiberContinuation,
    FiberMotion,
    is_continuation_error,
    make_continuation,
)