# mojito_async/fiber/__init__.mojo
#
# A1.4 (issue #52) — fiber packaging.
#
# This A1.4 lane delivers the per-worker stack cache (stack_pool.mojo);
# the Fiber wrapper/context binding lives in the sibling fiber lane (#49)
# and re-exports from here when it lands.  Consumers of the stack pool
# (fiber binding, one-shot continuation, driver suites) import the pool from
# mojito_async.fiber.stack_pool.
from mojito_async.fiber.stack_pool import (
    StackCache,
    make_stack_cache,
)