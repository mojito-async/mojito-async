# mojito_async/fiber/__init__.mojo
#
# A1.x — fiber package public surface (batch A1.1/#49 fiber binding, A1.2/#50
# one-shot continuation over a bound fiber, A1.4/#52 stack pool).
#
# Spec §14-16: a `Fiber` is an internal mojito-async execution object
# composed from mojito-sys mechanisms; the stack pool owns reservations; a
# one-shot continuation is the current parked segment of a task.  This
# package re-exports the concrete fiber binding surface so consumers import
# from mojito_async.fiber (spec §6 layout).
#
# The stack pool (issue #52) and the continuation layer (issue #50) are
# provided by sibling files in this package; their names are added to this
# module as those lanes land (additive, never removed).
from mojito_async.fiber.fiber import (
    Fiber,
    FiberFrame,
    bind,
    make_fiber,
)