# mojito_async/test/unit/negative_continuation_copy.mojo
#
# NEGATIVE driver for A1.2 (issue #50) ownership fold (A1 consensus T2 /
# fold #5): FiberContinuation is Movable (NOT ImplicitlyCopyable), so any
# attempt to COPY a continuation is a COMPILE ERROR.
#
# The pre-fold hazard: FiberContinuation was ImplicitlyCopyable, so
#     var cont = make_continuation(...)
#     var twin = cont        # copy duplicates the carrier alias/state
# could silently alias the same carrier handle and diverge state machines.
# With Movable (and a non-copyable Fiber carrier), `var twin = cont` is
# rejected by the compiler — single-owner state by construction.
#
# This file deliberately DOES NOT compile; it is kept out of the green suite
# (run.sh only globs `t[0-9][0-9]_*.mojo`) and is verified by AOT-building it
# and asserting the "cannot be implicitly copied" diagnostic — the same
# verification the fiber lane uses for negative_fiber_copy.mojo.
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    entry_pointer,
    ms_page_size,
    ms_stack_alloc,
)
from mojito_async.fiber import (
    Fiber,
    FiberContinuation,
    make_continuation,
    make_fiber,
)
from std.memory import stack_allocation


@export("negc_entry")
def negc_entry(ud: BytePtr) abi("C"):
    pass


def main() raises:
    var ps = Int(ms_page_size())
    var slots = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(ps * 4, slots, slots + 1) != 0:
        raise Error("negc: stack alloc failed")
    var ns = NativeStack(slots[0], (slots + 1)[])
    var f = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns),
        entry_pointer["negc_entry"](),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
    )
    var cont = make_continuation(
        UnsafePointer[Fiber, MutAnyOrigin](to=f),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
    )
    # COMPILE-TIME copy attempt: FiberContinuation is Movable, so this MUST
    # be rejected.
    var twin = cont
    twin.start()
    f.destroy()