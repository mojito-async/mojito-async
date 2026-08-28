# mojito_async/test/unit/negative_fiber_copy.mojo
#
# NEGATIVE driver for A1.1 (issue #49) copy-safety fold (T4): Fiber is Movable
# (NOT ImplicitlyCopyable), so any attempt to COPY a Fiber is a COMPILE ERROR.
#
# The pre-fold bug this guards: `Fiber` was ImplicitlyCopyable, so
#     var f = make_fiber(...)
#     var g = f          # copy aliases the SAME stack + heap block
#     g.destroy()        # frees the stack + block
#     f.destroy()        # SECOND free on freed memory -> malloc double-free / UAF
#
# With Movable the `var g = f` line below is rejected by the compiler, so the
# copy->destroy->destroy sequence can never be written -- single-owner by
# construction.  This file deliberately DOES NOT compile; it is kept out of the
# green suite (run.sh only globs `t[0-9][0-9]_*.mojo`) and is verified by
# AOT-building it and asserting the "cannot be implicitly copied" diagnostic.
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    entry_pointer,
    ms_page_size,
    ms_stack_alloc,
)
from mojito_async.fiber.fiber import Fiber, make_fiber
from std.memory import stack_allocation


@export("neg_entry")
def neg_entry(ud: BytePtr) abi("C"):
    pass


def main() raises:
    var ps = Int(ms_page_size())
    var slots = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(ps * 4, slots, slots + 1) != 0:
        raise Error("neg: stack alloc failed")
    var ns = NativeStack(slots[0], (slots + 1)[])
    var f = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns),
        entry_pointer["neg_entry"](),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
    )
    # COMPILE-TIME copy attempt: Fiber is Movable, so this MUST be rejected.
    var g = f
    g.destroy()
    f.destroy()