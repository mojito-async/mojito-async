# mojito_async/test/unit/t24_fiber_guards_aot.mojo
#
# A1.1 (issue #49) — fiber fold guards (review A1 consensus, PR #97):
#   T1  no-cap concurrency (issue #101): the vendored resume table
#       (64-row / 32-fiber ceiling, SIGILL on saturation) was REMOVED by the
#       A2.0 M:N rework, so 34 concurrent fibers bind without any Error and
#       the live-stack count restores exactly after teardown.
#   T4  `_block` guard: resume() on a stack-only Fiber (the public
#       Fiber(stack) ctor, where _block == 0) raises instead of SEGV when the
#       sidecar write would touch the missing block.
#   T2/T4 `_completed` + finished(): mark_completed() records completion;
#       finished() observes it; resume() on a completed fiber raises a loud
#       Error instead of letting the asm trampoline re-entry trap (brk 0x66).
#
# EXTERN DISCIPLINE: AOT build (mojo build + execute) so the vendored
# mojito-sys externs (ms_stack_alloc, ms_live_stack_count, entry_pointer)
# resolve through the firewall; the harness links the dylib.  Identical
# pattern to t23_fiber_aot.
#
# Verdict: exit 0 + "PASS"; any failure prints RED + raises (exit 1).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    entry_pointer,
    ms_live_stack_count,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
)
from mojito_async.fiber.fiber import Fiber, make_fiber
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime STACK_PAGES = 4
comptime MAX_FIBERS = 34  # > the retired 32-fiber substrate cap (issue #101)


# Dummy entry: the fiber body never runs in this driver (the completed guard
# fires before any switch).
@export("t24_entry")
def t24_entry(ud: BytePtr) abi("C"):
    pass


def red(what: String) raises -> None:
    print("T24 guards: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var ps = Int(ms_page_size())
    var stack_bytes = STACK_PAGES * ps
    var failures = List[String]()

    # ------------------------------------------------------------------
    # T1 no-cap concurrency (issue #101): all MAX_FIBERS (34, > the retired
    # 32-fiber resume-table ceiling) bind WITHOUT error and without trap; the
    # live-stack count restores exactly after teardown.
    var cnt0 = ms_live_stack_count()
    comptime count = Int(MAX_FIBERS)
    var stacks = stack_allocation[count, BytePtr]()
    var fibers = stack_allocation[count, Fiber]()
    var bound = 0
    for i in range(count):
        var slots = stack_allocation[2, BytePtr]()
        if ms_stack_alloc(stack_bytes, slots, slots + 1) != 0:
            red("T1: ms_stack_alloc failed at i=" + String(i))
        var base = slots[0]
        var top = (slots + 1)[]
        stacks[i] = base
        var ns = NativeStack(base, top)
        try:
            var f = make_fiber(
                UnsafePointer[NativeStack, MutAnyOrigin](to=ns),
                entry_pointer["t24_entry"](),
                UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
            )
            fibers[i] = f^  # move the fiber (owns the acquired stack) in
            bound += 1
        except Error:
            red("T1: unexpected bind error at i=" + String(i))
    if bound != Int(MAX_FIBERS):
        failures.append("T1: bound " + String(bound) + "/" + String(MAX_FIBERS))

    # destroy every bound fiber (each frees its own stack); live count must
    # restore exactly (no row/stack leak in the no-cap substrate).
    for i in range(Int(MAX_FIBERS)):
        fibers[i].destroy()
    if ms_live_stack_count() != cnt0:
        failures.append("T1: live-stack count not restored after cleanup")

    # ------------------------------------------------------------------
    # T4 `_block` guard: Fiber(stack) public ctor (no block) resume must raise.
    # ------------------------------------------------------------------
    var slots_b = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(stack_bytes, slots_b, slots_b + 1) != 0:
        red("T4: stack alloc failed")
    var nsb = NativeStack(slots_b[0], (slots_b + 1)[])
    var fb = Fiber(nsb)  # _block == 0
    var block_guard = False
    try:
        fb.resume()
    except Error:
        block_guard = True
    if not block_guard:
        failures.append("T4: resume on a stack-only Fiber did not raise")
    fb.destroy()  # frees the acquired stack

    # ------------------------------------------------------------------
    # T2/T4 `_completed` + finished().
    # ------------------------------------------------------------------
    var slots_c = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(stack_bytes, slots_c, slots_c + 1) != 0:
        red("T4: stack alloc failed")
    var fns0 = NativeStack(slots_c[0], (slots_c + 1)[])
    var fc = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=fns0),
        entry_pointer["t24_entry"](),
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
    )
    if fc.finished():
        failures.append("T4: finished() true before mark_completed")
    fc.mark_completed()
    if not fc.finished():
        failures.append("T4: finished() false after mark_completed")
    var resumed_completed = False
    try:
        fc.resume()
    except Error:
        resumed_completed = True
    if not resumed_completed:
        failures.append("T4: resume on a completed fiber did not raise")
    fc.destroy()

    if len(failures) != 0:
        print("T24 fiber guards: RED")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
    print("T24 fiber guards: PASS")