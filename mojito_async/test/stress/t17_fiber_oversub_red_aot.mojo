# mojito_async/test/stress/t17_fiber_oversub_red_aot.mojo
#
# A1.5 (issue #53, consensus T1) — >32-concurrent-fiber no-cap regression
# after the substrate M:N rework (issue #101).
#
# The vendored resume table (64 rows, 2 per fiber => 32-live-fiber cap,
# brk #0x67 SIGILL) was REMOVED by #101: ms_ctx_t carries its own return_to
# and the switch path is thread-safe.  This driver asserts the post-#101
# contract: N_OVERS concurrent bindings succeed (no Error, no trap) and
# lifetime churn of >64 distinct blocks does not trap — the old cap and the
# old row per-address leak are both gone.
#
# EXTERN DISCIPLINE (modular/modular#6971): imports the extern-bearing
# fiber seam, so it MUST be AOT (*_aot.mojo — picked up by test/run.sh).
#
# Verdict: RED + exit 1 when the guard fires; FAIL otherwise (exit 1 without
# the RED marker, or exit 2 on harness errors).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    c_free,
    c_malloc,
    entry_pointer,
    ms_page_size,
    ms_stack_alloc,
)
from mojito_async.fiber.fiber import Fiber
from mojito_async.runtime.fiber_seam import (
    SeamSlot,
    make_seam_slot,
    seam_bind_slot,
    seam_destroy_slot,
    seam_slot_stride,
)
from mojito_async.runtime.runtime import create
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime N_OVERS = Int(34)   # > the old 32-fiber cap: all must bind


@export("t17_ovr_entry")
def t17_ovr_entry(ud: BytePtr) abi("C"):
    # Never driven in this driver (binding alone exercises the guard); the
    # thunk exists so make_fiber has a valid entry pointer.
    return


def main() raises:
    var ps = Int(ms_page_size())
    var stack_bytes = 4 * ps

    # --- N_OVERS reservations + slots (never driven) ----------------------
    var sbuf = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(N_OVERS * 2 * 8))
    )
    var slots_block = c_malloc(N_OVERS * seam_slot_stride())
    var slots = UnsafePointer[SeamSlot, MutAnyOrigin](
        unsafe_from_address=Int(slots_block)
    )
    for i in range(N_OVERS):
        (slots + i)[0] = make_seam_slot()

    var bound = 0
    for i in range(N_OVERS):
        var scell = sbuf + 2 * i
        if ms_stack_alloc(stack_bytes, scell, scell + 1) != 0:
            print("T17 no-cap: stack alloc failed at " + String(i))
            _iso_exit(2)
        var ns = NativeStack(scell[0], (scell + 1)[0])
        seam_bind_slot(
            slots + i, ns, entry_pointer["t17_ovr_entry"](),
            UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
        )
        bound = i + 1

    # --- teardown slot 0 (bound in the concurrency loop above) ------------
    seam_destroy_slot(slots + 0)

    # --- churn: repeated bind/destroy over one slot must never trap -------
    # (the old per-address row leak would trap after 64 distinct block
    # addresses over a lifetime; the no-cap substrate must not)
    var churn = 0
    for i in range(200):
        var rbuf = UnsafePointer[BytePtr, MutUntrackedOrigin](
            unsafe_from_address=Int(c_malloc(2 * 8))
        )
        if ms_stack_alloc(stack_bytes, rbuf, rbuf + 1) != 0:
            print("T17 no-cap: churn stack alloc failed at " + String(i))
            _iso_exit(2)
        var ns2 = NativeStack(rbuf[0], (rbuf + 1)[0])
        seam_bind_slot(
            slots + 0, ns2, entry_pointer["t17_ovr_entry"](),
            UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
        )
        seam_destroy_slot(slots + 0)
        c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(rbuf)))
        churn += 1

    # --- verdict ----------------------------------------------------------
    if bound != N_OVERS:
        print("T17 no-cap: FAIL (bound " + String(bound) + ", expected " +
              String(N_OVERS) + ")")
        _iso_exit(1)

    # --- teardown (all slots inert; destroy is safe) ----------------------
    for i in range(N_OVERS):
        seam_destroy_slot(slots + i)
    c_free(slots_block)
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(sbuf)))

    print("T17 no-cap: PASS (34 concurrent bindings + 200-churn lifetime, "
          + "no cap, no trap; issue #101)")
