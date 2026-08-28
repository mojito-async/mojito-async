# mojito_async/test/stress/t17_fiber_oversub_red_aot.mojo
#
# A1.5 (issue #53, consensus T1) — >32-concurrent-fiber oversubscription
# regression, RED by design on the FROZEN substrate.
#
# The vendored mojito-sys keeps a FIXED 64-row resume table (2 rows per
# fiber), so a process can host at most 32 concurrently live fibers.  The
# binder's oversubscription guard (fiber.make_fiber / bind, MS_MAX_LIVE_
# FIBERS) raises a CATCHABLE Error at the 33rd live fiber instead of letting
# the 33rd switch trap SIGILL (brk #0x67).  This driver asserts that loud
# Error path.
#
# RED-BY-DESIGN: the driver VERIFIES the guard fires and then reports RED +
# exit 1 — the substrate limit is the documented, tracked constraint
# (tracking issue #101, EPIC #2 substrate rework lifts it), allow-listed via
# precommit/known-red.tsv (`suite\t<issue-101>`).  When #101 lands and the
# limit is gone, this driver flips to a PASS-style assertion (33+ fibers
# bind and run) and the known-red row is removed.
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


comptime N_OVERS = Int(34)   # 32 is the cap; the 33rd bind must raise
comptime MS_CAP = Int(32)


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

    var raised = False
    var msg = ""
    var bound = 0
    for i in range(N_OVERS):
        var scell = sbuf + 2 * i
        if ms_stack_alloc(stack_bytes, scell, scell + 1) != 0:
            print("T17 oversub: stack alloc failed at " + String(i))
            _iso_exit(2)
        var ns = NativeStack(scell[0], (scell + 1)[0])
        try:
            seam_bind_slot(
                slots + i, ns, entry_pointer["t17_ovr_entry"](),
                UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1),
            )
            bound = i + 1
        except e:
            msg = String(e)
            raised = True
            break
        if i == N_OVERS - 1:
            break

    # --- verdict ----------------------------------------------------------
    if not raised:
        print("T17 oversub: FAIL (no oversubscription Error raised; " +
              String(bound) + " fibers bound)")
        _iso_exit(1)
    if "oversubscription" not in msg:
        print("T17 oversub: FAIL (raised, but not the oversubscription " +
              "guard: " + msg + ")")
        _iso_exit(1)
    if bound != MS_CAP:
        print("T17 oversub: FAIL (guard fired at " + String(bound) +
              " bound fibers, expected " + String(MS_CAP) + ")")
        _iso_exit(1)

    # --- teardown (the 32 bound fibers are inert; destroy is safe) --------
    for i in range(bound):
        seam_destroy_slot(slots + i)
    c_free(slots_block)
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(sbuf)))

    # RED by design: the frozen-substrate 32-fiber cap is a documented,
    # tracked constraint (issue #101); allow-listed via known-red.
    print("T17 oversub: RED (32-fiber substrate cap raised loudly at the "
          + "33rd bind, as documented; issue #101 lifts this)")
    _iso_exit(1)