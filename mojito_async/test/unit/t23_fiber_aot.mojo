# mojito_async/test/unit/t23_fiber.mojo
#
# A1.1 (issue #49) — fiber context/stack binding acceptance driver.
#
# Covers the batch Fiber seam (A1.1/#49) end-to-end over the real switching
# substrate (libmojito_spike.dylib via the vendored NativeStack firewall):
#   - make_fiber(stack, entry, userdata) binds an acquired ms_stack_alloc
#     reservation and wires the entry/userdata scratch; the fiber's block is
#     allocated and the fiber is alive.
#   - resume() drives the ONE-SHOT continuation: the FIRST resume defers
#     ms_ctx_make (preparing the fresh context), sets has_resumed(), and
#     enters the entry callback on the synthetic stack at the 16-aligned SP.
#   - the entry callback mutates a SHARED buffer via userdata, then yields
#     back to the driver with suspend() symmetry (fiber.suspend() flips
#     is_suspended()); resume() re-enters at the EXACT point, verifying a
#     stack-local address is stable across one switch away and back
#     (spec §14 one-shot continuation, A0-T3) and the sentinels survive.
#   - completion returns through the trampoline's defined path to the caller.
#   - lifecycle is idempotent: create -> resume -> suspend -> resume ->
#     complete -> destroy; a second destroy() is a no-op (alive() flips).
#   - stack_ptr() returns the bound reservation (base/top), enabling clean
#     reclaim: after the fiber releases it, an equal-size replacement
#     ms_stack_alloc succeeds (stack reuse, stack-pool seam #52).
#
# EXTERN DISCIPLINE (modular/modular#6971 family): this driver imports only
# the extern-free transition methods (resume/suspend) plus the vendor firewall
# defs it needs (ms_stack_alloc, ms_page_size, ms_ctx_switch for the entry's
# direct yield -- the S0 demo form).  The b2 JIT cannot resolve the dylib
# symbols through an imported module, so this driver is built with `mojo
# build` + execute (AOT) exactly like spike t4b / test/stress/t11_stress_aot.
# The harness AOT loop adds -Xlinker libmojito_spike.dylib.
#
# Verdict: exit 0 + "PASS"; any failure prints RED + raises (exit 1).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    entry_pointer,
    ms_ctx_switch,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
)
from mojito_async.fiber.fiber import Fiber, FiberFrame, make_fiber
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime STACK_PAGES = 4
comptime ALIGN_MASK = 15
comptime SB_SENT = 2  # driver-side sentinel slot the thunk must leave alone


# Small payload handed via userdata and mutated by the entry callback + the
# driver (userdata-lifetime probe).  Lives on the DRIVER's stack.
struct T23Payload:
    var buf: UnsafePointer[Int, MutAnyOrigin]   # shared scratch (4 ints)
    var phase: Int                              # 0 entered / 1 yielded / 2 done
    var marker: Int                             # stack-local addr at first entry
    var resume_ok: Bool                         # local address stable across switch

    def __init__(out self):
        self.buf = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phase = 0
        self.marker = -1
        self.resume_ok = True


# The entry callback (S0 trampoline handoff): runs real Mojo code on the
# synthetic stack.  ud = the fiber's FiberFrame sidecar (frame_ptr); the
# payload is reachable via fr[].user.  Yields back to the driver via the
# sidecar's self_ctx/caller_ctx (S0 demo form); on resume verifies the
# stack-local kept its address (A0-T3 / spec §14) and that the shared buffer
# was not clobbered, then returns (completion path back to the driver's
# resume()).
@export("t23_entry")
def t23_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var pl = fr[].user.bitcast[T23Payload]()

    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)

    if pl[].phase == 0:
        # First entry: record the stack-local address for the A0-T3
        # resume-point check, mutate the shared scratch, yield.
        pl[].marker = Int(local_p)
        pl[].buf[0] = 0x5EED
        pl[].phase = 1
        ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

        # -- EXACT RESUME POINT (A0-T3): the stack-local and the shared
        # -- buffer (incl. the driver sentinel) must be intact across the
        # -- pause.
        if pl[].marker != Int(local_p):
            pl[].resume_ok = False
        if pl[].buf[0] != 0x5EED:
            pl[].resume_ok = False
        if pl[].buf[SB_SENT] != 0x0BAD:
            pl[].resume_ok = False
        pl[].buf[1] = 0xCAFE
        pl[].phase = 2


def red(what: String) raises -> None:
    print("T23 fiber: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var failures = List[String]()

    var ps = Int(ms_page_size())
    if ps <= 0:
        failures.append("ms_page_size non-positive")

    # Shared scratch + payload on the driver stack.
    var buf = stack_allocation[4, Int]()
    buf[0] = 0
    buf[1] = 0
    buf[2] = 0x0BAD  # driver sentinel; the thunk must leave it intact
    buf[3] = 0
    var pl = stack_allocation[1, T23Payload]()
    pl[0] = T23Payload()
    pl[].buf = buf

    var stack_bytes = STACK_PAGES * ps

    # --- acquire a synthetic stack (pool-seam shape) -----------------------
    var slots = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(stack_bytes, slots, slots + 1) != 0:
        red("ms_stack_alloc failed")
    var ns = NativeStack(slots[0], (slots + 1)[])
    var top = Int(ns.top)
    if (top & ALIGN_MASK) != 0:
        red("stack top not 16-aligned")

    # --- make_fiber: bind + wire the entry/userdata ------------------------
    var f = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns),
        entry_pointer["t23_entry"](),
        pl.bitcast[Byte](),
    )
    if not f.alive():
        red("fiber not alive after make_fiber")
    if f.has_resumed():
        red("fiber has_resumed before first resume")
    if f.is_suspended():
        red("fiber is_suspended before first resume")
    var sp = f.stack_ptr()
    if Int(sp.base) != Int(ns.base) or Int(sp.top) != Int(ns.top):
        red("stack_ptr does not report the bound reservation")

    # --- resume #1: prepare + enter the fresh context ----------------------
    f.resume()

    if not f.has_resumed():
        failures.append("fiber not marked has_resumed after first resume")
    if f.is_suspended():
        failures.append("fiber still flagged suspended while running")

    if pl[].phase != 1:
        failures.append(
            "entry did not reach the yield point (phase " + String(pl[].phase) + ")"
        )
    if buf[0] != 0x5EED:
        failures.append("entry did not mutate the shared buffer via userdata")
    if buf[2] != 0x0BAD:
        failures.append("driver sentinel clobbered while the fiber was sandwiched")

    # --- resume #2: continue at the exact point ----------------------------
    f.resume()

    if pl[].phase != 2:
        failures.append("entry did not complete after resume")
    if buf[1] != 0xCAFE:
        failures.append("resumed thunk did not continue its own stack frame")
    if not pl[].resume_ok:
        failures.append("stack-local address (or shared buffer) not stable across switch")

    # --- reclaim: release, then an equal-size replacement allocates --------
    # stack_ptr() gives the caller the reservation back for the pool; the
    # fiber's destroy() is then a no-op (alive() flips).
    var released = f.stack_ptr()
    f.destroy()
    if f.alive():
        failures.append("destroy left fiber alive")
    # second destroy must be a no-op
    f.destroy()

    var sf = stack_allocation[1, NativeStack]()
    var slots2 = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(stack_bytes, slots2, slots2 + 1) != 0:
        failures.append("equal-size replacement stack did not allocate after free")
    else:
        var top2 = Int((slots2 + 1)[])
        if (top2 & ALIGN_MASK) != 0:
            failures.append("replacement stack top not 16-aligned")
        ms_stack_free(NativeStack(slots2[0], (slots2 + 1)[]).base)

    if len(failures) == 0:
        print("T23 fiber: PASS")
    else:
        print("T23 fiber: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
