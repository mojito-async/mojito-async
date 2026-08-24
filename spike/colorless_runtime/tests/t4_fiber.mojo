# spike/colorless_runtime/tests/t4_fiber.mojo
#
# A0.4 (issue #13) — fiber wrapper over the vendored NativeStack / NativeContext
# substrate (ms_stack_alloc/free, ms_ctx_make/switch).  TDD: written RED first
# against fiber.mojo (which does not exist yet => compile-failure RED), goes
# GREEN once fiber.mojo lands.
#
# Covers spec A0.4 equivalences + the A0-T3 resume-point invariant:
#   - fresh context enters the S0 trampoline on a synthetic stack
#     (ms_stack_alloc), running real Mojo code (the @export abi("C") entry);
#   - the entry callback mutates a SHARED buffer through userdata (a small
#     struct payload passed through the Fiber, unmodified);
#   - a fiber-side suspend() yields back to the driver, and resume() continues
#     at the exact same point with the same ordinary stack state (A0-T3: a
#     stack-local address stable across one switch away and back);
#   - completion (entry returns) unwinds through the trampoline's defined
#     completion path back to the caller;
#   - the synthetic stack is freed cleanly (destroy) and an equal-size
#     replacement allocates (clean reclaim); sentinels in the shared buffer
#     survive every switch;
#   - sp alignment at entry is held (the vendored trampoline traps on
#     misalignment, so a clean first entry proves entry-SP alignment; the
#     entry's own Mojo frame alignment is also sampled on the synthetic stack).
#
# Linked against the dylib: the suite harness adds -Xlinker.
from fiber import Fiber, FiberFrame
from mojito_spike import (
    BytePtr,
    entry_pointer,
    ms_page_size,
    ms_stack_total_size,
)
from std.memory import stack_allocation


@extern("exit")
def _c_exit(code: Int32) abi("C"): ...


comptime STACK_PAGES = 4
comptime ALIGN_MASK = 15
comptime SB_SENT = 2  # driver-side sentinel slot the thunk must leave alone


# Small struct payload passed through userdata and mutated by the entry
# callback + the driver (userdata-lifetime probe).  Lives on the DRIVER's
# stack; the fiber callback reaches it through the FiberFrame sidecar
# (fiber.mojo hands `user` through unmodified).
struct T4Payload:
    var buf: UnsafePointer[Int, MutAnyOrigin]  # shared scratch (4 ints)
    var fiber_addr: Int                        # address of the driver's Fiber
    var phase: Int                             # 0 entered / 1 yielded / 2 done
    var marker: Int                            # stack-local addr at first entry
    var align_ok: Bool                         # synthesized-stack frame alignment
    var resume_ok: Bool                        # local address stable across switch

    def __init__(out self):
        self.buf = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=0)
        self.fiber_addr = 0
        self.phase = 0
        self.marker = -1
        self.align_ok = True
        self.resume_ok = True


# The fiber entry (S0 trampoline handoff): runs real Mojo code on the
# synthetic stack.  Receives ud = the Fiber's FiberFrame sidecar; the payload
# is reachable via fr[].user.  Yields once via Fiber.suspend(); on resume
# verifies the stack local kept its address (A0-T3) and that the shared buffer
# was not clobbered, then returns through the completion path.
@export("t4_fiber_entry")
def t4_fiber_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var pl = fr[].user.bitcast[T4Payload]()

    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)

    # synthesized-stack Mojo frame must be 16-byte aligned.
    if (Int(local_p) & ALIGN_MASK) != 0:
        pl[].align_ok = False

    if pl[].phase == 0:
        # First entry through the trampoline: record the stack-local address
        # for the resume-point check, mutate the shared buffer, yield.
        pl[].marker = Int(local_p)
        pl[].buf[0] = 0x5EED
        pl[].phase = 1

        var fp = UnsafePointer[Fiber, MutAnyOrigin](unsafe_from_address=pl[].fiber_addr)
        fp[].suspend()

        # -- EXACT RESUME POINT (A0-T3): the stack-local must still live at the
        # -- same address; the shared buffer (incl. the driver's sentinel) must
        # -- be intact across the switch.
        if pl[].marker != Int(local_p):
            pl[].resume_ok = False
        if pl[].buf[0] != 0x5EED:
            pl[].resume_ok = False
        if pl[].buf[SB_SENT] != 0x0BAD:
            pl[].resume_ok = False
        pl[].buf[1] = 0xCAFE
        pl[].phase = 2


def record(failures: List[String], what: String):
    failures.append(what)


def main() raises:
    var failures = List[String]()

    var ps = Int(ms_page_size())
    if ps <= 0:
        record(failures, "ms_page_size non-positive")

    # Shared scratch + userdata payload on the driver's stack.
    var buf = stack_allocation[4, Int]()
    buf[0] = 0
    buf[1] = 0
    buf[2] = 0x0BAD  # driver sentinel; the thunk must leave it intact
    buf[3] = 0
    var pl = stack_allocation[1, T4Payload]()
    pl[0] = T4Payload()
    pl[].buf = buf

    var stack_bytes = STACK_PAGES * ps

    # --- 1. fresh context over a synthetic stack (ms_stack_alloc) -----------
    var f = create(stack_bytes, entry_pointer["t4_fiber_entry"](), pl.bitcast[Byte]())
    if not f.alive():
        record(failures, "fiber did not allocate a synthetic stack")
    if (Int(f.stack_top()) & ALIGN_MASK) != 0:
        record(failures, "synthetic stack top not 16-byte aligned")

    # The driver's Fiber reserves usable pages + one guard page.
    var living = ms_stack_total_size()
    if living <= 0:
        record(failures, "ms_stack_total_size should be > 0 while a stack is allocated")

    # Wire the fiber back-pointer into the payload (needed by suspend() from
    # inside the entry; only valid once `f` has landed in this local).
    pl[].fiber_addr = Int(UnsafePointer[Fiber, MutAnyOrigin](to=f))

    # --- 2. enter through the trampoline; thunk mutates shared buf, yields ---
    f.resume()
    if pl[].phase != 1:
        record(failures, "entry did not reach the yield point (phase " + String(pl[].phase) + ")")
    if buf[0] != 0x5EED:
        record(failures, "entry did not mutate the shared buffer via userdata")
    if buf[2] != 0x0BAD:
        record(failures, "driver sentinel clobbered while the fiber was sandwiched")
    if not pl[].align_ok:
        record(failures, "entry Mojo frame not 16-byte aligned on the synthetic stack")

    # -- 3. resume at the exact point; thunk verifies, completes --------------
    f.resume()
    if pl[].phase != 2:
        record(failures, "entry did not complete after resume (phase=" + String(pl[].phase) + ")")
    if buf[1] != 0xCAFE:
        record(failures, "resumed thunk did not continue its own stack frame")
    if not pl[].resume_ok:
        record(failures, "stack-local address (or shared buffer) not stable across switch")

    # -- 4. clean reclaim: free, then equal-size realloc succeeds --------------
    f.destroy()
    if f.alive():
        record(failures, "destroy() did not release the synthetic stack")
    if ms_stack_total_size() >= living:
        record(failures, "ms_stack_total_size did not drop after destroy")

    var f2 = create(stack_bytes, entry_pointer["t4_fiber_entry"](), pl.bitcast[Byte]())
    if not f2.alive():
        record(failures, "equal-size replacement stack did not allocate after free")
    f2.destroy()
    if f2.alive():
        record(failures, "second destroy() leaked its stack")

    # --- verdict ------------------------------------------------------------------
    if len(failures) == 0:
        print("T4 fiber: PASS")
    else:
        print("T4 fiber: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _c_exit(1)