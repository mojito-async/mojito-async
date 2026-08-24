# spike/colorless_runtime/tests/t4_fiber.mojo
#
# A0.4 (issue #13) — synthetic-stack entry/resume/reclaim coverage, written
# TDD-red first against fiber.mojo (absent => compile-fail RED) and landed
# GREEN in the S0-demo shape per Main's decision: all ctx switching is done
# INLINE in this module (local ms_ctx_t buffers + direct ms_ctx_make /
# ms_ctx_switch), and the @export abi("C") entry yields via
# ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx) — exactly the verified
# spike/context_switch/demo.mojo shape (probe: 3/3 stable under load).
#   The b2-JIT imported-module factory-CALL instability (bisected: concat-
# in-raise SIGSEGV 10/10; load-dependent codegen "failed to lower module to
# LLVM IR"; repro delivered to Main for upstream filing) does not touch this
# module: no factory is called cross-module here.
#
# Covers spec A0.4 + A0-T3:
#   - fresh ctx over a synthetic stack (ms_stack_alloc) with a 16-aligned SP
#     (stack_top alignment check; the vendored trampoline also traps a
#     misaligned entry, so a clean first entry confirms entry-SP alignment);
#   - the entry callback mutates a SHARED buffer via userdata (small struct
#     payload passed through unmodified), then yields (ms_ctx_switch);
#   - resume continues at the EXACT point with ordinary stack state intact
#     (A0-T3: a stack-local address stable across one switch away and back);
#   - completion returns through the trampoline's defined path to the caller;
#   - ms_stack_free + an equal-size replacement ms_stack_alloc succeeds
#     (clean reclaim); sentinels survive every switch.
#
# Linked: the suite harness adds -Xlinker <repo>/libmojito_spike.dylib.
from mojito_spike import (
    BytePtr,
    MS_CTX_SIZE,
    entry_pointer,
    ms_ctx_make,
    ms_ctx_switch,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
)
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime STACK_PAGES = 4
comptime ALIGN_MASK = 15
comptime SB_SENT = 2  # driver-side sentinel slot the thunk must leave alone


# Small payload handed via userdata and mutated by the entry callback + the
# driver (userdata-lifetime probe).  Lives on the DRIVER's stack.
struct T4Payload:
    var buf: UnsafePointer[Int, MutAnyOrigin]   # shared scratch (4 ints)
    var phase: Int                             # 0 entered / 1 yielded / 2 done
    var marker: Int                            # stack-local addr at first entry
    var resume_ok: Bool                        # local address stable across switch

    def __init__(out self):
        self.buf = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.phase = 0
        self.marker = -1
        self.resume_ok = True


# Sidecar layout == the vendored demo's Frame (self_ctx / caller_ctx / user);
# handed to ms_ctx_make as userdata, reached by the entry as
# ud.bitcast[T4Frame]().
struct T4Frame:
    var self_ctx: BytePtr
    var caller_ctx: BytePtr
    var user: BytePtr

    def __init__(out self, ac: BytePtr, mc: BytePtr):
        self.self_ctx = ac
        self.caller_ctx = mc
        self.user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)


# The entry callback (S0 trampoline handoff): runs real Mojo code on the
# synthetic stack.  ud = the T4Frame sidecar; payload reachable via
# fr[].user.  Yields via direct ms_ctx_switch (S0 demo form); on resume
# verifies the stack-local kept its address (A0-T3) and that the shared
# buffer was not clobbered, then returns (completion path back to the
# driver's ms_ctx_switch).
@export("t4_entry")
def t4_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[T4Frame]()
    var pl = fr[].user.bitcast[T4Payload]()

    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)

    if pl[].phase == 0:
        # First entry: record the stack-local address for the A0-T3
        # resume-point check, mutate the shared scratch, yield.
        pl[].marker = Int(local_p)
        pl[].buf[0] = 0x5EED
        pl[].phase = 1
        ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

        # -- EXACT RESUME POINT (A0-T3): the stack-local and the shared buffer
        # -- (incl. the driver sentinel) must be intact across the pause.
        if pl[].marker != Int(local_p):
            pl[].resume_ok = False
        if pl[].buf[0] != 0x5EED:
            pl[].resume_ok = False
        if pl[].buf[SB_SENT] != 0x0BAD:
            pl[].resume_ok = False
        pl[].buf[1] = 0xCAFE
        pl[].phase = 2


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
    var pl = stack_allocation[1, T4Payload]()
    pl[0] = T4Payload()
    pl[].buf = buf

    var stack_bytes = STACK_PAGES * ps

    # --- synthetic stack + entry-SP alignment ------------------------------
    var slots = stack_allocation[2, BytePtr]()
    var rc = ms_stack_alloc(stack_bytes, slots, slots + 1)
    if rc != 0:
        _iso_exit(1)
    var top = Int((slots + 1)[])
    if (top & ALIGN_MASK) != 0:
        _iso_exit(1)

    # ctx buffers: ALT = the synthetic-stack context, MAIN = the driver's
    # caller context (both ms_ctx_t-sized workspaces).
    var main_buf = stack_allocation[MS_CTX_SIZE // 8, Int]()
    var alt_buf = stack_allocation[MS_CTX_SIZE // 8, Int]()
    var main_ctx = main_buf.bitcast[Byte]()
    var alt_ctx = alt_buf.bitcast[Byte]()

    # Sidecar: self_ctx = ALT, caller_ctx = MAIN, user = the payload.
    var fs = stack_allocation[1, T4Frame]()
    fs[0] = T4Frame(alt_ctx, main_ctx)
    fs[].user = pl.bitcast[Byte]()

    # Prepare + enter the fresh context via the S0 entry_pointer mechanism.
    ms_ctx_make(alt_ctx, (slots + 1)[], entry_pointer["t4_entry"](), fs.bitcast[Byte]())
    ms_ctx_switch(main_ctx, alt_ctx)

    if pl[].phase != 1:
        failures.append("entry did not reach the yield point (phase " + String(pl[].phase) + ")")
    if buf[0] != 0x5EED:
        failures.append("entry did not mutate the shared buffer via userdata")
    if buf[2] != 0x0BAD:
        failures.append("driver sentinel clobbered while the fiber was sandwiched")

    # Resume ALT at the exact point; the thunk verifies, then completes.
    ms_ctx_switch(main_ctx, alt_ctx)
    if pl[].phase != 2:
        failures.append("entry did not complete after resume")
    if buf[1] != 0xCAFE:
        failures.append("resumed thunk did not continue its own stack frame")
    if not pl[].resume_ok:
        failures.append("stack-local address (or shared buffer) not stable across switch")

    # -- clean reclaim: free, then an equal-size replacement allocates -----
    ms_stack_free(slots[0])

    var slots2 = stack_allocation[2, BytePtr]()
    var rc2 = ms_stack_alloc(stack_bytes, slots2, slots2 + 1)
    if rc2 != 0:
        failures.append("equal-size replacement stack did not allocate after free")
    else:
        var top2 = Int((slots2 + 1)[])
        if (top2 & ALIGN_MASK) != 0:
            failures.append("replacement stack top not 16-aligned")
        ms_stack_free(slots2[0])

    if len(failures) == 0:
        print("T4 fiber: PASS")
    else:
        print("T4 fiber: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)