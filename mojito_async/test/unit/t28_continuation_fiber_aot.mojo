# mojito_async/test/unit/t28_continuation_fiber_aot.mojo
#
# A1.2 (issue #50) — REAL-CARRIER continuation acceptance driver (A1
# consensus fold T2).  Proves the FULL episode cycle of
# FiberContinuation[Fiber] over a REAL ms_ctx switch (vendored substrate,
# libmojito_spike.dylib):
#
#     start -> [body parks] -> resume -> [body re-parks] -> resume -> COMPLETED
#
# The mock-only evidence (t23) could not see the fold's three bugs:
#   - resume() flipped the ledger AFTER the switch, so during a real second
#     episode the ledger still read SUSPENDED and an in-body suspend()
#     REJECTED ITSELF (a real fiber could never replay a second episode);
#   - driver-side suspend() on a parked real fiber silently rewound the
#     saved registers instead of raising;
#   - the continuation never landed in COMPLETED through a real unwind
#     (the post-switch blind write to RESUMED_ONCE clobbered completion).
# This driver runs the REAL choreography and asserts:
#   - the ledger reads RUNNING (RESUMED_ONCE) BEFORE each entering switch —
#     the in-body suspend() during EVERY episode sees a running continuation
#     and parks a real fiber;
#   - after each driver-side resume() returns, the reconcile reflects the
#     carrier's REAL signals: parked -> SUSPENDED, unwound -> COMPLETED;
#   - exactly-once wake per episode (a phase/wake ledger in the body matches
#     the driver's resume count — no double-dispatch, no missed wake);
#   - finished() (carrier truth) is true at the end; the terminal
#     continuation raises on any further verb; complete() is idempotent;
#   - the single `user` payload is visible in-fiber and survives the episode;
#   - teardown: the driver's owning Fiber handle destroys the stack (the
#     continuation is a non-owning viewer over the pinned carrier), the
#     live-stack count is restored, destroy() is idempotent.
#
# STABILITY: the fiber and the continuation live in EXPLICIT stack cells
# (stack_allocation), not bare named vars, so the fiber body (running on its
# own stack, reached through a pinned pointer) and the driver ALWAYS address
# the same memory.  This is the same discipline the fiber seam uses
# ("fibers never relocate"; Scope/SeamCell own their objects in stable
# slots).  A named var can be register/spill-cached, which would let the
# driver and the in-fiber body observe different field values.
#
# EXTERN DISCIPLINE: AOT build (mojo build + execute) so the vendored
# mojito-sys externs (ms_stack_alloc, ms_live_stack_count, entry_pointer)
# resolve through the firewall; the harness links the dylib.  Identical
# pattern to t23_fiber_aot / t24_fiber_guards_aot.
#
# Verdict: exit 0 + "PASS"; any failure prints RED + raises (exit 1).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    c_free,
    c_malloc,
    entry_pointer,
    ms_live_stack_count,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
)
from mojito_async.fiber import (
    Fiber,
    FiberContinuation,
    FiberFrame,
    is_continuation_error,
    make_continuation,
    make_fiber,
)
from std.memory import stack_allocation


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


# The body drives the continuation through several caller frames (entry ->
# suspend -> running-check -> fiber methods); STACK_PAGES gives it generous
# headroom over the bare t23 thunk, which suspended with only one extra
# frame.
comptime STACK_PAGES = 16
# Generous comptime sizes for the opaque heap cells (>= the real struct sizes;
# b2 exposes no public sizeof in this build).
comptime CELL_PAYLOAD = Int(64)
comptime CELL_FIBER = Int(256)
comptime CELL_CONT = Int(256)



struct T28Payload:
    var cont: UnsafePointer[FiberContinuation[Fiber], MutAnyOrigin]
    var wakes: Int      # body entries since birth (one per episode)
    var parks: Int      # body parks since birth
    var phase: Int      # 1 entered-ep1 / 2 ep2 / 3 ep3 (park-point evidence)
    var user_seen: Int  # the user payload as observed from in-fiber

    def __init__(out self):
        self.cont = UnsafePointer[FiberContinuation[Fiber], MutAnyOrigin](
            unsafe_from_address=1
        )
        self.wakes = 0
        self.parks = 0
        self.phase = 0
        self.user_seen = 0


# The fiber body: real Mojo code on the synthetic stack.
#   - episode 1 (trampoline entry): record the user payload, park via the
#     continuation's suspend() — the fold made this legal by flipping the
#     ledger to RUNNING BEFORE the entering switch;
#   - episode 2 (exact park-point resume): re-park (re-entry edge);
#   - episode 3 (exact park-point resume): complete() (the carrier-unwind
#     seam) and return — the trampoline tail-switches back, and the
#     driver-side resume() reconcile reads finished() -> COMPLETED.
@export("t28_entry")
def t28_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var pl = fr[].user.bitcast[T28Payload]()

    # ---- episode 1: the first (and only) trampoline entry -----------------
    # (abi("C") cannot raise; the continuation verbs are captured in a
    # try/except that latches a negative phase so the driver reports a RED.)
    try:
        pl[].wakes += 1
        pl[].phase = 1
        pl[].user_seen = Int(pl[].cont[].user_payload())
        pl[].parks += 1
        pl[].cont[].suspend()   # park #1 -> the driver's resume() returns
    except e:
        pl[].phase = -1
        return

    # ---- episode 2: resumed EXACTLY at the park point ---------------------
    try:
        pl[].wakes += 1
        if pl[].phase != 1:
            pl[].phase = -2     # wrong continuation point (double-dispatch?)
        pl[].phase = 2
        pl[].parks += 1
        pl[].cont[].suspend()   # park #2 -> the driver's resume() returns
    except e:
        pl[].phase = -2
        return

    # ---- episode 3: resumed EXACTLY at the park point; the body unwinds ---
    try:
        pl[].wakes += 1
        if pl[].phase != 2:
            pl[].phase = -3
        pl[].phase = 3
        pl[].cont[].complete()  # carrier-unwind seam; the return below
        # switches back through the trampoline to the driver
    except e:
        pl[].phase = -3


def red(what: String) raises -> None:
    print("T28 continuation fiber: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var failures = List[String]()

    var cnt0 = ms_live_stack_count()
    var ps = Int(ms_page_size())
    var stack_bytes = STACK_PAGES * ps

    # Payload + continuation live in EXPLICIT STABLE CELLS on the driver's
    # stack (stable addresses for the whole episode; the fiber never owns
    # them and the body reaches them by pinned pointer).
    # Shared cells live in OPAQUE HEAP MEMORY (c_malloc), not on any stack or
    # in register-spillable structs: the fiber body runs on its own stack and
    # must observe exactly the same continuation/fiber fields the driver
    # writes.  Struct fields can otherwise be register-cached, which lets the
    # driver and the in-fiber body (across a register switch) read diverging
    # copies.
    var pl = UnsafePointer[T28Payload, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(CELL_PAYLOAD))
    )
    pl[0] = T28Payload()
    var fslot = UnsafePointer[Fiber, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(CELL_FIBER))
    )
    var ccell = UnsafePointer[FiberContinuation[Fiber], MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(CELL_CONT))
    )

    # --- acquire the synthetic stack (pool-seam shape) ----------------------
    var slots = stack_allocation[2, BytePtr]()
    if ms_stack_alloc(stack_bytes, slots, slots + 1) != 0:
        red("ms_stack_alloc failed")
    var ns = NativeStack(slots[0], (slots + 1)[])

    # --- bind the FIBER into its heap cell ------------------------------
    fslot[0] = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns),
        entry_pointer["t28_entry"](),
        pl.bitcast[Byte](),
    )

    # --- the continuation PINS the fiber cell, into its own heap cell;
    #     single owner of the fiber = the driver (destroyed below) ----------
    var user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x5EED)
    ccell[0] = make_continuation(fslot, user)

    # The body reaches the continuation cell through this payload pointer.
    pl[].cont = ccell

    if ccell[].state() != FiberContinuation.NEW:
        failures.append("fresh continuation not NEW")
    if ccell[].carrier_has_resumed():
        failures.append("carrier has_resumed before the first resume")

    # --- start(): claim the once-shot entry ---------------------------------
    ccell[].start()
    if ccell[].state() != FiberContinuation.STARTED:
        failures.append("start must move to STARTED")

    # --- driver-side suspend BEFORE any entry must raise (fold T2: the
    #     carrier reports no running fiber — never silently rewind) ---------
    var pre_suspend_rejected = False
    try:
        ccell[].suspend()
    except e:
        pre_suspend_rejected = is_continuation_error(e)
    if not pre_suspend_rejected:
        failures.append(
            "driver-side suspend before the first entry was NOT rejected"
        )

    # --- episode 1: resume enters; the body parks a REAL fiber; the
    #     reconcile reads the carrier's is_suspended() -> SUSPENDED ----------
    ccell[].resume()
    if ccell[].state() != FiberContinuation.SUSPENDED:
        failures.append(
            "after the body parked, the reconcile must read SUSPENDED (state="
            + ccell[].label() + ")"
        )
    if not ccell[].carrier_is_suspended():
        failures.append(
            "carrier must report the fiber suspended after the body parked"
        )
    if pl[].phase != 1:
        failures.append(
            "episode 1 did not run to its park point (phase="
            + String(pl[].phase) + ")"
        )
    if pl[].wakes != 1:
        failures.append(
            "episode 1 must wake the body exactly once (wakes="
            + String(pl[].wakes) + ")"
        )
    if pl[].parks != 1:
        failures.append("episode 1 must park exactly once")
    if pl[].user_seen != 0x5EED:
        failures.append("user payload not visible in-fiber")

    # --- a driver-side suspend of the PARKED continuation must raise --------
    var parked_suspend_rejected = False
    try:
        ccell[].suspend()
    except e:
        parked_suspend_rejected = is_continuation_error(e)
    if not parked_suspend_rejected:
        failures.append(
            "driver-side suspend of a parked continuation was NOT rejected"
        )

    # --- episode 2: the winning wake; the in-body suspend() sees the
    #     ledger RUNNING (fold T1 pre-switch flip) and re-parks --------------
    ccell[].resume()
    if ccell[].state() != FiberContinuation.SUSPENDED:
        failures.append(
            "the re-park must reconcile back to SUSPENDED (state="
            + ccell[].label() + ")"
        )
    if pl[].phase != 2:
        failures.append(
            "episode 2 did not resume at the first park point (phase="
            + String(pl[].phase) + ")"
        )
    if pl[].wakes != 2:
        failures.append(
            "episode 2 must wake the body exactly once (wakes="
            + String(pl[].wakes) + ")"
        )
    if pl[].parks != 2:
        failures.append("episode 2 must park exactly once")

    # --- episode 3: the final wake; the body unwinds; the driver-side
    #     reconcile reads finished() -> COMPLETED ----------------------------
    ccell[].resume()
    if ccell[].state() != FiberContinuation.COMPLETED:
        failures.append(
            "the final resume must reconcile to COMPLETED (state="
            + ccell[].label() + ")"
        )
    if pl[].phase != 3:
        failures.append(
            "episode 3 did not resume at the second park point (phase="
            + String(pl[].phase) + ")"
        )
    if pl[].wakes != 3:
        failures.append(
            "episode 3 must wake the body exactly once, no double-dispatch "
            "(wakes=" + String(pl[].wakes) + ")"
        )
    if pl[].parks != 2:
        failures.append("parks must stay at 2 after the final episode")
    if not ccell[].carrier_finished():
        failures.append(
            "carrier finished() must be true after the body unwound"
        )
    if ccell[].carrier_is_suspended():
        failures.append("carrier must not report suspended after completion")
    if not ccell[].is_completed():
        failures.append("ledger must report COMPLETED")

    # --- terminal: any further verb raises ----------------------------------
    var post_resume = False
    try:
        ccell[].resume()
    except e:
        post_resume = is_continuation_error(e)
    if not post_resume:
        failures.append("resume of a COMPLETED continuation was NOT rejected")

    var post_suspend = False
    try:
        ccell[].suspend()
    except e:
        post_suspend = is_continuation_error(e)
    if not post_suspend:
        failures.append("suspend of a COMPLETED continuation was NOT rejected")

    # complete() is idempotent on the terminal state.
    try:
        ccell[].complete()
    except e:
        failures.append("complete on COMPLETED must be idempotent, not raise")
    if ccell[].state() != FiberContinuation.COMPLETED:
        failures.append("complete must keep COMPLETED terminal")

    # --- the single user payload survives every episode ---------------------
    if Int(ccell[].user_payload()) != 0x5EED:
        failures.append("user payload lost")

    # --- teardown: the DRIVER owns the fiber and destroys it; the
    #     live-stack count is restored; a second destroy is a no-op ----------
    fslot[].destroy()
    if fslot[].alive():
        failures.append("destroy left the fiber alive")
    fslot[].destroy()  # second destroy must be a no-op
    if ms_live_stack_count() != cnt0:
        failures.append("live-stack count not restored after destroy")
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(ccell)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(fslot)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(pl)))

    if len(failures) != 0:
        print("T28 continuation fiber: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)

    # --- the exact episode sequence, asserted one last time -----------------
    print("T28 continuation fiber: PASS (start -> park -> resume -> re-park -> resume -> COMPLETED)")