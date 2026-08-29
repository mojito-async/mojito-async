# mojito_async/runtime/fiber_seam.mojo
#
# A1.5 (issue #53) — fiber integration into the A1 scheduler seam.
#
# This is the FIBER-BACKED DRIVE of the scheduler seam.  The generic
# scheduler_loop (runtime/scheduler.mojo, spec §21) stays verbatim and
# extern-free — the b2 JIT unit drivers (t11..t18/t20..t22) import it and
# must keep linking without the mojito-sys dylib (modular/modular#6971) —
# and the FIBER HANDLE IS THREADED THROUGH THE DRIVER VALUE (design decision
# #4, issue #53): an *_aot driver's dispatcher (statically known bodies,
# never dynamic dispatch) drives each record's fiber through THIS concrete
# module, where the ms_ctx_* externs legally sit.
#
# The choreography (spec §21/§27/§60):
#   - each RUNNABLE record claims a fiber (a SeamSlot bound over an ACQUIRED
#     synthetic stack — issue #49 binding; the #52 StackCache ownership
#     transfer is an EPIC #2 consumption point, see below);
#   - one drive slice = claim RUNNING (task.claim_running) -> seam_drive:
#     first entry makes the fresh context (ms_ctx_make, once) and switches
#     caller -> fiber; every later slice RE-ENTERS the fiber at its exact
#     saved frame (caller -> fiber) — no frame re-walk;
#   - a park is the task body's seam_park_switch (fiber -> caller): the
#     frame PHYSICALLY leaves the worker's native context; the dispatcher
#     then commits fiber_suspend_current (RUNNING -> PARKING -> WAITING,
#     generation-bumped reason) or fiber_yield_now (early-wake edge
#     PARKING -> RUNNABLE + FIFO re-enqueue, no wait epoch);
#   - a wake is fiber_resume_current (WAITING -> RUNNABLE + re-enqueue, the
#     #39 unpark kernel); the next scheduler slice re-enters the fiber at
#     its exact point;
#   - completion: the body UNWINDS (its entry thunk returns), so the frame
#     never reports a park — the seam_drive verdict is read from the
#     FiberFrame.parked flag that seam_park_switch stamps on the fiber
#     BEFORE the switch back (see DriveVerdict below).
#
# Each worker owns ONE native context (spec #18): the fiber's caller is the
# worker's own stack; any number of task frames run through it without
# stacking.
#
# Cheap path (issue #15, acceptance): a task whose body never parks is
# dispatched with plain execute() on the worker's native context — ZERO
# fiber switches; the Runtime's fiber_drives/fiber_switches toggle stays
# flat (the fast-path regression guard asserts 0 for non-parking runs).
#
# Frame contract (hardened): seam_drive on an already-terminal slot raises
# LOUDLY ("already-terminal") instead of silently walking the frame; the
# slot returns to service only through the explicit terminal transition
# (seam_destroy_slot; seam_bind_slot REJECTS a live fiber).
#
# EXTERN DISCIPLINE: every extern call site (ms_stack_alloc, ms_ctx_switch)
# sits at this CONCRETE module scope (or in the vendored firewall /
# fiber.mojo's concrete methods); generic functions here (fiber_suspend_*)
# never lower externs.  Consumed ONLY by the schema driver (b2 JIT integrity).
#
# Stack-cache seam (#52): A1.5 binds FRESH #49 reservations (the issue's
# "else a fresh #49 binding" branch).  Reusing a #52 StackCache cell across
# a fiber lifecycle needs the pool/fiber ownership transfer (the fiber owns
# the reservation from bind until destroy; the pool cannot observe that) —
# that transfer is an EPIC #2 (M:N) consumption point recorded here, with
# the #52 policy exercised stand-alone by the A1.5 stress driver.
from std.memory import stack_allocation

from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    ms_ctx_switch,
)
from mojito_async.fiber.fiber import Fiber, FiberFrame, make_fiber
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.runtime.join_handle import JoinHandle, SuspendReason
from mojito_async.runtime.park import park_commit, park_prepare, park_validate, unpark_current
from mojito_async.runtime.scheduler import yield_now


comptime FIBER_TOGGLE = True


# ---------------------------------------------------------------------------
# DriveVerdict — the frame-reported outcome of one seam_drive slice (T3)
# ---------------------------------------------------------------------------
#
# A frame-reported park/complete verdict (T3, issue #53 / consensus): the
# disassembler NO LONGER relies on hardcoded driver slice counts to decide
# whether a resume() returned because the body PARKED mid-frame or because a
# task COMPLETED.  seam_park_switch stamps FiberFrame.parked = 1 right
# before the fiber->caller switch; seam_drive reads the flag after resume()
# returns and returns Parked | Completed accordingly.  A generic EPIC #2
# dispatcher keys off this verdict, so a miscount can never silently wrong-
# transition a task.
struct DriveVerdict:
    """Frame-reported outcome of one seam_drive slice (issue #15, T3)."""

    comptime Parked = Int(1)
    comptime Completed = Int(0)

    var _v: Int

    def __init__(out self, v: Int):
        self._v = v

    def value(self) -> Int:
        return self._v

    def is_parked(self) -> Bool:
        return self._v == Self.Parked


# ---------------------------------------------------------------------------
# SeamSlot — one task's fiber + lifecycle state
# ---------------------------------------------------------------------------

struct SeamSlot(Movable, ImplicitlyDeletable):
    """One task's fiber and its drive lifecycle (A1.5, issue #53).

    DRIVER-OWNED and STABLE: each slot lives in a heap cell or a stable
    local the driver never moves once the fiber is WIRED (ADR-007 — the
    fiber sidecar and in-flight entry hold pointers into its block slots).
    A slot is Movable and never copied (its Fiber is Movable post-#49-fold);
    a WIRED slot must never be moved either — only its address is threaded.

    Lifecycle:
      make_seam_slot()        inert slot (no fiber)
      seam_bind_slot(...)     claim a fiber over an ACQUIRED stack; REJECTS
                              a slot whose fiber is still alive (terminal
                              transition first: seam_destroy_slot)
      seam_drive(rt, slot)    one dispatch slice; raises LOUDLY past
                              terminal
      seam_mark_completed     the driver's verdict that the body unwound
                              (terminal); drive past it is a loud error
      seam_destroy_slot(...)  idempotent teardown (stack + block released)
    """

    var fiber: Fiber
    var started: Bool   # at least one body entry happened (spec §14.1)
    var finished: Bool  # terminal (driver-declared); drive past it is an error

    def __init__(out self):
        self.fiber = Fiber()
        self.started = False
        self.finished = False


def make_seam_slot() -> SeamSlot:
    """Module factory (b2 has no static methods): an inert SeamSlot."""
    return SeamSlot()


def seam_slot_stride() -> Int:
    """sizeof(SeamSlot) measured as a pointer stride — drivers carve slot
    arrays in heap blocks (stack_pool's caller-owns-cells pattern)."""
    var one = stack_allocation[1, SeamSlot]()
    return Int(one + 1) - Int(one)


# ---------------------------------------------------------------------------
# Bind / drive / destroy
# ---------------------------------------------------------------------------

def seam_bind_slot(
    slot: UnsafePointer[SeamSlot, MutAnyOrigin],
    stack: NativeStack,
    entry: BytePtr,
    ud: BytePtr,
) raises:
    """Bind the ACQUIRED reservation into the slot's fiber: a fresh #49
    binding (make_fiber wires the entry/userdata scratch; the fiber OWNS the
    stack from here until destroy).  Resets the lifecycle flags.

    Reuse gate (frame contract): a slot whose fiber is still ALIVE is
    REJECTED loudly — the previous task must reach TERMINAL first
    (seam_destroy_slot) before the slot may serve a new task."""
    if slot[].fiber.alive():
        raise Error(
            "fiber_seam.seam_bind_slot: slot still holds a live fiber "
            "(terminal transition required before reuse; frame contract)"
        )
    var ns = stack  # stable lvalue so the by-value reservation can be bound
    slot[].fiber = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns), entry, ud
    )
    slot[].started = False
    slot[].finished = False


def seam_drive(mut rt: Runtime, slot: UnsafePointer[SeamSlot, MutAnyOrigin]) raises -> DriveVerdict:
    """ONE fiber-backed dispatch slice (the drive the generic scheduler_loop
    reaches through the driver value).  Returns the frame-reported Parked |
    Completed verdict (T3).

      - LOUD frame contract: an already-terminal slot raises instead of
        silently walking the frame again;
      - switch caller -> fiber (counted): the FIRST slice makes the fresh
        context (ms_ctx_make, deferred to the fiber's first resume); every
        later slice RE-ENTERS at the exact saved point;
      - after resume() returns, seam_park_switch stamps FiberFrame.parked=1
        iff the body parked mid-frame -> the return verdict is
        DriveVerdict.Parked; an unwound body (no stamp) -> DriveVerdict.
        Completed.  The verdict replaces hardcoded slice accounting;
      - counts: exactly 2 ms_ctx_switch calls per drive slice (switch-in +
        switch-out at the park or the completion trampoline).  The runtime
        fiber toggle is gated by the comptime FIBER_TOGGLE (T4) so the
        counters compile out of a release build; t16 asserts exact values.
    """
    if slot[].finished:
        raise Error(
            "fiber_seam.seam_drive: resume of an already-terminal fiber "
            "(resumed past its completion; frame contract violated)"
        )
    comptime if FIBER_TOGGLE:
        rt.note_fiber_drive()           # one fiber-backed dispatch slice
        rt.note_fiber_switch()          # caller -> fiber
    var fr = slot[].fiber.frame_ptr().bitcast[FiberFrame]()
    fr[].parked = False
    slot[].fiber.resume()
    comptime if FIBER_TOGGLE:
        rt.note_fiber_switch()          # fiber -> caller (park or trampoline)
    if not slot[].started:
        slot[].started = True
    if fr[].parked:
        return DriveVerdict(DriveVerdict.Parked)
    return DriveVerdict(DriveVerdict.Completed)


def seam_mark_completed(slot: UnsafePointer[SeamSlot, MutAnyOrigin]):
    """Record the slot's task reached TERMINAL (the body unwound on the
    completing dispatch slice — declared by the driver's slice accounting).
    From here seam_drive raises LOUDLY (the hardened frame contract) until
    the slot is torn down (seam_destroy_slot)."""
    slot[].finished = True
    slot[].started = True


def seam_destroy_slot(slot: UnsafePointer[SeamSlot, MutAnyOrigin]) raises:
    """Terminal teardown (idempotent): the fiber releases its synthetic
    stack reservation and its heap block; the slot returns to inert.

    T6 (issue #53): destroying a slot whose fiber is PARKED/SUSPENDED raises
    LOUDLY instead of destructing a live frame — the fiber's synthetic stack
    reserve is still in use, so a silent destroy here would free a live
    reservation (the stack-pool double-free class).  The FIRED (started)
    state checks first: only a started slot can hold a live frame; inert
    (never bound / already destroyed) and TERMINAL (finished) slots fall
    through to the idempotent destroy."""
    if slot[].started and not slot[].finished:
        var fr = slot[].fiber.frame_ptr().bitcast[FiberFrame]()
        var live = fr[].parked or slot[].fiber.is_suspended()
        if live:
            raise Error(
                "fiber_seam.seam_destroy_slot: slot holds a parked/suspended "
                "(live) fiber frame; teardown would free a live reservation "
                "(drive to terminal first)"
            )
    slot[].fiber.destroy()
    slot[].started = False
    slot[].finished = False


def seam_park_switch(fr: UnsafePointer[FiberFrame, MutAnyOrigin]):
    """In-fiber FIBER PARK: migrate the RUNNING frame OFF the worker's
    native context onto the fiber's saved registers (fiber -> caller).
    Stamps FiberFrame.parked = 1 BEFORE the switch so seam_drive can report
    a Parked verdict (T3) when the switch returns.  Called from the task
    body at its exact park point (spec §60)."""
    fr[].parked = True
    ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

# ---------------------------------------------------------------------------
# The A1.5 fiber-backed seam spellings of the scheduler primitives
# ---------------------------------------------------------------------------
# The FRAME already migrated (seam_park_switch + seam_drive); these close
# the STATE half through the #39 single-source park/wake kernel
# (park_prepare/park_validate/park_commit / unpark_current) and
# scheduler.yield_early's early-wake edge.  Generic-parameter functions
# here perform NO extern calls.

def fiber_suspend_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    reason: Int = SuspendReason.PARK,
) raises:
    """A1.5 `_suspend_current` (spec §60): the state commit of a fiber park.

    TWO-PHASE (#112 item 3, migrated from single-phase `park_current`):
    after seam_park_switch (the frame has ALREADY physically left this
    worker's native context), commit through park_prepare/park_validate/
    park_commit — PARKING -> WAITING (reason stamped, fresh wait epoch)
    normally, OR PARKING -> RUNNABLE in the early-wake case below.  A
    single-phase commit here would silently drop a cross-worker wake that
    lands in the PARKING window (e.g. a fiber parked mid-channel-recv or
    mid-timer-sleep, both now two-phase consumers themselves) — the exact
    A4.1/issue #55 class of bug, just on the fiber-seam consumer instead
    of Mutex.

    Early-wake window: unlike mutex/semaphore's claim_running (continue
    synchronously in the SAME call — their frame never left the worker),
    a fiber's frame has ALREADY left via seam_park_switch, so there is no
    more code to run in THIS dispatch.  A validate hit therefore commits
    PARKING -> RUNNABLE (no WAITING, no epoch bump — Q6: the record was
    never dequeued in the first place) and RE-ENQUEUES onto this (owner)
    worker's remote-ready queue — exactly the delivery unpark_current's
    own claimed-wake path already uses (spec §19.2: a started fiber is
    never local/stealable) — so a LATER scheduler slice re-enters the
    fiber at its exact seam_park_switch return point.  The worker is free
    for other RUNNABLE records either way."""
    park_prepare(h)
    if park_validate(h):
        park_commit(h)
        rt.push_remote(Int(h.tcb()), h.id())
        return
    park_commit(h, reason)


def fiber_yield_now[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """A1.5 yield_now (spec §27): the state commit of a fiber yield.

    Takes the early-wake edge (spec A0.5): PARKING -> RUNNABLE with
    immediate FIFO re-enqueue and NO wait epoch — the task was never
    WAITING, its generation is untouched.  The worker picks the next
    RUNNABLE record; this task's fiber re-enters on its next slice."""
    yield_now(rt, h)


def fiber_resume_current[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """A1.5 `resume_current` (spec §60): deliver readiness ONCE for a parked
    fiber — WAITING -> RUNNABLE + FIFO re-enqueue via the #39 unpark kernel
    (enqueue-once; an already-RUNNABLE task is a no-op).  The next scheduler
    slice re-enters the fiber at its exact saved frame."""
    unpark_current(rt, h)