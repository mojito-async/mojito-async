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
#   - completion: the body parks through a DIRECT ms_ctx_switch (it never
#     touches the fiber's suspended flag), so park-vs-complete is decided by
#     the DRIVER's deterministic slice bookkeeping (the single-worker loop
#     knows which slice it is — the t15/t25 pattern): on the completing
#     slice the driver calls seam_mark_completed() and settles RUNNING ->
#     COMPLETED + result.
#
# Each worker owns ONE native context (spec §18): the fiber's caller is the
# worker's own stack; any number of task frames run through it without
# stacking.
#
# Cheap path (issue #53, acceptance): a task whose body never parks is
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
# never lower externs.  Consumed ONLY by *_aot drivers (the b2 JIT cannot
# resolve dylib symbols through an imported module).
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
from mojito_async.runtime.park import park_current, unpark_current
from mojito_async.runtime.scheduler import yield_now


# ---------------------------------------------------------------------------
# SeamSlot — one task's fiber + lifecycle state
# ---------------------------------------------------------------------------

struct SeamSlot(ImplicitlyCopyable, ImplicitlyDeletable):
    """One task's fiber and its drive lifecycle (A1.5, issue #53).

    DRIVER-OWNED and STABLE: each slot lives in a heap cell or a stable
    local the driver never moves once the fiber is WIRED (ADR-007 — the
    fiber sidecar and in-flight entry hold pointers into its block slots).
    A slot is implicitly copyable (all-scalar Fiber handle) but a WIRED
    slot must never be copied; only its address is threaded.

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


def seam_drive(mut rt: Runtime, slot: UnsafePointer[SeamSlot, MutAnyOrigin]) raises:
    """ONE fiber-backed dispatch slice (the drive the generic scheduler_loop
    reaches through the driver value).

      - LOUD frame contract: a terminal slot (body already unwound) raises
        instead of silently walking the frame again;
      - switch caller -> fiber (counted): the FIRST slice makes the fresh
        context (ms_ctx_make, deferred to the fiber's first resume); every
        later slice RE-ENTERS at the exact saved point;
      - when the switch returns, the body either PARKED mid-frame (worker's
        native context untouched; the dispatcher then commits
        fiber_suspend_current / fiber_yield_now) or UNWOUND — park-vs-
        complete is the DRIVER's deterministic slice bookkeeping (t15/t25
        pattern); the completing slice calls seam_mark_completed() and
        settles RUNNING -> COMPLETED.  The frame never reports completion
        itself, because the body parks through a direct switch that does not
        touch the fiber's suspended flag;
      - counts: exactly 2 ms_ctx_switch calls per drive slice (switch-in +
        switch-out at the park or the completion trampoline).
    """
    if slot[].finished:
        raise Error(
            "fiber_seam.seam_drive: resume of an already-terminal fiber "
            "(resumed past its completion; frame contract violated)"
        )
    rt.note_fiber_drive()
    rt.note_fiber_switch()          # caller -> fiber
    slot[].fiber.resume()
    rt.note_fiber_switch()          # fiber -> caller (park or trampoline)
    if not slot[].started:
        slot[].started = True


def seam_mark_completed(slot: UnsafePointer[SeamSlot, MutAnyOrigin]):
    """Record the slot's task reached TERMINAL (the body unwound on the
    completing dispatch slice — declared by the driver's slice accounting).
    From here seam_drive raises LOUDLY (the hardened frame contract) until
    the slot is torn down (seam_destroy_slot) and reused through the
    required terminal transition."""
    slot[].finished = True
    slot[].started = True


def seam_destroy_slot(slot: UnsafePointer[SeamSlot, MutAnyOrigin]):
    """Terminal teardown (idempotent): the fiber releases its synthetic
    stack reservation and its heap block; the slot returns to inert."""
    slot[].fiber.destroy()
    slot[].started = False
    slot[].finished = False


def seam_park_switch(fr: UnsafePointer[FiberFrame, MutAnyOrigin]):
    """In-fiber FIBER PARK: migrate the RUNNING frame OFF the worker's native
    context onto the fiber's saved registers (fiber -> caller).  Called from
    the task body at its exact park point (spec §60); the worker's native
    context is untouched and immediately free for the next RUNNABLE record.
    The frame re-enters at this exact point on the next seam_drive.  The
    switch is counted by seam_drive (it covers the return side)."""
    ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

# ---------------------------------------------------------------------------
# The A1.5 fiber-backed seam spellings of the scheduler primitives
# ---------------------------------------------------------------------------
# The FRAME already migrated (seam_park_switch + seam_drive); these close
# the STATE half through the #39 single-source park/wake kernel
# (park_current / unpark_current) and scheduler.yield_now's early-wake edge.
# Generic-parameter functions here perform NO extern calls (b2 discipline).

def fiber_suspend_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    reason: Int = SuspendReason.PARK,
) raises:
    """A1.5 `_suspend_current` (spec §60): the state commit of a fiber park.

    RUNNING -> PARKING -> WAITING over the #39 kernel, stamping the wait
    REASON and claiming a fresh wait epoch (generation-bumped).  The worker
    is free for other RUNNABLE records; only a later wake re-enters this
    fiber."""
    park_current(rt, h, reason)


def fiber_yield_now[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """A1.5 `yield_now` (spec §27): the state commit of a fiber yield.

    Takes the early-wake edge (spec A0.5): PARKING -> RUNNABLE with
    immediate FIFO re-enqueue and NO wait epoch — the task was never
    WAITING, its generation is untouched.  The worker picks the next
    RUNNABLE record; this task's fiber re-enters on its next slice."""
    yield_now(rt, h)


def fiber_resume_current[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """A1.5 `resume_current` (spec §27): deliver readiness ONCE for a parked
    fiber — WAITING -> RUNNABLE + FIFO re-enqueue via the #39 unpark kernel
    (enqueue-once; an already-RUNNABLE task is a no-op).  The next scheduler
    slice re-enters the fiber at its exact saved frame via seam_drive."""
    unpark_current(rt, h)
