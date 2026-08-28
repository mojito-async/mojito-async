# mojito_async/fiber/fiber.mojo
#
# A1.1 (issue #49) — fiber context/stack binding over the vendored mojito-sys
# NativeStack / NativeContext substrate (ms_stack_alloc/free, ms_ctx_make/
# switch).  This is the A0.4 (spike #13) fiber productionized onto the A1
# codebase, keeping the proven spike geometry VERBATIM.
#
# The full S0/A0.4 bisect (PR #25, tests t4/t4b, 3/3) showed this exact one-
# heap-block layout survives real switch cycles, so we keep it rather than
# re-architecting:
#
#     _fiber  — THIS fiber's own continuation (registers saved whenever the
#               fiber suspends; the fresh-ctx ms_ctx_make target on first
#               entry).
#     _caller — the driver's (caller's) continuation, saved by the first
#               resume() and restored every time the fiber suspends.
#
# Layout: the two 168-byte save areas, the 24-byte FiberFrame sidecar and
# the entry/userdata scratch live in ONE malloc'd heap block
# (2*MS_CTX_SIZE + 40 bytes, 16-aligned), referenced by addresses, freed in
# destroy().  This keeps the Fiber struct all-scalar.
#
# Lifecycle: resume() DEFERS ms_ctx_make until the FIRST call, so the sidecar
# and entry pointers bind to this object's finally-located slots (the object
# must not be relocated while a switch is in flight -- see the invariants
# below).  destroy() is idempotent (its alive() guards make a second call a
# no-op) and frees BOTH the synthetic stack and the heap block.  Fiber is
# MOVABLE (non-copyable), NOT ImplicitlyCopyable: copying a Fiber is a COMPILE
# error, so destruction is single-owner by construction -- a move (make_fiber
# value-return, bind's slot fill, slot[].fiber = f) transfers ownership to
# exactly one handle, and only that handle may destroy().  This closes the
# copy->destroy->destroy double-free / use-after-destroy alias hazard the
# previous ImplicitlyCopyable handle left open.
#
# A1.1 fold (issue #49): `_completed` + finished().  resume() raises a loud
# Error (not a raw brk 0x66 trap) on an already-completed fiber.
# mark_completed() is the driver/carrier seam that records "the body unwound";
# finished() unblocks the FiberContinuation conformance (#50/#96) which
# reconciles via finished()/is_suspended().
#
# --- b2 EXTERN DISCIPLINE (modular/modular#6971/#7004 family) -------------
# ALL extern declarations live at concrete module scope in the vendored
# binder mojito_async/vendor/mojito_sys.mojo -- the C-ABI firewall.  This
# module declares NO @extern of its own; its methods call the vendor-module
# defs.  The BODY of the switch also stays concrete: resume()/suspend()
# lower ms_ctx_make/ms_ctx_switch through the firewall, verified end-to-end
# (mojo build + execute) to survive a real switch cycle via the imported
# module.  JIT (`mojo run`) drivers must stay extern-free; the switch-bearing
# acceptance driver therefore uses the *_aot pattern (mojo build + execute),
# exactly like spike t4b and test/stress/t11_stress_aot.
#
# Public API (the batch Fiber seam, frozen):
#   make_fiber(stack, entry, userdata) raises -> Fiber   module factory
#   bind(dst, stack, entry, userdata) raises             module factory
#   struct Fiber:
#     resume(mut self) raises        # prepare + resume the one-shot continuation
#     suspend(mut self) raises       # fiber-side: yield back to the driver
#     destroy(mut self)              # idempotent teardown (stack + block)
#     finished() -> Bool              # the body unwound (mark_completed set it)
#     mark_completed()                # driver/carrier seam: records completion
#     stack_ptr() -> NativeStack     # the bound reservation (for pool reclaim)
#     alive() -> Bool
#     stack_base()/stack_top()/fiber_ctx()/caller_ctx()/frame_ptr()/
#     entry_ptr()/userdata_ptr()     # accessors for the switch choreography
#     set_targets(entry, userdata)   # late-wire the entry/userdata scratch
#
# Invariants (documented, driver-owned):
#   - one live Fiber = one owner local; never move a WIRED Fiber (post-first-
#     resume) while a switch is in flight (sidecar + in-flight entry hold
#     pointers into its block slots).
#   - resume()/suspend() raise on an unbound (no stack) fiber.
#   - after destroy(), the fiber is an inert shell (alive() == False);
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    MS_CTX_SIZE,
    NativeStack,
    c_free,
    c_malloc,
    ms_ctx_make,
    ms_ctx_switch,
    ms_live_stack_count,
    stack_free,
)
from mojito_async.fiber.continuation import FiberMotion
from std.atomic import Ordering, fence

# Sidecar handed to the entry thunk (as the ms_ctx_make userdata, unmodified).
# self_ctx/caller_ctx are the addresses of this Fiber's two save areas;
# `user` is whatever payload the driver bound via set_targets/userdata.
# An in-fiber entry yields with fiber.suspend() (Fiber-side) or directly
# with ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx) (S0 demo form).
struct FiberFrame:
    var self_ctx: BytePtr
    var caller_ctx: BytePtr
    var user: BytePtr
    # T3 (issue #53): set by seam_park_switch right BEFORE the fiber->caller
    # switch; cleared on entry.  seam_drive reads it after resume() returns to
    # report Parked vs Completed — the frame-reported verdict a generic EPIC
    # #2 dispatcher keys on (no driver slice arithmetic).
    var parked: Bool

    def __init__(out self):
        self.self_ctx = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
        self.caller_ctx = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
        self.user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)


# All state is scalar addresses; the bodies live OUT of line (one heap block +
# the ms_stack_alloc'd synthetic stack).  MOVABLE (NOT ImplicitlyCopyable): a
# copy is a COMPILE error, so destruction stays single-owner by construction.
struct Fiber(Movable, ImplicitlyDeletable, FiberMotion):
    # ms_stack_alloc base (0 = not allocated; passed to ms_stack_free) and the
    # initial SP of the synthetic stack.
    # Layout constants for the out-of-line heap block (single source of truth
    # for the block size AND the set_targets/accessor offsets).
    comptime FRAME_BYTES = 32   # sizeof(FiberFrame): 3 BytePtr + parked Bool
    comptime SCRATCH_BYTES = 16  # entry fn ptr + userdata ptr
    # Cross-switch flags (3 Ints) live in the heap block tail so the fiber
    # body (running on its own stack) and the driver see ONE consistent copy:
    # b2 keeps struct fields register-resident across the register switch, so
    # a struct-field flag written in one context would never be observed by
    # the other.  Heap-thru-pointer fields DO synchronize across the opaque
    # ms_ctx_switch (identical to the driver payload channel).
    comptime FLAG_BYTES = 24
    comptime TAIL_BYTES = Self.FRAME_BYTES + Self.SCRATCH_BYTES + Self.FLAG_BYTES
    # Oversubscription cap (A1.1 fold, issue #49): the vendored substrate's
    # process-global resume table holds 64 rows (2 per fiber), so the process
    # can have at most 32 concurrently live fibers; a 33rd saturates the table
    # and traps with SIGILL (brk 0x67).  bind()/make_fiber() raise a catchable
    # Error BEFORE allocating, so A1 fails loudly instead of trapping.
    # EPIC #2 (issue #101) removes the cap.
    # (retired: the 32-live-fiber substrate cap was removed by issue #101)

    var _stack: Int
    var _top: Int
    # Address of the out-of-line block: fiber ctx @0, caller ctx @MS_CTX_SIZE,
    # FiberFrame sidecar @2*MS_CTX_SIZE, entry scratch @+FRAME_BYTES,
    # userdata scratch @+FRAME_BYTES+8.  (0 = not allocated.)
    var _block: Int

    # ms_ctx_make is deferred to the first resume() so the sidecar/userdata
    # pointers bind to THIS object's slots (stable once it has landed).
    var _prepared: Bool
    # True once the first resume() has prepared+entered the fresh context.
    var _started: Bool
    # True while the fiber has yielded back to the driver and is waiting for
    # the next resume (set by suspend(), cleared by a resume() that re-enters).
    var _suspended: Bool
    # True once the body unwound (the driver/carrier called mark_completed()).
    # resume() raises on a completed fiber instead of letting the asm
    # trampoline re-entry trap (brk 0x66).
    var _completed: Bool

    # A1.3 (issue #51): the WORKER identity this fiber is affine to once
    # started (spec §19.2 / ADR-006).  0 = not pinned.  b2 has no TLS, so
    # worker identity is threaded explicitly: the creating worker (EPIC #2's
    # pool) calls set_owner() before the first resume; the owner becomes
    # OBSERVABLE via owner_worker() only after STARTED.
    var _owner: Int

    # A1.3 (issue #51) — ADR-007: live stacks never relocate.  In fault-
    # enabled builds the switch helpers assert stack-locality before every
    # switch; the assertion is a single compare (no allocation).
    comptime FAULT_CHECKS = True

    # Zero-arg ctor: inert Fiber (nothing allocated).  The driver may hold
    # `var f = Fiber()` and hand `to=f` to bind()/init.
    def __init__(out self):
        self._stack = 0
        self._top = 0
        self._block = 0
        self._prepared = False
        self._started = False
        self._suspended = False
        self._completed = False
        self._owner = 0

    # All-scalar ctor binding an existing reservation: `stack` (base/top from
    # ms_stack_alloc).  The block is not yet allocated (deferred to the
    # factories / a later bind).
    def __init__(out self, stack: NativeStack):
        self._stack = Int(stack.base)
        self._top = Int(stack.top)
        self._block = 0
        self._prepared = False
        self._started = False
        self._suspended = False
        self._completed = False
        self._owner = 0

    # -- queries -----------------------------------------------------------

    def alive(self) -> Bool:
        return self._stack != 0

    def has_resumed(self) -> Bool:
        if self._block == 0:
            return False
        return self._started_flag()[0] == 1

    def is_suspended(self) -> Bool:
        if self._block == 0:
            return False
        return self._suspended_flag()[0] == 1

    # True once the body unwound (mark_completed set it).  The continuation
    # seam (#50/#96) reconciles on finished()/is_suspended(); a caller should
    # never resume() a finished fiber (resume() raises loudly instead).
    def finished(self) -> Bool:
        if self._block == 0:
            return False
        return self._completed_flag()[0] == 1

    # Driver/carrier seam: record that the body unwound (the single-shot
    # continuation is done).  No-op-safe: does not itself free anything
    # (destroy() owns teardown).
    def mark_completed(mut self):
        self._completed = True
        if self._block != 0:
            self._completed_flag()[0] = 1

    def is_started(self) -> Bool:
        """Spec §14.1 `started`: True exactly once the fiber has entered body
        entry (set on the FIRST resume — not at construction).  Reads the
        cross-switch heap flag (synced through the fiber's own block) so a
        driver sees the body-run state after a switch back."""
        if self._block == 0:
            return False
        return self._started_flag()[0] == 1

    def owner_worker(self) -> Int:
        """The worker identity this started fiber is affine to (spec §19.2 /
        ADR-006).  An unstarted fiber has NO owner pinned until first entry:
        0 means not started / no owner.  Once STARTED this never changes —
        set_owner() rejects a conflicting re-pin.

        T5 (issue #51): this owner read is an ACQUIRE fence paired with the
        RELEASE in set_owner(), so a cross-worker producer (EPIC #2's pool)
        reading the pinned worker id from ANOTHER worker thread sees the
        fiber's synchronized state together with the store.  A no-op pass-
        through on today's single cooperative worker."""
        if not self._started:
            return 0
        fence[Ordering.ACQUIRE]
        return self._owner

    def assert_never_relocated(self) raises:
        """ADR-007 (issue #51): the live stack address is fixed at the first
        ms_stack_alloc and never reassigned; this Fiber only ever releases
        that SAME reservation (in destroy()).  Called from the switch helpers
        in fault-enabled builds (FAULT_CHECKS) and by drivers/EPIC #2 as a
        direct policy assertion.  Raises only on a fiber with no live stack
        (unbound/destroyed)."""
        if self._stack == 0:
            raise Error(
                "fiber.assert_never_relocated: no live stack (unbound/destroyed)"
            )

    def set_owner(mut self, worker_id: Int) raises:
        """Pin the owner WORKER this fiber is affine to (ADR-006).  Called by
        the worker that will START the fiber (the sole worker today; EPIC #2's
        pool pins at spawn, before the first resume).  Once the fiber has
        STARTED (entered body entry) the owner is immutable: a re-pin to a
        DIFFERENT worker raises; a re-pin to the SAME worker is an idempotent
        no-op.  The pin store is RELEASED so any acquiring owner_worker() on
        another worker thread sees the fiber's synchronized state together
        with the store."""
        if self._started:
            if self._owner != worker_id:
                raise Error(
                    "fiber.set_owner: owner is immutable once started "
                    "(pinned to " + String(self._owner) + ")"
                )
            return
        self._owner = worker_id
        fence[Ordering.RELEASE]

    def stack_ptr(self) -> NativeStack:
        return NativeStack(
            UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._stack),
            UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._top),
        )

    # -- address accessors --------------------------------------------------

    def stack_base(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._stack)

    def stack_top(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._top)

    def fiber_ctx(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._block)

    def caller_ctx(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](
            unsafe_from_address=self._block + MS_CTX_SIZE
        )

    def frame_ptr(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](
            unsafe_from_address=self._block + 2 * MS_CTX_SIZE
        )

    def entry_ptr(mut self) -> BytePtr:
        var p = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=self._block + 2 * MS_CTX_SIZE + Self.FRAME_BYTES
        )
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=p[])

    def userdata_ptr(mut self) -> BytePtr:
        var p = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=self._block + 2 * MS_CTX_SIZE + Self.FRAME_BYTES + 8
        )
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=p[])

    # Cross-switch flag accessors (heap).  Each returns an opaque pointer into
    # the fiber's heap block; reads/writes through them are honored as memory
    # by the compiler across the register switch.
    def _flag_base(self) -> Int:
        return self._block + 2 * MS_CTX_SIZE + Self.FRAME_BYTES + Self.SCRATCH_BYTES

    def _suspended_flag(self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=self._flag_base())

    def _started_flag(self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=self._flag_base() + 8)

    def _completed_flag(self) -> UnsafePointer[Int, MutAnyOrigin]:
        return UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=self._flag_base() + 16)

    def _zero_flags(mut self):
        if self._block == 0:
            return
        self._suspended_flag()[0] = 0
        self._started_flag()[0] = 0
        self._completed_flag()[0] = 0

    # Wire the entry/userdata addresses into the heap scratch.  This is what
    # a late-bound driver calls AFTER make_fiber/bind with @export'd entry
    # pointers materialized at the driver's own scope.
    def set_targets(mut self, entry: Int, ud: Int):
        var base = self._block + 2 * MS_CTX_SIZE
        var ep = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=base + Self.FRAME_BYTES
        )
        ep[] = entry
        var up = UnsafePointer[Int, MutAnyOrigin](
            unsafe_from_address=base + Self.FRAME_BYTES + 8
        )
        up[] = ud

    # -- switching ---------------------------------------------------------

    # Driver side: save the current (driver) registers into the caller slot
    # and resume this fiber.  On the FIRST call this also prepares the fresh
    # context (ms_ctx_make) bound to the entry callback; afterwards it resumes
    # the fiber at its exact suspension point.
    def resume(mut self) raises:
        if self._stack == 0:
            raise Error("fiber.resume: no bound stack")
        # The public `Fiber(stack)` ctor builds a Fiber with no out-of-line
        # block yet (deferred to a factory/bind); resume() writes the sidecar
        # into that block, so guard _block (not just _stack) to raise rather
        # than SEGV on an un-bound block.
        comptime if Self.FAULT_CHECKS:
            self.assert_never_relocated()  # ADR-007 before every switch
        if self._block == 0:
            raise Error("fiber.resume: no bound block (Fiber not factory-bound)")
        comptime if Self.FAULT_CHECKS:
            self.assert_never_relocated()  # ADR-007 before every switch
        # A completed fiber's body has unwound; re-entering it through the asm
        # trampoline would trap (brk 0x66).  Raise a loud Error instead.  Read
        # the heap flag (synced cross-switch).
        if self._completed_flag()[0] != 0:
            raise Error("fiber.resume: fiber already complete")
        if not self._prepared:
            self._prepared = True
            var fp = self.frame_ptr()
            var fr = fp.bitcast[FiberFrame]()
            fr[].self_ctx = self.fiber_ctx()
            fr[].caller_ctx = self.caller_ctx()
            fr[].user = self.userdata_ptr()
            ms_ctx_make(
                self.fiber_ctx(), self.stack_top(), self.entry_ptr(), fp
            )
            self._started = True
            self._started_flag()[0] = 1
        self._suspended = False
        self._suspended_flag()[0] = 0
        ms_ctx_switch(self.caller_ctx(), self.fiber_ctx())

    # Fiber side: save this fiber's registers and resume the driver.  Meant
    # to be invoked from in-fiber code that holds a pointer to the Fiber (a
    # scheduler drives resume()/suspend() symmetry; the entry thunk may also
    # yield directly through its FiberFrame self_ctx/caller_ctx pointers).
    def suspend(mut self) raises:
        if self._stack == 0:
            raise Error("fiber.suspend: no bound stack")
        # Guard _block too (mirror of resume()): the public Fiber(stack) ctor
        # has no block yet; a driver suspending a stack-only Fiber must raise,
        # not SEGV (fold T4).
        comptime if Self.FAULT_CHECKS:
            self.assert_never_relocated()  # ADR-007 before every switch
        if self._block == 0:
            raise Error("fiber.suspend: no bound block (Fiber not factory-bound)")
        if self._completed_flag()[0] != 0:
            raise Error("fiber.suspend: fiber already complete")
        self._suspended = True
        self._suspended_flag()[0] = 1
        ms_ctx_switch(self.fiber_ctx(), self.caller_ctx())

    # -- teardown ----------------------------------------------------------

    # Idempotent: releases the synthetic stack (guard page included) and the
    # heap block.  alive() guards make a second destroy() (and destroy() on
    # a never-wired fiber) a no-op.  Single-owner semantics: exactly ONE
    # destroy() per live Fiber, on the OWNING handle.
    def destroy(mut self):
        if self._stack != 0:
            stack_free(NativeStack(self.stack_base(), self.stack_top()))
            self._stack = 0
            self._top = 0
            self._prepared = False
            self._started = False
            self._suspended = False
            self._completed = False
            self._owner = 0
            self._zero_flags()
        if self._block != 0:
            c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._block))
            self._block = 0


# Module-level factory (b2 has no static methods).
#
# Allocates the out-of-line heap block for the two ctx save areas + sidecar,
# wires entry/userdata into the scratch, then fills `dst` with a live Fiber
# bound to `stack`.  `stack` is an acquired ms_stack_alloc reservation (the
# stack-pool seam, issue #52); the Fiber OWNS it from here until destroy().
#
# Raises when the heap block cannot be allocated (the stack is left
# untouched -- the caller/stack-pool retains ownership on failure).
def bind(
    dst: UnsafePointer[Fiber, MutAnyOrigin],
    stack: UnsafePointer[NativeStack, MutAnyOrigin],
    entry: BytePtr,
    userdata: BytePtr,
) raises:
    """Bind an ACQUIRED synthetic stack into a live Fiber at dst.

    b2-codegen note (modular/modular#6971 family): extern CALLS stay at the
    vendor firewall's concrete module scope; this factory only does pointer
    arithmetic + heap-block allocation (c_malloc is also a firewall extern),
    so it is safe under mojo build + execute (verified end-to-end).
    """
    # The substrate resume table that once capped the process at 32 live
    # fibers (2 rows/fiber, brk #0x67 SIGILL) was REMOVED by issue #101
    # (A2.0 M:N rework): ms_ctx_t carries its own return_to and the switch
    # path is thread-safe, so there is no live-fiber cap to guard.
    var block = Int(c_malloc(2 * MS_CTX_SIZE + Fiber.TAIL_BYTES))
    if block == 0:
        raise Error("fiber.bind: heap allocation failed")

    var f0 = Fiber(stack[])
    f0._block = block
    f0._zero_flags()
    f0.set_targets(Int(entry), Int(userdata))
    dst[0] = f0^


# Module-level factory creating a Fiber over an ACQUIRED stack (the stack-pool
# seam, issue #52) and wiring the entry/userdata scratch.  Equivalent to
# bind() but returns the Fiber by value (single stable local on the caller's
# stack is required -- see the invariants above).
#
# `stack` was acquired from ms_stack_alloc (via the pool).  On heap-block
# allocation failure, the stack is left untouched and this raises.
def make_fiber(
    stack: UnsafePointer[NativeStack, MutAnyOrigin],
    entry: BytePtr,
    userdata: BytePtr,
) raises -> Fiber:
    var f0 = Fiber(stack[])
    # The 32-live-fiber substrate cap was removed by issue #101 (see bind()).
    var block = Int(c_malloc(2 * MS_CTX_SIZE + Fiber.TAIL_BYTES))
    if block == 0:
        raise Error("fiber.make_fiber: heap allocation failed")
    f0._block = block
    f0._zero_flags()
    f0.set_targets(Int(entry), Int(userdata))
    return f0 ^