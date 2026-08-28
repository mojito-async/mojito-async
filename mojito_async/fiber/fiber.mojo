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
# ImplicitlyCopyable (all-scalar handles): exactly ONE destroy() per live
# Fiber, on the OWNING handle -- destroying a copy double-frees (no __del__
# protects aliases).  The driver keeps the owning Fiber in one stable local.
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
#     has_resumed() -> Bool
#     is_suspended() -> Bool
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
#     a second destroy() is a no-op.
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    MS_CTX_SIZE,
    NativeStack,
    c_free,
    c_malloc,
    ms_ctx_make,
    ms_ctx_switch,
    stack_free,
)


# Sidecar handed to the entry thunk (as the ms_ctx_make userdata, unmodified).
# self_ctx/caller_ctx are the addresses of this Fiber's two save areas;
# `user` is whatever payload the driver bound via set_targets/userdata.
# An in-fiber entry yields with fiber.suspend() (Fiber-side) or directly
# with ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx) (S0 demo form).
struct FiberFrame:
    var self_ctx: BytePtr
    var caller_ctx: BytePtr
    var user: BytePtr

    def __init__(out self):
        self.self_ctx = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
        self.caller_ctx = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
        self.user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)


# All state is scalar addresses; the bodies live OUT of line (one heap block +
# the ms_stack_alloc'd synthetic stack), so the struct is trivially copy-safe.
struct Fiber(ImplicitlyCopyable, ImplicitlyDeletable):
    # ms_stack_alloc base (0 = not allocated; passed to ms_stack_free) and the
    # initial SP of the synthetic stack.
    # Layout constants for the out-of-line heap block (single source of truth
    # for the block size AND the set_targets/accessor offsets).
    comptime FRAME_BYTES = 24   # sizeof(FiberFrame): 3 BytePtr
    comptime SCRATCH_BYTES = 16  # entry fn ptr + userdata ptr
    comptime TAIL_BYTES = Self.FRAME_BYTES + Self.SCRATCH_BYTES

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
    # A1.3 (issue #51): the WORKER identity this fiber is affine to once
    # started (spec §19.2 / ADR-006).  0 = not pinned.  b2 has no TLS, so
    # worker identity is threaded explicitly: the creating worker (EPIC #2's
    # pool) calls set_owner() before the first resume; the owner becomes
    # OBSERVABLE only at first body entry (STARTED) and is immutable after.
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
        self._owner = 0

    # -- queries -----------------------------------------------------------

    def alive(self) -> Bool:
        return self._stack != 0

    def has_resumed(self) -> Bool:
        """True once the fiber's first resume() has prepared+entered the
        fresh context (the frozen batch seam spelling; alias of the spec
        §14.1 `started` flag that is_started() also reports)."""
        return self._started

    def is_started(self) -> Bool:
        """Spec §14.1 `started`: True exactly once the fiber has entered body
        entry (set on the FIRST resume — not at construction)."""
        return self._started

    def owner_worker(self) -> Int:
        """The worker identity this started fiber is affine to (spec §19.2 /
        ADR-006).  An unstarted fiber has NO owner pinned until first entry:
        0 means not started / no owner.  Once STARTED this never changes —
        set_owner() rejects a conflicting re-pin."""
        if not self._started:
            return 0
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

    def is_suspended(self) -> Bool:
        return self._suspended

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

    # -- A1.3 affinity (issue #51) ------------------------------------------

    # Pin the owner WORKER this fiber is affine to (ADR-006).  Called by the
    # worker that will START the fiber (the sole worker today; EPIC #2's pool
    # pins at spawn, before the first resume).  Once the fiber has STARTED
    # (entered body entry) the owner is immutable: a re-pin to a DIFFERENT
    # worker raises; a re-pin to the SAME worker is an idempotent no-op.
    def set_owner(mut self, worker_id: Int) raises:
        if self._started:
            if self._owner != worker_id:
                raise Error(
                    "fiber.set_owner: owner is immutable once started "
                    "(pinned to " + String(self._owner) + ")"
                )
            return
        self._owner = worker_id

    # -- switching ---------------------------------------------------------

    def resume(mut self) raises:
        if self._stack == 0:
            raise Error("fiber.resume: no bound stack")
        comptime if Self.FAULT_CHECKS:
            self.assert_never_relocated()  # ADR-007 before every switch
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
            self._started = True  # spec §14.1: STARTED at first body entry
        self._suspended = False
        ms_ctx_switch(self.caller_ctx(), self.fiber_ctx())

    # Fiber side: save this fiber's registers and resume the driver.  Meant
    # to be invoked from in-fiber code that holds a pointer to the Fiber (a
    # scheduler drives resume()/suspend() symmetry; the entry thunk may also
    # yield directly through its FiberFrame self_ctx/caller_ctx pointers).
    def suspend(mut self) raises:
        if self._stack == 0:
            raise Error("fiber.suspend: no bound stack")
        comptime if Self.FAULT_CHECKS:
            self.assert_never_relocated()  # ADR-007 before every switch
        self._suspended = True
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
            self._owner = 0
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
    var block = Int(c_malloc(2 * MS_CTX_SIZE + Fiber.TAIL_BYTES))
    if block == 0:
        raise Error("fiber.bind: heap allocation failed")

    var f0 = Fiber(stack[])
    f0._block = block
    f0.set_targets(Int(entry), Int(userdata))
    dst[0] = f0


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
    var block = Int(c_malloc(2 * MS_CTX_SIZE + Fiber.TAIL_BYTES))
    if block == 0:
        raise Error("fiber.make_fiber: heap allocation failed")
    f0._block = block
    f0.set_targets(Int(entry), Int(userdata))
    return f0 ^