# spike/colorless_runtime/fiber.mojo
#
# A0.4 (issue #13) — fiber wrapper over the vendored NativeStack /
# NativeContext substrate (ms_stack_alloc/free, ms_ctx_make/switch).
#
# Struct Fiber wraps ONE synthetic stack plus the two 168-byte ms_ctx_t save
# areas needed for self-contained switching:
#
#     _fiber  — THIS fiber's own continuation (registers saved whenever the
#               fiber suspends; the fresh-ctx ms_ctx_make target on first
#               entry).
#     _caller — the driver's (caller's) continuation, saved by the first
#               resume() and restored every time the fiber suspends.
#
# Entry-callback mechanism is the S0-proven one, kept verbatim: the caller
# supplies an @export'd abi("C") function and materializes its code address
# with mojito_spike.entry_pointer; create() hands it to ms_ctx_make.  The
# trampoline entered on the synthetic stack receives userdata = a small
# FiberFrame sidecar containing
#     self_ctx / caller_ctx — the two slot addresses (S0 return_to
#               bookkeeping preserved),
#     user                — the payload given to create(), passed through
#               UNMODIFIED (S0 trampoline contract).
# The supplied entry runs real Mojo code on the synthetic stack; a yield is a
# switch from self_ctx to caller_ctx; returning unwinds through the trampoline
# completion path back to the driver (the driver's resume() returns).
#
# Mojo 1.0.0b2 dialect notes (same conventions as the vendored bindings and
# the sibling lanes; see S0 SPIKE_REPORT "Observed Mojo/compiler
# assumptions"):
#   - `def` only; module-level `create()` factory (b2 has no statics);
#   - UnsafePointer via `to=<mut referent>` / keyword-only
#     `unsafe_from_address`; origins are concrete (MutAnyOrigin here);
#   - raise() accepts the builtin Error only.
#
# --- b2-JIT bug note (CONDITIONAL-GO candidate; Main acknowledged) ---------
# Calling an imported-module factory under `mojo run` (JIT) reliably crashes
# the compiler runtime at codegen: SIGSEGV inside libKGENCompilerRTShared (or
# alternately "failed to lower module to LLVM IR"), for EVERY shape tried
# (by-value return, out-slot void, with/without `raises`).  The identical
# construction INLINE in the calling module compiles and runs a full switch
# cycle (bisect evidence in PR #25; matches the S0 SPIKE_REPORT JIT-fragility
# finding "crashes inside libKGENCompilerRTShared, never inside
# libmojito_spike").  This factory therefore stays for API completeness
# (A0.6 consumes it), and call sites under `mojo run` MUST construct the
# Fiber inline (Fiber(stack, top, block) + set_targets + resume/suspend/
# destroy + ms_stack_free) until the toolchain is upgraded — re-test on
# upgrade.  t4_fiber.mojo does exactly that.
# ---------------------------------------------------------------------------
#
# Layout: the two 168-byte save areas, the 24-byte sidecar and the
# entry/userdata scratch live in ONE heap block (2*MS_CTX_SIZE + 40 bytes,
# C malloc'd, 16-aligned), referenced by addresses, freed in destroy().  This
# keeps the Fiber struct all-scalar; the InlineArray-fields variant crashed
# the b2 JIT for nested-field + two-array aggregates (also bisected).
#
# Lifecycle: create() raises when ms_stack_alloc OR the heap allocation
# fails (releasing what it acquired); destroy() (idempotent) frees the
# synthetic stack AND the heap block.  No __del__: the driver owns the
# lifecycle (calls destroy()).  Single-owner semantics: exactly ONE destroy()
# per live Fiber, on the OWNING handle -- destroying any COPY of the same
# handle double-frees (Fiber is ImplicitlyCopyable; no __del__ guards aliases).
# A
# Fiber whose save areas were wired by the first resume() must not be
# relocated while a switch is in flight (the sidecar and any in-flight entry
# hold pointers into its slots) — the spike keeps the Fiber in one stable
# local; A0.6's scheduler owns the same invariant.
#
# Link with:  mojo run -Xlinker <repo>/libmojito_spike.dylib
#                    -I spike/colorless_runtime -I spike/colorless_runtime/vendor/mojito-sys

from std.memory import stack_allocation

from mojito_spike import (
    BytePtr,
    MS_CTX_SIZE,
    entry_pointer,
    ms_ctx_make,
    ms_ctx_switch,
    ms_stack_alloc,
    ms_stack_free,
)


@extern("malloc")
def _c_malloc(size: Int) abi("C") -> BytePtr: ...

@extern("free")
def _c_free(p: BytePtr) abi("C"): ...


# Sidecar handed to the entry thunk (as the ms_ctx_make userdata, unmodified).
# self_ctx/caller_ctx are the addresses of this Fiber's two save areas; `user`
# is whatever payload create() was given.  An in-fiber entry yields with
# ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx) — exactly the S0 demo entry.
def _ms_stack_alloc_bound(
    bytes: Int,
    out_base: UnsafePointer[BytePtr, MutUntrackedOrigin],
    out_top: UnsafePointer[BytePtr, MutUntrackedOrigin],
) raises -> Int32:
    """Thin binding of ms_stack_alloc with a named-Int first argument
    (see the b2 codegen workaround note above)."""
    return ms_stack_alloc(bytes, out_base, out_top)


struct FiberFrame:
    var self_ctx: BytePtr
    var caller_ctx: BytePtr
    var user: BytePtr

    def __init__(out self):
        self.self_ctx = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
        self.caller_ctx = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
        self.user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)


# All state is scalar addresses; the bodies live OUT of line (one heap block +
# the ms_stack_alloc'd synthetic stack), so the struct is trivially copy-safe
# under the b2 JIT.
struct Fiber(ImplicitlyCopyable, ImplicitlyDeletable):
    # ms_stack_alloc base (0 = not allocated; passed to ms_stack_free) and the
    # initial SP of the synthetic stack.
    # Layout constants for the out-of-line heap block (single source of
    # truth for create()'s malloc size AND the set_targets/accessor offsets).
    comptime FRAME_BYTES = 24  # sizeof(FiberFrame): 3 BytePtr
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

    # Zero-arg ctor: inert Fiber (nothing allocated).  The driver may hold
    # `var f = Fiber()` and hand `to=f` to create()/inline construction.
    def __init__(out self):
        self._stack = 0
        self._top = 0
        self._block = 0
        self._prepared = False

    # All-scalar ctor binding an existing allocation: `stack`/`top` from
    # ms_stack_alloc, `block` = the heap base (see the layout note).
    def __init__(out self, stack: Int, top: Int, block: Int):
        self._stack = stack
        self._top = top
        self._block = block
        self._prepared = False

    # Wire the entry/userdata addresses into the heap scratch (no aggregate
    # mutation; keeps the ctor scalar).
    def set_targets(mut self, entry: Int, ud: Int):
        var base = self._block + 2 * MS_CTX_SIZE
        var ep = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=base + Self.FRAME_BYTES)
        ep[] = entry
        var up = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=base + Self.FRAME_BYTES + 8)
        up[] = ud

    # -- queries -----------------------------------------------------------

    def alive(self) -> Bool:
        return self._stack != 0

    # -- address accessors --------------------------------------------------

    def stack_base(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._stack)

    def stack_top(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._top)

    def fiber_ctx(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._block)

    def caller_ctx(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._block + MS_CTX_SIZE)

    def frame_ptr(mut self) -> BytePtr:
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._block + 2 * MS_CTX_SIZE)

    def entry_ptr(mut self) -> BytePtr:
        var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=self._block + 2 * MS_CTX_SIZE + 24)
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=p[])

    def userdata_ptr(mut self) -> BytePtr:
        var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=self._block + 2 * MS_CTX_SIZE + 32)
        return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=p[])

    # -- switching ---------------------------------------------------------

    # Driver side: save the current (driver) registers into the caller slot
    # and resume this fiber.  On the FIRST call this also prepares the fresh
    # context (ms_ctx_make) bound to the entry callback; afterwards it resumes
    # the fiber at its exact suspension point.
    def resume(mut self):
        if not self._prepared:
            self._prepared = True
            var fp = self.frame_ptr()
            var fr = fp.bitcast[FiberFrame]()
            fr[].self_ctx = self.fiber_ctx()
            fr[].caller_ctx = self.caller_ctx()
            fr[].user = self.userdata_ptr()
            ms_ctx_make(self.fiber_ctx(), self.stack_top(), self.entry_ptr(), fp)
        ms_ctx_switch(self.caller_ctx(), self.fiber_ctx())

    # Fiber side: save this fiber's registers and resume the driver.  Meant to
    # be invoked from in-fiber code that holds a pointer to the Fiber (a
    # scheduler drives resume()/suspend() symmetry; the entry thunk may also
    # yield directly through its FiberFrame self_ctx/caller_ctx pointers).
    def suspend(mut self):
        ms_ctx_switch(self.fiber_ctx(), self.caller_ctx())

    # -- teardown ----------------------------------------------------------

    # Idempotent: releases the synthetic stack (guard page included) and the
    # heap block.  Single-owner semantics: exactly ONE destroy() per live
    # Fiber, on the OWNING handle.  Fiber is ImplicitlyCopyable (all-scalar
    # handles), so destroying any COPY of the same handle double-frees — no
    # __del__ protects aliases implicitly; the driver owns the lifecycle.
    def destroy(mut self):
        if self._stack != 0:
            ms_stack_free(self.stack_base())
            self._stack = 0
            self._top = 0
            self._prepared = False
        if self._block != 0:
            _c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=self._block))
            self._block = 0


# Module-level factory (b2 has no static methods).
#
# NOTE — b2-JIT bug (see the header note): DO NOT CALL this from a test/suite
# driver under `mojo run` right now; the JIT crashes at codegen on
# imported-module factory CALLS.  Call sites must construct the Fiber inline
# (Fiber(stack, top, block) + set_targets) until the toolchain is upgraded —
# re-test on upgrade.  The factory remains for API completeness (A0.6).
#
# CONDITIONAL-GO ITEM for SPIKE_REPORT (A0.4): the b2 codegen bug above is
# NOT scoped to the factory -- ANY extern call lowered inside this module
# (resume()'s ms_ctx_make/ms_ctx_switch, suspend(), destroy()'s free)
# miscompiles under the current toolchain, JIT and AOT alike.  Until an
# upgrade fixes modular/modular#6971:
#   - resume/suspend/destroy MUST NOT be invoked through this module;
#     consumers inline their bodies (see tests/t4_fiber.mojo, which passes
#     3/3 using raw vendored calls).
#   - tests/t4b_fiber_module_aot.mojo covers the extern-free wiring and
#     lifecycle surface (create/accessors/alive/destroy-state flips).#
# Allocates a guarded synthetic stack of `stack_bytes` (page-rounded + guard)
# and the out-of-line block, wires entry + userdata into the scratch, then
# fills `dst`.  Raises when an allocation fails (releasing what it acquired).
def create(stack: Int, top: Int, entry: BytePtr, userdata: BytePtr,
           dst: UnsafePointer[Fiber, MutAnyOrigin]) raises:
    """Bind an ALREADY-ALLOCATED synthetic stack into a new Fiber at dst.

    b2-codegen note (modular/modular#6971 family): extern calls inside an
    imported-module factory lower incorrectly (AOT and JIT).  Stack
    allocation therefore lives with the CALLER (call ms_stack_alloc
    directly in your module -- direct driver-side calls are proven safe);
    this factory only does pointer arithmetic + scratch wiring.

    Raises when the heap block for ctx save areas cannot be allocated.
    """
    var block = Int(_c_malloc(2 * MS_CTX_SIZE + Fiber.TAIL_BYTES))
    if block == 0:
        raise Error("fiber.create: heap allocation failed")

    dst[0] = Fiber(stack, top, block)
    dst[].set_targets(Int(entry), Int(userdata))