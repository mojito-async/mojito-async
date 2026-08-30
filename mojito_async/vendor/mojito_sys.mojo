# mojito_async/vendor/mojito_sys.mojo
#
# A1.1 (issue #49) — productionized Mojo bindings for the frozen mojito-sys
# S0 substrate (ms_stack_alloc/free, ms_ctx_make/switch, ms_page_size) under
# mojito_async/vendor/mojito-sys/ (C/asm vendored verbatim from
# github.com/mojito-async/mojito-sys @ 0ad48315bebc1fcb02834074afbff6e59173cdb1,
# see VENDORED_AT.txt).  This module is the batch-wide SINGLE home of the
# mojito-sys type surface + extern definitions:
#
#   - `NativeStack` (base/top) is the batch contract for the stack-pool
#     (issue #52) and fiber transport (issue #49): stack_pool acquires and
#     releases stacks as UnsafePointer[NativeStack, MutAnyOrigin]; Fiber
#     binds an acquired stack.  Do NOT redefine NativeStack elsewhere.
#   - every `ms_*` extern lives at CONCRETE module scope here (the b2
#     extern discipline, modular/modular#6971: externs stay out of generic
#     struct methods -- fiber.mojo's switch methods are extern-free and the
#     actual ms_ctx_* calls are lowered at the concrete consumer modules).
#   - `integration/sys.mojo` (`BytePtr`/`IntResult`) is deliberately kept
#     as the A1.1 extern-free adapter and is NOT touched by this module.
#
# b2 (1.0.0b2, 2cf4d08a) formulation, kept verbatim from the spike binding
# (spike/colorless_runtime/vendor/mojito-sys/mojito_spike.mojo, S0-proven):
#   - `def` only; extern symbols declared with `@extern("<c_symbol>")` plus
#     an explicit `abi("C")` effect and a `...` body; the library is chosen
#     at link time via `mojo run/build -Xlinker <repo>/libmojito_spike.dylib`.
#   - UnsafePointer carries concrete mutability/origin parameters inside
#     extern signatures: pointers handed to / received from C are
#     `MutAnyOrigin`; Mojo-side stack scratch is `MutUntrackedOrigin`.
#
# ENTRY-CALLBACK MECHANISM (S0-proven, kept verbatim): the C trampoline
# calls entry(userdata) with AAPCS64 semantics.  A bare Mojo `def` value
# cannot be converted to a raw code pointer, so the working mechanism is:
#   1. declare the callback `abi("C")` so its lowering IS the C ABI,
#   2. give it a stable link name with `@export("<name>")` (Mach-O prefixes
#      an underscore, so asm references `_<name>`),
#   3. materialize its address with `entry_pointer["<name>"]()` (an
#      adrp/add pair via std.sys.intrinsics.inlined_assembly) and pass it to
#      ms_ctx_make.

from std.sys.intrinsics import inlined_assembly

# Wild pointer used when passing raw byte payloads / userdata channels
# (same shape as mojito_async.integration.sys.BytePtr).
comptime BytePtr = UnsafePointer[Byte, MutAnyOrigin]

# Pointer to caller-provided scratch holding one BytePtr each; this is what
# ms_stack_alloc writes *out_base / *out_top through.
comptime OutSlots = UnsafePointer[BytePtr, MutAnyOrigin]

# sizeof(ms_ctx_t) per the v3 header (include/mojito_spike.h, issue #101
# A2.0 M:N rework): regs[12]=x19..x30 @0, fps[8]=d8..d15 @96, sp @160,
# return_to @168 => 176 bytes.  (The O(1) in-ctx return link replaces the
# removed process-global resume table; the fiber heap block grows via the
# 2*MS_CTX_SIZE + TAIL layout below.)
comptime MS_CTX_SIZE = 176


# Code address of an @export'd abi("C") Mojo callback as a C function
# pointer (ms_entry_fn).  `symbol_name` is the @export name WITHOUT the
# Mach-O underscore prefix.
def entry_pointer[symbol_name: String]() -> BytePtr:
    comptime asm_str = (
        "adrp ${0:x}, _" + symbol_name + "@PAGE\n"
        "add ${0:x}, ${0:x}, _" + symbol_name + "@PAGEOFF\n"
    )
    var addr = inlined_assembly[asm_str, UInt, constraints="=r"]()
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(addr))


# ---------------------------------------------------------------------------
# System page size (bytes).  NativeStack reservations are page-rounded +
# guard, so drivers compute stack sizes from this.
# ---------------------------------------------------------------------------

@extern("ms_page_size")
def ms_page_size() abi("C") -> Int32:
    ...


# ---------------------------------------------------------------------------
# NativeStack — one ms_stack_alloc reservation: `base` (mmap base; guard at
# [base, base+ps)), `top` (initial SP = highest usable address, 16-aligned).
# Batch-contract type: owned by the stack pool (issue #52) / fiber binding
# (issue #49); ms_stack_free is the only release path.
#
# NOTE (b2 extern discipline): the release helper lives here at concrete
# module scope -- NOT inside a generic struct method -- so stack_pool.mojo
# and fiber.mojo can call it without lowering externs from their own
# modules.
# ---------------------------------------------------------------------------

struct NativeStack(ImplicitlyCopyable, ImplicitlyDeletable):
    var base: BytePtr
    var top: BytePtr

    def __init__(out self):
        self.base = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0)
        self.top = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0)

    def __init__(out self, base: BytePtr, top: BytePtr):
        self.base = base
        self.top = top

    def alive(self) -> Bool:
        return Int(self.base) != 0

    # stack_bytes of the reservation (usable region; guard page excluded).
    def stack_bytes(self) -> Int:
        return Int(self.top) - Int(self.base)


def stack_free(stack: NativeStack):
    """Release one ms_stack_alloc reservation (idempotent when base == 0)."""
    ms_stack_free(stack.base)


@extern("ms_stack_alloc")
def ms_stack_alloc(
    bytes: Int,
    out_base: OutSlots,
    out_top: OutSlots,
) abi("C") -> Int32:
    ...


@extern("ms_stack_free")
def ms_stack_free(base: BytePtr) abi("C"):
    ...


# Number of currently-live ms_stack_alloc reservations (process-wide).  Used
# by the fiber factories' oversubscription guard (T1): the vendored resume
# table caps the process at 32 concurrent fibers (64 rows / 2 per fiber) and
# traps with `brk #0x67` (SIGILL) on saturation, so A1 raises a catchable
# Error before the 33rd live fiber rather than trapping.  EPIC #2 (#101)
# removes the cap; this surface is the fail-loud seam until then.
@extern("ms_live_stack_count")
def ms_live_stack_count() abi("C") -> Int:
    ...


# Liveness probe for a stack reservation: 1 when `base` is a still-registered
# (not-yet-freed) reservation, 0 when it was never allocated or was already
# ms_stack_free'd (munmap'd).  Used by the stack pool (issue #52) to refuse
# release()/warm-acquire of a reservation a Fiber.destroy already freed.
@extern("ms_stack_is_live")
def ms_stack_is_live(base: BytePtr) abi("C") -> Int32:
    ...


@extern("ms_stack_total_size")
def ms_stack_total_size() abi("C") -> Int:
    ...


# ctx: 176-byte ms_ctx_t write target; stack_top: initial sp (16-aligned);
# entry: ms_entry_fn code pointer (see entry_pointer above);
# userdata: passed through unmodified to entry(userdata).
@extern("ms_ctx_make")
def ms_ctx_make(
    ctx: BytePtr,
    stack_top: BytePtr,
    entry: BytePtr,
    userdata: BytePtr,
) abi("C"):
    ...


# Saves current callee-saved state (x19-x30, d8-d15, sp) into *from_;
# resumes *to.  Also records return_to := from_ for the trampoline.
@extern("ms_ctx_switch")
def ms_ctx_switch(from_: BytePtr, to: BytePtr) abi("C"):
    ...

# ---------------------------------------------------------------------------
# Out-of-line heap block backing (fiber ctx save areas + sidecar; issue #49).
# The Fiber struct stays all-scalar; its two 176-byte ms_ctx_t slots, the
# FiberFrame sidecar and the entry/userdata scratch live in ONE malloc'd
# block -- so the struct is trivially copy-safe, and destroy() frees one
# block.  libc is not mojito-sys, but the C-ABI firewall is the single touch
# of every extern the runtime needs, so the allocator pair lives here too
# (never in fiber.mojo).
# ---------------------------------------------------------------------------

@extern("malloc")
def c_malloc(size: Int) abi("C") -> BytePtr:
    ...

@extern("free")
def c_free(ptr: BytePtr) abi("C"):
    ...


# ---------------------------------------------------------------------------
# A2.1 (issue #67) — worker-pool substrate: logical CPU count, NativeThread,
# NativeTlsKey.  The frozen mojito-sys S0 substrate exports NO thread-creation
# primitive, so the documented S2.2 recipe applies: an @export'd abi("C")
# entry (the pool trampoline, runtime/thread_entry.mojo) + the adrp/add
# code-address recipe (entry_pointer above, AOT-proven in *_aot drivers) and
# the pthread_* externs BELOW at concrete module scope (libc; per the A1
# notes libc symbols resolve under both JIT and AOT, the ms_* dylib symbols
# are AOT-only).  The producers/consumers of NativeThread must be core
# scheduler threads (spec phase A2 "N mojito-sys.NativeThread workers").
#
# NULL-CAPABLE C PARAMETERS: typed UInt (zero passes through as the null
# pointer ABI-wise) — Mojo pointers are non-nullable in 1.0.0b2
# (std.memory.unsafe_pointer: "use Optional[UnsafePointer] to model
# nullability"), and pthread_create/pthread_join/pthread_key_create all take
# NULL in our usage (default attributes, no dtor, no exit value).
# ---------------------------------------------------------------------------

@extern("ms_cpu_logical_count")
def cpu_logical_count() abi("C") -> Int:
    ...


@extern("pthread_create")
def pthread_create(
    thread: UnsafePointer[UInt, MutAnyOrigin],
    attr: UInt,
    start_routine: BytePtr,
    arg: BytePtr,
) abi("C") -> Int32:
    ...


@extern("pthread_join")
def pthread_join(thread: UInt, retval: UInt) abi("C") -> Int32:
    ...


@extern("pthread_self")
def pthread_self() abi("C") -> UInt:
    ...


@extern("pthread_key_create")
def pthread_key_create(
    key: UnsafePointer[UInt, MutAnyOrigin], destructor: UInt
) abi("C") -> Int32:
    ...


@extern("pthread_key_delete")
def pthread_key_delete(key: UInt) abi("C") -> Int32:
    ...


@extern("pthread_getspecific")
def pthread_getspecific(key: UInt) abi("C") -> BytePtr:
    ...


@extern("pthread_setspecific")
def pthread_setspecific(key: UInt, value: BytePtr) abi("C") -> Int32:
    ...


# ---------------------------------------------------------------------------
# #112 (item 6): pthread_attr_t plumbing — RuntimeConfig.stack_reserve_bytes
# reaches pthread_create as a real reserved-stack size instead of being
# validated and then silently dropped (every worker thread spawned with
# pthread's DEFAULT attrs regardless of config).  PTHREAD_ATTR_BYTES is a
# generous, rounded scratch size (the same "round + headroom, not a brittle
# exact sizeof" convention as thread_entry.mojo's CELL_WORKER/CELL_ENTRY):
# Darwin's opaque pthread_attr_t is 64 bytes (8-byte __sig + 56-byte
# __opaque), glibc's is 56 bytes; 128 covers either with headroom.
# `stack_initial_commit_bytes` (RuntimeConfig's OTHER stack knob) has no
# portable pthread_attr_* equivalent (eager page-commit is an OS-specific
# mmap flag, not a pthread attribute) — it stays advisory-only, as
# config.mojo's own doc already frames it ("0 = commit on demand").
# ---------------------------------------------------------------------------
comptime PTHREAD_ATTR_BYTES = Int(128)


@extern("pthread_attr_init")
def pthread_attr_init(attr: BytePtr) abi("C") -> Int32:
    ...


@extern("pthread_attr_setstacksize")
def pthread_attr_setstacksize(attr: BytePtr, stacksize: UInt) abi("C") -> Int32:
    ...


@extern("pthread_attr_destroy")
def pthread_attr_destroy(attr: BytePtr) abi("C") -> Int32:
    ...


# ---------------------------------------------------------------------------
# NativeThread — one OS thread handle (pthread_t on LP64 Darwin; the A2.1
# worker pool's worker carrier).  Value type: holding a handle does not own
# the thread; join_native_thread is the only release path (like
# ms_stack_free for NativeStack).
# ---------------------------------------------------------------------------

struct NativeThread(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    var _tid: UInt

    def __init__(out self):
        self._tid = 0

    def tid(self) -> UInt:
        return self._tid

    def alive(self) -> Bool:
        return self._tid != 0


def make_native_thread() -> NativeThread:
    """Module-level factory (b2 has no static methods)."""
    return NativeThread()


def spawn_native_thread(
    entry: BytePtr, arg: BytePtr, stack_size: Int = 0
) raises -> NativeThread:
    """Create one OS thread running entry(arg).

    `entry` is the C-ABI code address of an @export'd abi("C") def obtained
    via entry_pointer[...] (the S2.2 adrp/add recipe).  Raises when
    pthread_create fails (thread limit / resource exhaustion); the caller
    stays the thread's creator and MUST eventually join it.

    `stack_size` (issue #112 item 6): 0 (default) keeps the ORIGINAL
    default-pthread-attrs behavior — every existing caller (bench,
    t35/t41's raw spawns, the pool's non-worker-pool test paths) is
    BYTE-FOR-BYTE unaffected.  A positive value builds a scratch
    pthread_attr_t (PTHREAD_ATTR_BYTES above), sets its stack size via
    pthread_attr_setstacksize, and passes that attr's ADDRESS through
    pthread_create's existing `attr: UInt` slot — the same NULL-CAPABLE
    raw-value convention this module's other pthread_* calls already use
    for a maybe-pointer C parameter (module header); the attr is
    destroyed (POSIX allows this immediately after pthread_create returns
    — the implementation copies what it needs) and freed before this
    function returns either way.  worker_pool.mojo's spawn_all_workers is
    the one production caller that passes RuntimeConfig.stack_reserve_bytes
    here (M6, issue #112 item 6: the config's validated stack knob used to
    be validated then silently dropped)."""
    var t = NativeThread()
    var tp = UnsafePointer[UInt, MutAnyOrigin](to=t._tid)
    if stack_size <= 0:
        var rc = pthread_create(tp, 0, entry, arg)
        if rc != 0:
            raise Error(String(rc))
        return t
    var attr = c_malloc(PTHREAD_ATTR_BYTES)
    var irc = pthread_attr_init(attr)
    if irc != 0:
        c_free(attr)
        raise Error(String(irc))
    var src = pthread_attr_setstacksize(attr, UInt(stack_size))
    if src != 0:
        _ = pthread_attr_destroy(attr)
        c_free(attr)
        raise Error(String(src))
    var rc = pthread_create(tp, UInt(Int(attr)), entry, arg)
    _ = pthread_attr_destroy(attr)
    c_free(attr)
    if rc != 0:
        # NOTE (b2 1.0.0b2 #compiler-crash, probed in this lane): mixing a
        # const String literal + a dynamic String(rc) in one concatenation
        # inside a raise path that also calls an extern crashed the compiler;
        # keep the message a SINGLE dynamic conversion.
        raise Error(String(rc))
    return t


def join_native_thread(t: NativeThread) raises:
    """Join one thread; its exit value is discarded (retval = NULL)."""
    if not t.alive():
        return
    var rc = pthread_join(t._tid, 0)
    if rc != 0:
        raise Error(String(rc))


# ---------------------------------------------------------------------------
# NativeTlsKey — one OS-worker-local storage slot (pthread_key_t; spec §69:
# current_worker / current_task / current_scope).  Native TLS is
# OS-worker-local, never task-local (spec §69 documents the migration
# caveat).  A2.1 writes current_worker at THREAD ENTRY only (coarse
# granularity — the C layer behind pthread reads holds one global registry
# mutex, per the S2.4 scalability note; no per-task get() hot paths this
# lane).  current_task / current_scope are reserved slots for the E-lanes.
# ---------------------------------------------------------------------------

struct NativeTlsKey(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    var _key: UInt

    def __init__(out self):
        self._key = 0

    def raw(self) -> UInt:
        return self._key


def make_tls_key() raises -> NativeTlsKey:
    """Create one pthread TLS key (no destructor; slots hold raw pointers to
    runtime-owned cells, freed by the pool lifecycle, never the OS)."""
    var k = NativeTlsKey()
    var kp = UnsafePointer[UInt, MutAnyOrigin](to=k._key)
    var rc = pthread_key_create(kp, 0)
    if rc != 0:
        raise Error(String(rc))
    return k



def delete_tls_key(key: NativeTlsKey) raises:
    """Release one pthread TLS key (the pool's finalize path, PR #104 M5
    fold).  After this returns the OS slot is gone: no thread may read or
    write the key (the pool only calls this after every worker joined, or
    at re-arm before new keys are created)."""
    var rc = pthread_key_delete(key._key)
    if rc != 0:
        raise Error(String(rc))


def tls_get(key: NativeTlsKey) -> BytePtr:
    """Read this OS thread's value for `key` (address 0 when unset — the
    null pointer; never dereferenced unguarded)."""
    return pthread_getspecific(key._key)


# ---------------------------------------------------------------------------
# A2.6 (issue #72) — NativeEvent: the S3.5 OS-worker sleep/wake primitive.
# The vendored S0 substrate exported NO event API, so this lane implements
# it in-repo (vendor/mojito-sys/ms_event.c, auto-picked-up by the root
# Makefile's source wildcards; byte-identical in the spike/prod trees).
#
# Semantics (S3.5, issue #72, spec §17/§21): AUTO-RESET + BREADTH-ONE (one
# signal wakes at most ONE parked waiter), at most ONE token pending, STICKY
# when nobody waits (a signal leaves a token a future waiter consumes
# immediately — closes the lost-wakeup race), COALESCING (N signals while a
# token is pending deliver one token), and CLOCK_MONOTONIC wait_until.  The
# predicate loop (re-check token on every condvar wake) lives INSIDE the C so
# wait_until returns ok ONLY on a consumed token — a spurious wake never
# surfaces as a fake ready (spec §17 win).
#
# Handle model: ms_event_create() returns an opaque Int handle (0 == OOM);
# the pool owns the backing and destroys it at finalize().  Mojo carries the
# handle as an Int (b2 pointers are non-nullable; 0 is the null sentinel).
# ---------------------------------------------------------------------------

@extern("ms_event_create")
def ms_event_create() abi("C") -> Int:
    ...


@extern("ms_event_destroy")
def ms_event_destroy(handle: Int) abi("C"):
    ...


@extern("ms_event_signal")
def ms_event_signal(handle: Int) abi("C"):
    ...


@extern("ms_event_wait")
def ms_event_wait(handle: Int) abi("C") -> Int:
    ...


@extern("ms_event_wait_until")
def ms_event_wait_until(handle: Int, deadline_ns: Int) abi("C") -> Int:
    ...


@extern("ms_monotonic_ns")
def ms_monotonic_ns() abi("C") -> Int:
    ...


# NativeEvent — one OS-worker sleep/wake event (the per-pool idle park
# target).  Value type: holding the handle does not own the backing; the
# pool destroys it (ms_event_destroy).  Deadline computations use
# ms_monotonic_ns().
struct NativeEvent(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    var _handle: Int

    def __init__(out self):
        self._handle = 0

    def handle(self) -> Int:
        return self._handle

    def alive(self) -> Bool:
        return self._handle != 0


def make_native_event() raises -> NativeEvent:
    """Create one NativeEvent (raises on allocation failure)."""
    var e = NativeEvent()
    e._handle = ms_event_create()
    if e._handle == 0:
        raise Error(String(1))  # single conversion (b2 extern-raise crash note)
    return e


def native_event_destroy(mut e: NativeEvent):
    """Destroy the event's backing (idempotent; no waiters may be blocked —
    call only after the pool joined every worker)."""
    ms_event_destroy(e._handle)
    e._handle = 0


def native_event_signal(e: NativeEvent):
    """Signal the event: set the sticky token + wake at most one waiter
    (breadth-one; coalesces while a token is pending)."""
    ms_event_signal(e._handle)


def native_event_wait_until(handle: Int, deadline_ns: Int) -> Bool:
    """Block until `deadline_ns` (absolute CLOCK_MONOTONIC); True iff a token
    was CONSUMED (the C predicate loop means a spurious wake never returns
    True).  False = timed out with no token (caller re-checks + re-parks).
    `handle` is the opaque ms_event handle (Int; 0 = null, returns False)."""
    return ms_event_wait_until(handle, deadline_ns) != 0


def monotonic_now_ns() -> Int:
    """Current CLOCK_MONOTONIC time in nanoseconds (deadline base)."""
    return ms_monotonic_ns()
