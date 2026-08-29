# mojito_async/test/stress/t58_stack_registry_aot.mojo
#
# RED driver for issue #145 — the vendored stack registry is unsynchronised,
# and its liveness probe has an ABA hole.
#
# `mojito_async/vendor/mojito-sys/native_stack.c:32-33`:
#
#     static ms_reservation *g_resv;  static size_t g_resv_len, g_resv_cap;
#
# Grep that file for locks or atomics: zero.  `ms_stack_alloc` linear-scans
# this global for a free slot, `realloc`s it on growth and bumps `g_resv_len`,
# while `ms_stack_free`, `ms_stack_is_live`, `ms_live_stack_count` and
# `ms_stack_total_size` linear-scan the same storage.
#
# `fiber/stack_pool.mojo` is explicitly a PER-WORKER cache, so acquire(),
# release() and retire() all run on N worker threads concurrently.  Worker
# A's `realloc` against worker B's mid-scan is a heap use-after-free; two
# threads picking the same free slot record two reservations in one slot, so
# one mapping is orphaned — a later free munmaps the wrong total or leaks it
# outright.
#
# The canonical `mojito-sys/native/posix/mjs_stack.c:54-66` says outright
# "S1 is single-thread... deliberately unsynchronized".  That was true when
# it was written and stopped being true when S2 shipped threads.
#
# SCENARIO 1 — the registry, without needing a sanitizer.  Two threads each
# allocate N reservations and record every base.  The registry's own
# bookkeeping then has to agree with what the threads hold:
#
#   - ms_live_stack_count() == 2N
#   - ms_stack_is_live(base) == 1 for EVERY base handed out
#   - ms_stack_total_size() == 2N * bytes_per_reservation
#   - after freeing all of them, ms_live_stack_count() == 0
#
# A slot claimed twice loses a registration, so the count under-reports, a
# live base reports not-live, and the mapping it names can never be freed.
# That is a leak with an exact number attached, not a sanitizer diagnostic.
#
# SCENARIO 2 — the ABA hole the pool arbitrates ownership with.
# `fiber/fiber.mojo:419-432` munmaps a reservation directly even when the
# stack is a pool cell, and `fiber/stack_pool.mojo:22-29,236,292` then
# consults `ms_stack_is_live(base)` to detect "fiber destroyed before
# release".  After base B is munmapped, any later mmap can return B, at which
# point the registry reports B live again — so a STALE cell and a LIVE one
# are indistinguishable to the only probe the pool has.  Two fibers, one
# stack.
#
# SCENARIO 3 — the out-slot origin.  `vendor/mojito_sys.mojo:48` declares
#
#     comptime OutSlots = UnsafePointer[BytePtr, MutUntrackedOrigin]
#
# and `mojito-sys/mojito_sys/memory/stack.mojo:56` documents exactly this
# shape as miscompiling: "ORIGIN HAZARD (PR #39): MutUntrackedOrigin
# out-slots on OPAQUE extern calls get their post-call loads hoisted ABOVE
# the call under optimization".  This driver reads those slots straight after
# the call and checks base/top against the geometry C promised.
#
# BUILD LEVEL: `-O 0`, and NOT by choice.  Scenario 3 only means anything at
# the default optimization level, since the hazard is an optimizer decision,
# and nothing in this driver spins on a plain cross-thread cell — so it should
# have been a default-`-O` driver.  It cannot be: `mojo build` at default `-O`
# CRASHES on it (a compiler stack dump, not a diagnostic), which is the
# upstream optimizer crash EPIC #140 excludes.  Scenario 3 was therefore also
# probed from a minimal standalone driver that DOES build at default `-O`,
# and 256/256 out-slot reads matched the geometry C returned.  The origin is
# still the one mojito-sys documents as miscompiling; it did not fire here,
# and that is recorded rather than dressed up.
#
# Verdict: exit 0 + "PASS"; any failure prints RED and exits 1.
# AOT-only (pthread + ms_* externs; modular/modular#6971).
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr
from mojito_async.vendor.mojito_sys import (
    NativeStack,
    OutSlots,
    c_malloc,
    entry_pointer,
    ms_live_stack_count,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
    ms_stack_is_live,
    ms_stack_total_size,
)


@extern("pthread_create")
def _pthread_create(
    thread: UnsafePointer[Int, MutAnyOrigin],
    attr: Int,
    start_routine: UnsafePointer[Byte, MutAnyOrigin],
    arg: BytePtr,
) abi("C") -> Int32: ...


@extern("pthread_join")
def _pthread_join(thread: UInt, retval: UInt) abi("C") -> Int32: ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


# The racing phase runs in a FORKED CHILD.  It is not defensiveness: the
# vendored registry corrupts the process heap, so libmalloc aborts the
# process before any in-process assertion can be reached or printed.  The
# parent observes the child's death and reports it, which is how a
# crash-expected test stays a test instead of becoming a suite outage.
@extern("fork")
def _c_fork() abi("C") -> Int32: ...


@extern("waitpid")
def _c_waitpid(pid: Int32, status: UnsafePointer[Int32, MutAnyOrigin], options: Int32) abi("C") -> Int32: ...


comptime PER_THREAD = Int(400)
comptime N_THREADS = Int(2)
comptime N_TOTAL = PER_THREAD * N_THREADS
comptime ABA_TRIES = Int(4096)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var bases: UnsafePointer[Int, MutAnyOrigin]   # N_TOTAL slots
    var tops: UnsafePointer[Int, MutAnyOrigin]    # N_TOTAL slots
    var c: UnsafePointer[Int, MutAnyOrigin]       # counters
    var stack_bytes: Int

    def __init__(out self):
        self.bases = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.tops = self.bases
        self.c = self.bases
        self.stack_bytes = 0


def _alloc_one(bytes: Int) raises -> NativeStack:
    """One reservation through the production seam, reading the out-slots
    immediately after the call — the exact shape scenario 3 is about."""
    var slots = stack_allocation[2, BytePtr]()
    # Address 1 is this tree's "unwritten" sentinel (UnsafePointer is
    # non-nullable in b2), so a slot the callee never wrote is detectable.
    slots[0] = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
    slots[1] = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)
    var base_slot = OutSlots(unsafe_from_address=Int(slots))
    var top_slot = OutSlots(unsafe_from_address=Int(slots) + 8)
    var rc = ms_stack_alloc(bytes, base_slot, top_slot)
    if Int(rc) != 0:
        raise Error("ms_stack_alloc failed rc=" + String(Int(rc)))
    return NativeStack(slots[0], slots[1])


def _serve(scp: UnsafePointer[Scene, MutAnyOrigin], lane: Int) raises:
    var sc = scp[]
    for k in range(PER_THREAD):
        var st = _alloc_one(sc.stack_bytes)
        var idx = lane * PER_THREAD + k
        sc.bases[idx] = Int(st.base)
        sc.tops[idx] = Int(st.top)


@export("t58_lane0")
def t58_lane0(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        _serve(sc, 0)
    except e:
        sc[].c[0] = 1


@export("t58_lane1")
def t58_lane1(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        _serve(sc, 1)
    except e:
        sc[].c[1] = 1


def scenario_registry(mut failures: List[String]) raises:
    var ps = Int(ms_page_size())
    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(256))
    )
    sc[0] = Scene()
    sc[].bases = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_TOTAL * 8))
    )
    sc[].tops = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_TOTAL * 8))
    )
    sc[].c = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(16 * 8))
    )
    for i in range(N_TOTAL):
        sc[].bases[i] = 0
        sc[].tops[i] = 0
    for i in range(16):
        sc[].c[i] = 0
    sc[].stack_bytes = ps          # one usable page + one guard page

    var live_before = ms_live_stack_count()
    var size_before = ms_stack_total_size()

    var t0: Int = 0
    var t1: Int = 0
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t0), 0,
        entry_pointer["t58_lane0"](), sc.bitcast[Byte](),
    )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=t1), 0,
        entry_pointer["t58_lane1"](), sc.bitcast[Byte](),
    )
    _ = _pthread_join(UInt(t0), 0)
    _ = _pthread_join(UInt(t1), 0)

    if sc[].c[0] != 0 or sc[].c[1] != 0:
        failures.append("registry: an allocating thread raised")
        return

    # --- what the threads hold vs what the registry believes --------------
    var handed_out = 0
    var not_live = 0
    var dup = 0
    for i in range(N_TOTAL):
        if sc[].bases[i] == 0:
            continue
        handed_out += 1
        if Int(ms_stack_is_live(
                UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=sc[].bases[i]))) != 1:
            not_live += 1
    # duplicate bases would mean two threads were handed the same mapping
    for i in range(N_TOTAL):
        if sc[].bases[i] == 0:
            continue
        for j in range(i + 1, N_TOTAL):
            if sc[].bases[j] == sc[].bases[i]:
                dup += 1

    var live_after = ms_live_stack_count()
    var size_after = ms_stack_total_size()
    var expect_live = live_before + handed_out
    var bytes_each = 2 * ps

    print("  registry: handed_out=" + String(handed_out) + "/" + String(N_TOTAL)
          + " live_count=" + String(live_before) + "->" + String(live_after)
          + " (expected " + String(expect_live) + ")"
          + " total_size delta=" + String(size_after - size_before)
          + " (expected " + String(handed_out * bytes_each) + ")"
          + " not_live=" + String(not_live) + " duplicate_bases=" + String(dup))

    if live_after != expect_live:
        failures.append(
            "REGISTRY LOST REGISTRATIONS — ms_live_stack_count() is "
            + String(live_after) + ", expected " + String(expect_live)
            + " after " + String(handed_out) + " successful allocations across"
            + " " + String(N_THREADS) + " threads. native_stack.c has zero"
            + " locks and zero atomics around g_resv/g_resv_len: two threads"
            + " that pick the same free slot record two reservations in one,"
            + " and the mapping that loses can never be freed."
        )
    if not_live != 0:
        failures.append(
            "LIVE BASE REPORTS NOT-LIVE — " + String(not_live)
            + " base(s) handed out by ms_stack_alloc are not in the registry."
            + " ms_stack_is_live is the ONLY ownership probe fiber/stack_pool"
            + " has (stack_pool.mojo:22-29,236,292)."
        )
    if dup != 0:
        failures.append(
            "SAME MAPPING HANDED OUT TWICE — " + String(dup)
            + " duplicate base(s) across threads."
        )
    if size_after - size_before != handed_out * bytes_each:
        failures.append(
            "TOTAL SIZE DISAGREES — ms_stack_total_size() moved by "
            + String(size_after - size_before) + ", expected "
            + String(handed_out * bytes_each)
        )

    # --- and the leak that survives freeing everything --------------------
    for i in range(N_TOTAL):
        if sc[].bases[i] != 0:
            ms_stack_free(
                UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=sc[].bases[i])
            )
    var live_end = ms_live_stack_count()
    if live_end != live_before:
        failures.append(
            "LEAKED RESERVATIONS — after freeing every base the threads were"
            + " handed, ms_live_stack_count() is " + String(live_end)
            + ", expected " + String(live_before)
            + ": those mappings are unreachable and will never be unmapped."
        )


def scenario_aba(mut failures: List[String]) raises:
    """Allocate, free, then allocate until the SAME base comes back. At that
    point a stale cell and a live one look identical to ms_stack_is_live,
    which is the only ownership evidence the pool consults."""
    var ps = Int(ms_page_size())
    var first = _alloc_one(ps)
    var stale_base = Int(first.base)
    var stale_top = Int(first.top)
    ms_stack_free(first.base)

    var reused = False
    var tries = 0
    var new_top = 0
    while tries < ABA_TRIES:
        var again = _alloc_one(ps)
        tries += 1
        if Int(again.base) == stale_base:
            reused = True
            new_top = Int(again.top)
            break
        ms_stack_free(again.base)

    if not reused:
        print("  aba: base was not reused within " + String(ABA_TRIES)
              + " allocations; inconclusive on this host, not a pass")
        failures.append(
            "aba: could not reproduce address reuse in " + String(ABA_TRIES)
            + " tries — the hole is still there, but this run did not"
            + " demonstrate it"
        )
        return

    var live = Int(ms_stack_is_live(
        UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=stale_base)))
    print("  aba: base reused after " + String(tries) + " allocation(s);"
          + " ms_stack_is_live(stale base)=" + String(live))
    if live == 1:
        failures.append(
            "ABA — a NativeStack destroyed and munmapped at base "
            + String(stale_base) + " (top " + String(stale_top)
            + ") now reports LIVE again, because a later mmap returned the"
            + " same address for a DIFFERENT reservation (top "
            + String(new_top) + "). fiber/stack_pool.mojo uses exactly this"
            + " probe to decide whether a cell it is handed is one it still"
            + " owns, so it would accept the stale cell and hand out a"
            + " NativeStack aliasing someone else's reservation: two fibers,"
            + " one stack."
        )
    ms_stack_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=stale_base))


def scenario_out_slot_origin(mut failures: List[String]) raises:
    """The MutUntrackedOrigin out-slots, read immediately after the opaque
    extern call, at whatever optimization level this driver was built with.
    stack_pool.mojo:240-245,258-261 reads them in exactly this position to
    build the NativeStack a fiber will run on; if the loads are hoisted
    above the call, base/top are stale garbage and the first switch lands on
    a wild sp."""
    var ps = Int(ms_page_size())
    var bad = 0
    for _i in range(64):
        var st = _alloc_one(ps)
        if Int(st.base) <= 1 or Int(st.top) <= 1:
            bad += 1
        elif Int(st.top) <= Int(st.base):
            bad += 1
        elif Int(st.top) - Int(st.base) != 2 * ps:
            bad += 1
        ms_stack_free(st.base)
    if bad != 0:
        failures.append(
            "OUT-SLOT ORIGIN — " + String(bad) + " of 64 reservations came"
            + " back with base/top that do not match the geometry C promised."
            + " OutSlots is MutUntrackedOrigin (vendor/mojito_sys.mojo:48),"
            + " the origin mojito-sys/mojito_sys/memory/stack.mojo:56"
            + " documents as getting its post-call loads hoisted ABOVE the"
            + " call under optimization."
        )
    else:
        print("  out-slot origin: 64/64 reservations returned the geometry C"
              + " promised at this build's optimization level — the"
              + " MutUntrackedOrigin hazard did NOT fire here")


def _run_registry_child() raises -> Int:
    """Fork the racing phase.  Returns 0 when the child completed cleanly,
    and reports what killed it otherwise."""
    var pid = _c_fork()
    if Int(pid) < 0:
        print("  registry: fork() failed; cannot isolate the racing phase")
        return 2
    if Int(pid) == 0:
        var child_failures = List[String]()
        try:
            scenario_registry(child_failures)
        except e:
            child_failures.append("registry scenario raised: " + String(e))
        if len(child_failures) != 0:
            for m in child_failures:
                print("  - " + m)
            _iso_exit(1)
        _iso_exit(0)
    var status: Int32 = 0
    _ = _c_waitpid(pid, UnsafePointer[Int32, MutAnyOrigin](to=status), 0)
    var st = Int(status)
    var sig = st & 0x7F
    var code = (st >> 8) & 0xFF
    if sig != 0:
        print("  registry: the child died on signal " + String(sig)
              + " inside the allocator (see the libmalloc report above)")
        return 3
    if code != 0:
        return 1
    return 0


def main() raises:
    var failures = List[String]()
    print("T58 stack registry (issue #145)")
    var rc = _run_registry_child()
    if rc == 3:
        failures.append(
            "REGISTRY HEAP CORRUPTION — two threads allocating through"
            + " ms_stack_alloc killed the process inside realloc(3)."
            + " native_stack.c:32-33 keeps g_resv/g_resv_len/g_resv_cap as"
            + " plain statics with zero locks and zero atomics, and"
            + " ms_stack_alloc reallocs that global while other threads"
            + " linear-scan it. fiber/stack_pool.mojo is a PER-WORKER cache,"
            + " so acquire/release/retire run on N worker threads"
            + " concurrently and every task body, park and resume runs on"
            + " memory from this path."
        )
    elif rc == 1:
        failures.append(
            "registry: the child completed but its bookkeeping assertions"
            + " failed (see the child's own output above)"
        )
    elif rc == 2:
        failures.append("registry: could not fork; scenario not run")
    try:
        scenario_aba(failures)
    except e:
        failures.append("aba scenario raised: " + String(e))
    try:
        scenario_out_slot_origin(failures)
    except e:
        failures.append("out-slot scenario raised: " + String(e))

    if len(failures) == 0:
        print("T58 stack registry: PASS")
    else:
        print("T58 stack registry: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
