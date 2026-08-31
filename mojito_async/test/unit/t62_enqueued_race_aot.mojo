# mojito_async/test/unit/t62_enqueued_race_aot.mojo
#
# issue #203 — Runtime._enqueued: unguarded cross-thread += race in
# push_remote.
#
# TDD acceptance driver (RED first): `Runtime._enqueued` is a plain `Int`
# bumped with a bare `+=` from BOTH the owner thread's own paths
# (enqueue_local, called here by one dedicated "owner" thread) AND
# push_remote (explicitly documented "ANY worker may push" — unpark_current's
# cross-worker wake delivery target, called here by N_REMOTE_THREADS foreign
# threads).  RemoteReadyQueue.push (called two lines above the racy
# increment inside push_remote) is properly SpinLock-guarded; the counter
# bump right after it is not.  Classic lost-update: two concurrent
# non-atomic `+=`s on the same field can net a single increment instead of
# two.  This driver seeds real concurrent load from multiple real OS
# threads (pthreads, not fibers) and asserts `enqueued()` equals the exact
# expected total — RED (undercounted) before the fix, GREEN after.
#
# Fix shape: rather than the issue's first-listed option (convert
# `_enqueued` to `Atomic[DType.int64]` directly on the `Runtime` struct),
# this repo's own history already proved that path unsound — issue #69's
# original commit message records "an atomic-RMW-on-Runtime-field fix was
# verified to miscompile the A1 fiber drive (t26)", which is exactly why
# `InjectQueue` counts its own accepted records under its OWN SpinLock
# (`_accepted`, folded into `enqueued()`) instead of touching a `Runtime`
# field atomically.  The fix here mirrors that proven-safe shape: give
# `RemoteReadyQueue` the identical `_accepted` counter under its own guard
# (bumped inside `push()`, alongside the append it already brackets) and
# fold `_remote.accepted()` into `enqueued()` the same way `_inject.accepted()`
# already is.  `push_remote` itself stops touching any `Runtime` scalar.
#
# Every call in this driver passes a dummy (never-dereferenced) tcb_addr —
# push_remote/enqueue_local only build and enqueue a TaskRecord and bump a
# counter; neither reads through the address, so no real TaskControlBlock
# or scheduler machinery is needed to exercise the race.
#
# EXTERN DISCIPLINE (modular/modular#6971): real pthread threads, so this
# driver MUST be AOT (`mojo build` + execute; t33/t38/t60 pattern) — the b2
# JIT cannot resolve dylib symbols through an imported module.
#
# Verdict: exit 0 + "PASS"; a count mismatch prints RED and forces exit 1.
from std.atomic import Atomic, Ordering
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.runtime import Runtime
from mojito_async.vendor.mojito_sys import c_malloc, entry_pointer


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


comptime N_REMOTE_THREADS = Int(7)
comptime ITERS_PER_THREAD = Int(50000)
comptime N_TOTAL_THREADS = Int(N_REMOTE_THREADS + 1)  # + 1 owner thread
comptime EXPECTED_TOTAL = Int(N_TOTAL_THREADS * ITERS_PER_THREAD)


def _fail(failures: UnsafePointer[Int, MutAnyOrigin], msg: String):
    failures[0] = failures[0] + 1
    print("  - " + msg)


# ---------------------------------------------------------------------------
# Shared scene (heap-backed; threads read/write through pointers — t33/t38
# pattern, never a stack-carved escapee).
# ---------------------------------------------------------------------------

struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var rt: UnsafePointer[Runtime, MutAnyOrigin]
    # Start gate: every thread spins on this until main releases it, so the
    # N_TOTAL_THREADS producers hit the shared counter at (as close to)
    # the same time as real OS scheduling allows — maximizing contention.
    var go: UnsafePointer[Atomic[DType.int64], MutAnyOrigin]
    var failures: UnsafePointer[Int, MutAnyOrigin]
    var thread_err: UnsafePointer[Int, MutAnyOrigin]

    def __init__(out self):
        self.rt = UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=1)
        self.go = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](unsafe_from_address=1)
        self.failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.thread_err = self.failures


# ---------------------------------------------------------------------------
# Thread bodies
# ---------------------------------------------------------------------------

def _wait_for_go(scp: UnsafePointer[Scene, MutAnyOrigin]):
    while scp[].go[].load[ordering=Ordering.ACQUIRE]() == 0:
        pass


def serve_remote(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    _wait_for_go(scp)
    var i = 0
    while i < ITERS_PER_THREAD:
        # push_remote: THIS is the foreign-thread path (issue #203) — ANY
        # worker may push a wake into another worker's RemoteReadyQueue.
        scp[].rt[].push_remote(1, i)
        i += 1


def serve_owner(scp: UnsafePointer[Scene, MutAnyOrigin]) raises:
    _wait_for_go(scp)
    var i = 0
    while i < ITERS_PER_THREAD:
        # enqueue_local: the owner's own local-deque spawn path — races
        # against the N_REMOTE_THREADS push_remote callers on the exact
        # same `_enqueued` field pre-fix.
        scp[].rt[].enqueue_local(1, i)
        i += 1


@export("t62_remote_worker")
def t62_remote_worker(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_remote(sc)
    except e:
        sc[].thread_err[0] = 1


@export("t62_owner_worker")
def t62_owner_worker(arg: BytePtr) abi("C"):
    var sc = arg.bitcast[Scene]()
    try:
        serve_owner(sc)
    except e:
        sc[].thread_err[0] = 1


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

def run_scenario(failures: UnsafePointer[Int, MutAnyOrigin]) raises:
    var rt = Runtime()
    var rtp = UnsafePointer[Runtime, MutAnyOrigin](to=rt)

    var cells = Int(c_malloc(4 * 8))
    var p = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    for i in range(4):
        p[i] = 0

    var go_cell = Int(c_malloc(8))
    var gop = UnsafePointer[Atomic[DType.int64], MutAnyOrigin](unsafe_from_address=go_cell)
    gop[0] = Atomic[DType.int64](0)

    var sc = UnsafePointer[Scene, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(2048))
    )
    sc[0] = Scene()
    sc[].rt = rtp
    sc[].go = gop
    sc[].failures = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells)
    sc[].thread_err = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=cells + 1 * 8)

    var tids = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_TOTAL_THREADS * 8))
    )
    for i in range(N_TOTAL_THREADS):
        tids[i] = 0

    # Spawn N_REMOTE_THREADS foreign pushers + 1 owner enqueuer, ALL still
    # blocked on the start gate.
    for i in range(N_REMOTE_THREADS):
        _ = _pthread_create(
            UnsafePointer[Int, MutAnyOrigin](to=tids[i]), 0,
            entry_pointer["t62_remote_worker"](), sc.bitcast[Byte](),
        )
    _ = _pthread_create(
        UnsafePointer[Int, MutAnyOrigin](to=tids[N_REMOTE_THREADS]), 0,
        entry_pointer["t62_owner_worker"](), sc.bitcast[Byte](),
    )

    # Release the gate: every thread starts hammering its enqueue path at
    # (as close to) the same instant as real OS thread scheduling allows.
    gop[0].store[ordering=Ordering.RELEASE](Int64(1))

    for i in range(N_TOTAL_THREADS):
        _ = _pthread_join(UInt(tids[i]), 0)

    if sc[].thread_err[0] != 0:
        _fail(failures, "a worker thread raised (see prints above)")

    var got = rtp[].enqueued()
    if got != EXPECTED_TOTAL:
        _fail(failures, "enqueued() = " + String(got) + ", expected "
              + String(EXPECTED_TOTAL) + " (lost update on Runtime._enqueued "
              + "under concurrent push_remote/enqueue_local — issue #203)")
    print("T62 enqueued race: enqueued()=" + String(got)
          + " expected=" + String(EXPECTED_TOTAL))


def main() raises:
    var failures = 0
    var fp = UnsafePointer[Int, MutAnyOrigin](to=failures)
    try:
        run_scenario(fp)
    except e:
        print("T62 enqueued race: RED (exception " + String(e) + ")")
        _iso_exit(1)
    if fp[] == 0:
        print("T62 enqueued race: PASS")
    else:
        print("T62 enqueued race: RED (" + String(fp[]) + " failure(s))")
        _iso_exit(1)
