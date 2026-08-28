# mojito_async/runtime/thread_entry.mojo
#
# A2.1 (issue #67) — the worker-thread entry: the C-ABI trampoline body
# (mjs_pool_entry_main), the per-worker entry-cell the trampoline reads, and
# the worker-loop seam (E2-OWNED; the queue lane, issue #68, fills the loop
# body; A2.6/E6, issue #72, fills the IDLE sleep/wake path).  This is the A2
# production home of the S2.2 thread-entry recipe (an @export'd abi("C")
# entry + the adrp/add code address, proven under mojito-sys and in the
# *_aot drivers).
#
# EMBEDDING RULE (b2 AOT linkage — probe-proven, and the same pattern the A1
# fiber lanes use where drivers export their own t28_entry/t24_entry bodies):
# the process that spawns pool threads must declare the trampoline SYMBOL in
# its own binary and forward to this shared body:
#
#     # in the embedding driver/main module:
#     @export("mjs_pool_entry")
#     def mjs_pool_entry(ud: BytePtr) abi("C"):
#         thread_entry.mjs_pool_entry_main(ud)
#
#     pool.start(entry_pointer["mjs_pool_entry"]())
#
# The @export must be driver-local because `mojo build` does not link @export
# symbols out of imported modules (modular/modular#6971 family; the adrp/add
# recipe needs the symbol present in the same binary).  The BODY is shared
# here so every embedder only writes the two-line forwarding export.
#
# ENTRY CHOREOGRAPHY:
#   1. TLS (spec §22/§69) is established AT THREAD ENTRY, once, at COARSE
#      granularity — current_worker receives the Worker cell address
#      (worker-stable for the thread's whole life); current_task /
#      current_scope are bound to the "cleared" sentinel (no task/scope is
#      current when a worker enters).  Per the S2.4 note the C TLS layer
#      holds one global registry mutex on reads, so NO per-task get() hot
#      paths exist; the single entry write + the acceptance read-back are the
#      whole TLS surface.  A2.6 formalizes the bind/clear via runtime/tls.mojo.
#   2. the worker runs the E2-OWNED seam (pool_worker_loop): this lane
#      drains the seeded seam units (direct current_worker observations), then
#      SLEEPS on the pool NativeEvent (A2.6/E6) instead of busy-spinning.
#   3. exit: the loop returns when it observes the latch; the trampoline
#      CLEARS all three TLS slots (spec §22/§69) and flags exited so
#      join_all() can assert every worker saw the shutdown.
#
# The entry cell lives in pool-owned heap (worker_pool.mojo) with a STABLE
# address for the worker's whole life; the worker's Runtime/TCB cells are
# never touched cross-thread by this lane (per-worker Runtime only).
#
# Externs: none HERE — pthread spawn/join/TLS + NativeEvent live in
# vendor/mojito_sys.mojo at concrete module scope; this module composes them.
#
# E6 b2 workaround (documented at worker_idle_park): the worker-side idle park
# body lives at MODULE scope HERE (the abi("C") trampoline's own module) and
# calls the vendor NativeEvent wrappers directly.  Deeper struct-method /
# cross-module extern chains from an abi("C") def mis-lower on 1.0.0b2
# ("Call parameter type does not match function signature!" — a UInt arg
# degrades to pass-by-ref through the layers); keeping the extern calls at
# concrete module scope in the trampoline module sidesteps it.  The shared
# accounting (idle.mojo) uses identical ACCT_* offsets, so producer and worker
# agree on the same block.
from std.atomic import Atomic, Ordering
from std.time import sleep
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.worker import Worker
from mojito_async.vendor.mojito_sys import (
    NativeTlsKey,
    monotonic_now_ns,
    native_event_wait_until,
    pthread_getspecific,
    pthread_setspecific,
    spawn_native_thread,
    tls_get,
)


# Worker/entry cell byte-strides for the pool's heap arrays (b2 exposes no
# public sizeof; >= real struct sizes is the documented t28-style contract).
# CELL_WORKER must cover the post-#68/#69/#70 Worker: its Runtime embeds the
# @align(128) LocalDeque + RemoteReadyQueue (cache-line isolation, issue #68
# step 5) plus the bounded InjectQueue, so the real struct is far larger
# than the A2.1 five-field Worker — 512 bytes overflowed and corrupted the
# heap (tcmalloc aborted freeing a bogus pointer in t30).  4096 keeps the
# headroom for the E-lanes' further field growth.
comptime CELL_WORKER = Int(4096)
comptime CELL_ENTRY = Int(256)


# Generous (2 s) so idle workers genuinely SLEEP for long stretches; the
# wake budget + shutdown both signal the event explicitly, so latency stays
# sub-millisecond regardless.
comptime IDLE_PARK_SLICE_NS = Int(2_000_000_000)  # 2 s backstop


# The pool-owned idle accounting block layout (SHARED with runtime/idle.mojo —
# must match idle.mojo's ACCT_* comptime offsets exactly).
comptime IDLE_ACCT_IDLE = Int(0)
comptime IDLE_ACCT_PENDING = Int(8)
comptime IDLE_ACCT_PARK = Int(16)
comptime IDLE_ACCT_WAKE = Int(24)
comptime IDLE_ACCT_SPUR = Int(32)


struct WorkerEntryCell(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Per-worker spawn payload, read/written by the worker thread and the
    pool owner at different lifecycle points (owner stores before spawn;
    worker mutates while running; owner observes after join — pthread_join
    provides the happens-before edge, so the observation fields need no
    atomics).

    worker_id   — this worker's id (seed units record it into obs; the
                  observation slice is the A2.1 acceptance surface).
    worker      — address of this worker's Worker cell (the current_worker
                  TLS VALUE; spec §69).
    cw/ct/cs    — the pool's three NativeTlsKey slots (spec §22/§69):
                  current_worker (written at entry) + current_task +
                  current_scope (bound to the cleared sentinel at entry).
    latch       — the pool's shutdown latch (Atomic[uint8]: 0 running, 1
                  shutdown requested); polled by the idle loop.
    units_left  — seam units still to run for this worker (issue #68 swaps
                  this for the local runnable queue).
    obs/obs_done/obs_cap — the seam unit's observation slice (driver-owned
                  heap): each unit records its worker id at obs[obs_done].
    event       — the pool's NativeEvent handle (A2.6/E6 idle park target).
    acct        — the pool's idle-accounting block (A2.6/E6 lock-free
                  atomics: idle sleepers, announced work, spec §71 counters).
    idle_parks  — how many times THIS worker parked as a sleeper (A2.6/E6;
                  the bench's parked-not-spinning observability, post-join).
    entry_ok    — TLS read-back at ENTRY matched the Worker cell address.
    unit_ok     — every seam unit saw the stable current_worker value.
    loop_ok/exited — the seam ran cleanly / the worker observed the latch.
    """

    var worker_id: Int
    var worker: BytePtr
    var cw: NativeTlsKey
    var ct: NativeTlsKey
    var cs: NativeTlsKey
    var latch: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin]
    var units_left: Int
    var units_seeded: Int
    var obs: UnsafePointer[Int, MutAnyOrigin]
    var obs_done: Int
    var obs_cap: Int
    var entry_ok: Bool
    var unit_ok: Bool
    var loop_ok: Bool
    var exited: Bool
    var event: Int             # pool NativeEvent handle (E6 park target)
    var acct: BytePtr          # pool idle-accounting block (E6 atomics)
    var idle_parks: Int        # times this worker parked as a sleeper

    def __init__(out self):
        self.worker_id = 0
        self.worker = BytePtr(unsafe_from_address=1)
        self.cw = NativeTlsKey()
        self.ct = NativeTlsKey()
        self.cs = NativeTlsKey()
        self.latch = UnsafePointer[Scalar[DType.uint8], MutAnyOrigin](
            unsafe_from_address=1
        )
        self.units_left = 0
        self.units_seeded = 0
        self.obs = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.obs_done = 0
        self.obs_cap = 0
        self.entry_ok = False
        self.unit_ok = True
        self.loop_ok = False
        self.exited = False
        self.event = 0
        self.acct = BytePtr(unsafe_from_address=1)
        self.idle_parks = 0


# ---------------------------------------------------------------------------
# E2-OWNED SEAM (echo to the queue lane): the worker-loop BODY lives in
# pool_worker_loop() below.  A2.1 implements it as drain-seam-units then
# latch-poll idle.  Issue #68 (queue lane) REPLACES the unit source with:
#     pop this worker's LOCAL runnable queue -> scheduler_loop(rt, <lane
#     dispatcher>, ud) — the seam function signature stays exactly
#     `pool_worker_loop(cell: BytePtr) raises`; stealing (#70) reaches the
#     stealable set from the same loop; the NativeEvent idle path is E6.
# A2.6 (issue #72) fills the E6 idle path: after the units a worker SLEEPS on
# the pool's NativeEvent (worker_idle_park) instead of busy-spinning on the
# latch; the bounded deadline slice is the shutdown backstop.
# ---------------------------------------------------------------------------

def _idle_cell(acct: BytePtr, off: Int) -> UnsafePointer[Int64, MutAnyOrigin]:
    """Address of one int64 atomic in the pool's idle-accounting block."""
    return UnsafePointer[Int64, MutAnyOrigin](unsafe_from_address=Int(acct) + off)


def _idle_pending(acct: BytePtr) -> Int:
    return Int(Atomic[DType.int64].load[ordering=Ordering.SEQUENTIAL](_idle_cell(acct, IDLE_ACCT_PENDING)))


def worker_idle_park(acct: BytePtr, event: Int, deadline_ns: UInt) -> Bool:
    """The OS-level idle park of one worker (issue #72 step 2/step 5), at
    module scope in the abi("C") trampoline's module (see the E6 b2 workaround
    note at the top) so the vendor NativeEvent wrappers are called at concrete
    scope.  Identical semantics to idle.mojo's idle_park_worker, and it reads/
    writes the SAME acct block via the same offsets.

      1. commit this OS worker as a sleeper (+1 _idle_workers);
      2. RE-CHECK the announced-work count immediately before sleeping — the
         lost-wakeup guard (work landed between the last pop and here); if
         present, withdraw and return True (the caller does not sleep);
      3. park on the pool NativeEvent with an absolute CLOCK_MONOTONIC
         deadline slice (native_event_wait_until consumes a token; the C
         predicate loop means ok ONLY on a real token — no fake spurious
         ready, spec §17), then leave the sleeper set;
      4. on a consumed token classify it: productive when announced work
         remains, spurious otherwise (bump spurious_wake_total, spec §71).

    Returns True when the caller should NOT sleep (work found at the pre-park
    re-check); False after a real park (woken by token or the slice elapsed —
    the caller re-checks the latch and re-parks)."""
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _idle_cell(acct, IDLE_ACCT_IDLE), 1
    )
    if _idle_pending(acct) > 0:
        _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
            _idle_cell(acct, IDLE_ACCT_IDLE), -1
        )
        return True
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _idle_cell(acct, IDLE_ACCT_PARK), 1
    )
    var consumed = native_event_wait_until(event, deadline_ns)
    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
        _idle_cell(acct, IDLE_ACCT_IDLE), -1
    )
    if consumed:
        _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
            _idle_cell(acct, IDLE_ACCT_WAKE), 1
        )
        if _idle_pending(acct) == 0:
            _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
                _idle_cell(acct, IDLE_ACCT_SPUR), 1
            )
    return False


def pool_worker_loop(cell: BytePtr):
    """E2-OWNED SEAM (issue #68 fills the queue-drive): the worker's main loop.

    A2.1: first drain this worker's seeded seam units — each unit runs
    seam_run_unit (one current_worker TLS observation, the lane's acceptance
    "task").

    E6 (issue #72) IDLE SLEEP/WAKE: after the seam units the worker does NOT
    busy-spin on the shutdown latch — it SLEEPS on the pool's NativeEvent via
    worker_idle_park (spec §21, issue #72 step 2/step 5): it commits as an
    idle sleeper, re-checks announced work (the lost-wakeup guard), and parks
    on wait_until(absolute CLOCK_MONOTONIC slice).  A producer's wake budget
    (pool.wake_one) signals the event; a parked worker wakes, re-checks the
    latch / re-parks.  The bounded deadline slice is the shutdown backstop:
    even if a signal races a just-parked worker, it wakes within one slice,
    sees the latch, and exits (never join-from-worker).

    NON-RAISING on purpose (b2 1.0.0b2 drops/compiles-out function calls
    inside try/except inside abi("C") defs — probed in this lane; the entry
    trampoline must not try/except, so every error surfaces as a FLAG).
    """
    var c = cell.bitcast[WorkerEntryCell]()
    while c[].units_left > 0:
        seam_run_unit(cell)
    while True:
        var latched = Atomic[DType.uint8].load[ordering=Ordering.ACQUIRE](
            c[].latch
        )
        if Int(latched) != 0:
            return
        # E6 idle park — body INLINED here (NOT via worker_idle_park): the
        # 1.0.0b2 compiler mis-lowers native_event_wait_until's UInt arg when
        # the extern is reached through THREE call layers from the abi("C")
        # trampoline (abiC -> pool_worker_loop -> worker_idle_park -> extern;
        # "Call parameter type does not match function signature" -> SEGV).
        # Two layers (abiC -> pool_worker_loop -> extern) lower correctly.
        # worker_idle_park stays for direct (non-abiC) callers.
        var deadline = monotonic_now_ns() + IDLE_PARK_SLICE_NS
        var acctp = c[].acct
        _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
            _idle_cell(acctp, IDLE_ACCT_IDLE), 1
        )
        var have_work = _idle_pending(acctp) > 0
        if have_work:
            _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
                _idle_cell(acctp, IDLE_ACCT_IDLE), -1
            )
            # Announced work present at the pre-park re-check: the embedder
            # drains the real task queues (a2.2 discipline; A2Bench contract).
            # Yield briefly — bounded, NOT a busy-spin — then re-check latch.
            sleep(0.0002)
        else:
            _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
                _idle_cell(acctp, IDLE_ACCT_PARK), 1
            )
            var consumed = native_event_wait_until(c[].event, deadline)
            _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
                _idle_cell(acctp, IDLE_ACCT_IDLE), -1
            )
            if consumed:
                _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
                    _idle_cell(acctp, IDLE_ACCT_WAKE), 1
                )
                if _idle_pending(acctp) == 0:
                    _ = Atomic[DType.int64].fetch_add[ordering=Ordering.SEQUENTIAL](
                        _idle_cell(acctp, IDLE_ACCT_SPUR), 1
                    )
            # This worker genuinely parked on the NativeEvent and returned
            # (consumed a token or the slice elapsed); re-check the latch.
            c[].idle_parks += 1


def seam_run_unit(cell: BytePtr):
    """One A2.1 seam unit = one acceptance 'task': read current_worker TLS,
    verify it is still THIS worker's cell (a task never sees a different
    worker id mid-stream on this lane), and record the worker id into the
    observation slice.  NON-RAISING (b2 try-in-abiC drop workaround; a full
    observation slice just trips unit_ok).  Issue #68 replaces this whole
    unit source with queue-driven scheduler slices; the current_worker read
    is the only part that stays."""
    var c = cell.bitcast[WorkerEntryCell]()
    var me = tls_get(c[].cw)
    if Int(me) != Int(c[].worker):
        c[].unit_ok = False
    if c[].obs_done >= c[].obs_cap:
        c[].unit_ok = False
        return
    c[].obs[c[].obs_done] = c[].worker_id
    c[].obs_done += 1
    c[].units_left -= 1


def mjs_pool_entry_main(ud: BytePtr) abi("C"):
    """THE worker-thread trampoline body (@export'ed by the embedding binary;
    see EMBEDDING RULE above).  Runs on the NEW OS thread:
      1. TLS (spec §22/§69) established AT ENTRY — current_worker := this
         worker's cell, current_task / current_scope := the cleared sentinel
         — via the RAW externs (coarse, entry-only).
      2. the E2-OWNED seam loop (pool_worker_loop, NON-raising — b2
         1.0.0b2 compiles-out calls inside try/except inside abi("C") defs,
         probed in this lane, so the entry has NO try/except at all).
      3. TLS slots CLEARED at exit + flags exited + loop_ok so join_all()
         can audit the shutdown."""
    var cell = ud.bitcast[WorkerEntryCell]()
    # b2 cannot construct a NULL BytePtr (non-nullable), so the "cleared / no
    # runtime bound" TLS value is the address-1 sentinel the codebase uses as
    # a null-equivalent; the accessors treat Int(p) <= 1 as "absent".
    var cleared = BytePtr(unsafe_from_address=1)
    var rc = pthread_setspecific(cell[].cw.raw(), cell[].worker)
    if rc != 0:
        cell[].entry_ok = False
        cell[].exited = True
        return
    if pthread_setspecific(cell[].ct.raw(), cleared) != 0:
        cell[].entry_ok = False
        cell[].exited = True
        return
    if pthread_setspecific(cell[].cs.raw(), cleared) != 0:
        cell[].entry_ok = False
        cell[].exited = True
        return
    var back = pthread_getspecific(cell[].cw.raw())
    cell[].entry_ok = Int(back) == Int(cell[].worker)
    pool_worker_loop(ud)
    # A2.6 (issue #72): clear ALL three TLS slots at exit (spec §22/§69) so
    # the OS-worker-local pointers never dangle past the trampoline.
    _ = pthread_setspecific(cell[].cw.raw(), cleared)
    _ = pthread_setspecific(cell[].ct.raw(), cleared)
    _ = pthread_setspecific(cell[].cs.raw(), cleared)
    cell[].loop_ok = True
    cell[].exited = True

# ---------------------------------------------------------------------------
# spawn_all_workers — the pool's thread-spawn loop, LIVING HERE (b2 crash
# workaround, see worker_pool.mojo's top note: the identical loop crashed the
# 1.0.0b2 compiler inside worker_pool.mojo in every shape — as a struct
# method, as a module fn there, and with hoisted locals; in this module it
# compiles and runs).  Pure pointer math + the key VALUE; no method calls per
# iteration.  Threads run entry(cell_addr); the caller (pool) must have fully
# written every entry cell BEFORE calling (happens-before via pthread_create).
# ---------------------------------------------------------------------------

def spawn_all_workers(
    workers_base: BytePtr,
    entries_base: BytePtr,
    key: NativeTlsKey,
    n: Int,
    entry: BytePtr,
) raises:
    for i in range(n):
        var w = UnsafePointer[Worker, MutAnyOrigin](
            unsafe_from_address=Int(workers_base) + i * CELL_WORKER
        )
        var e = UnsafePointer[WorkerEntryCell, MutAnyOrigin](
            unsafe_from_address=Int(entries_base) + i * CELL_ENTRY
        )
        var t = spawn_native_thread(entry, e.bitcast[Byte]())
        w[].mark_started(t, key)
