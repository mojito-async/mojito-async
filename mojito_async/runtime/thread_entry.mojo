# mojito_async/runtime/thread_entry.mojo
#
# A2.1 (issue #67) — the worker-thread entry: the C-ABI trampoline body
# (mjs_pool_entry_main), the per-worker entry-cell the trampoline reads, and
# the FUTURE worker-loop seam (E2-OWNED; the queue lane, issue #68, fills the
# loop body).  This is the A2 production home of the S2.2 thread-entry recipe
# (an @export'd abi("C") entry + the adrp/add code address, proven under
# mojito_sys and in the *_aot drivers).
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
# ENTRY CHOREOGRAPHY (A2.1 discipline):
#   1. current_worker TLS (spec §69) is written AT THREAD ENTRY, once, at
#      COARSE granularity — the slot receives the Worker cell address (the
#      value is then worker-stable for the thread's whole life).  Per the
#      S2.4 scalability note the C TLS layer holds one global registry mutex
#      on reads, so NO per-task get() hot paths exist this lane; the single
#      entry write + the acceptance read-back are the whole TLS surface.
#      current_task / current_scope keys (spec §22/§69) are RESERVED slots
#      carried in the cell and NOT written yet (the E-lanes claim them).
#   2. the worker runs the E2-OWNED seam (pool_worker_loop): this lane
#      drains the seeded seam units (direct current_worker observations — the
#      A2.1 acceptance "K tasks each observing current_worker"), then parks
#      idle on the shutdown latch.  Issue #68 replaces the unit source with
#      the worker's LOCAL runnable queue + scheduler drive; the latch check
#      stays.  The NativeEvent idle/wake path is E6.
#   3. exit: the loop returns when it observes the latch; the trampoline
#      flags exited so join_all() can assert every worker saw the shutdown.
#
# The entry cell lives in pool-owned heap (worker_pool.mojo) with a STABLE
# address for the worker's whole life; the worker's Runtime/TCB cells are
# never touched cross-thread by this lane (per-worker Runtime only).
#
# Externs: none HERE — pthread spawn/join/TLS live in vendor/mojito_sys.mojo
# at concrete module scope; this module only composes them.
from std.atomic import Atomic, Ordering
from std.time import sleep
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.worker import Worker
from mojito_async.vendor.mojito_sys import (
    NativeTlsKey,
    pthread_getspecific,
    pthread_setspecific,
    spawn_native_thread,
    tls_get,
)


# Worker/entry cell byte-strides for the pool's heap arrays (b2 exposes no
# public sizeof; >= real struct sizes is the documented t28-style contract).
comptime CELL_WORKER = Int(512)
comptime CELL_ENTRY = Int(256)


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
                  current_scope (RESERVED; the E-lanes write them).
    latch       — the pool's shutdown latch (Atomic[uint8]: 0 running, 1
                  shutdown requested); polled by the idle loop.
    units_left  — seam units still to run for this worker (issue #68 swaps
                  this for the local runnable queue).
    obs/obs_done/obs_cap — the seam unit's observation slice (driver-owned
                  heap): each unit records its worker id at obs[obs_done].
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


# ---------------------------------------------------------------------------
# E2-OWNED SEAM (echo to the queue lane): the worker-loop BODY lives in
# pool_worker_loop() below.  A2.1 implements it as drain-seam-units then
# latch-poll idle.  Issue #68 (queue lane) REPLACES the unit source with:
#     pop this worker's LOCAL runnable queue -> scheduler_loop(rt, <lane
#     dispatcher>, ud) — the seam function signature stays exactly
#     `pool_worker_loop(cell: BytePtr) raises`; stealing (#70) reaches the
#     stealable set from the same loop; the NativeEvent idle path is E6.
# A2.1 leaves the shutdown-latch poll as the ONLY loop motion after units,
# which is what makes "request_shutdown() observed by every worker loop;
# join_all() returns; no thread leaks" provable BEFORE the queue lands.
# ---------------------------------------------------------------------------

def pool_worker_loop(cell: BytePtr):
    """E2-OWNED SEAM (issue #68 fills this): the worker's main loop.

    A2.1: first drain this worker's seeded seam units — each unit runs
    seam_run_unit (one current_worker TLS observation, the lane's acceptance
    "task").  Then idle on the shutdown latch (checked every iteration; the
    A2.1 exit requirement is "parked idles see the latch and exit" — the
    latch poll IS the park; the NativeEvent idle path is E6).

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
        sleep(0.001)


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
      1. current_worker TLS := this worker's cell via the RAW externs
         (coarse, entry-only).
      2. the E2-OWNED seam loop (pool_worker_loop, NON-raising — b2
         1.0.0b2 compiles-out calls inside try/except inside abi("C") defs,
         probed in this lane, so the entry has NO try/except at all).
      3. flag exited + loop_ok so join_all() can audit the shutdown."""
    var cell = ud.bitcast[WorkerEntryCell]()
    var rc = pthread_setspecific(cell[].cw.raw(), cell[].worker)
    if rc != 0:
        cell[].entry_ok = False
        cell[].exited = True
        return
    var back = pthread_getspecific(cell[].cw.raw())
    cell[].entry_ok = Int(back) == Int(cell[].worker)
    pool_worker_loop(ud)
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
