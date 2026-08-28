# mojito_async/runtime/worker_pool.mojo
#
# A2.1 (issue #67) — the M:N worker pool over mojito-sys.NativeThread
# (spec phase A2: "N mojito-sys.NativeThread workers").  Owns the worker
# array, the pool lifecycle (start / request_shutdown / shutdown / join_all),
# the worker-thread entry cells, the shutdown latch and the three TLS slots.
#
# OWNERSHIP (wave-1 A2 map): this lane owns worker_pool.mojo,
# thread_entry.mojo, worker.mojo, config.mojo, the mojito_sys thread/TLS
# bindings and t30_worker_pool_aot.mojo.  The queue lane (#68) fills the
# E2-OWNED seam in thread_entry.pool_worker_loop; sibling lanes own
# queue.mojo / inject_queue.mojo / scheduler.mojo — NOT touched here.
#
# DISPATCH-FREE (b2): no queue and no dynamic dispatch this lane — the
# worker loop drains the SEEDED seam units (thread_entry.seam_run_unit:
# current_worker TLS observations, the acceptance "tasks") and then parks on
# the shutdown latch.  start() takes the trampoline CODE ADDRESS from the
# embedding binary (the EMBEDDING RULE in thread_entry.mojo): every binary
# that spawns pool threads declares
#     @export("mjs_pool_entry") def mjs_pool_entry(ud: BytePtr) abi("C"):
#         thread_entry.mjs_pool_entry_main(ud)
# and passes entry_pointer["mjs_pool_entry"]() — the adrp/add recipe, AOT-
# proven (the JIT cannot resolve @export symbols out of imported modules,
# modular/modular#6971).
#
# MEMORY: the worker array, the entry cells and the latch live in pool-owned
# HEAP allocated at construction (stable addresses while threads run; b2 has
# no placement new, so Worker/WorkerEntryCell values are moved into their
# cells via deref-assignment — the t28-style opaque-cell contract,
# CELL_WORKER/CELL_ENTRY >= real sizes).  join_all() joins every thread;
# finalize() returns the heap and the TLS keys.  pthread_join gives the
# happens-before edge: entry-cell observation fields are read without
# atomics after the joins.
#
# TLS: three NativeTlsKey slots reserved per spec §22/§69 — current_worker
# (written at thread entry, coarse granularity; the C registry mutex makes
# per-task gets a scalability mistake per the S2.4 note — no hot-path get()s
# this lane), current_task + current_scope (RESERVED, E-lanes claim them).
# The keys are created by the first start() and released by finalize()
# (pthread_key_delete; M5 fold, PR #104); a restarted pool re-arms by
# deleting the previous cycle's keys first, so no cycle leaks a slot.
#
# SHUTDOWN DISCIPLINE (issue #67): never join from inside a worker loop —
# the main thread owns all joins; request_shutdown() latches; parked idles
# see the latch and exit (the native-event idle path is E6).
#
# NOTE (b2 1.0.0b2 compiler crashes, worked around here): (1) raising joins of
# a dynamic String with a literal inside extern-bearing modules crash the
# compiler, so raise messages in vendor/mojito_sys.mojo are single
# conversions; (2) a spawn+mark_started LOOP inside THIS module crashed the
# compiler in every shape researched — as a WorkerPool method reading
# `self._tls.current_worker` in the loop, as a module fn here, and with
# hoisted locals (probed).  The loop body therefore lives at MODULE level in
# thread_entry.spawn_all_workers (the same code compiles and runs there);
# WorkerPool.spawn_all_workers() — the pool's ONLY spawn surface (H3 fold,
# PR #104) — hoists the TLS-key read OUT of any loop and delegates the loop
# to that module fn.
from std.atomic import Atomic, Ordering
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.config import RuntimeConfig, make_pool_config
from mojito_async.runtime.thread_entry import (
    CELL_ENTRY,
    CELL_WORKER,
    WorkerEntryCell,
1: @both
)
from mojito_async.runtime.worker import Worker
from mojito_async.vendor.mojito_sys import (
    NativeTlsKey,
    c_free,
    c_malloc,
    delete_tls_key,
    join_native_thread,
    make_native_thread,
    make_tls_key,
)


# ---------------------------------------------------------------------------
# WorkerTls — the pool's three spec §69 slots (one key set per pool; the
# keys are process-lifetime survivor values read by workers at entry).
# ---------------------------------------------------------------------------

struct WorkerTls(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    var current_worker: NativeTlsKey
    var current_task: NativeTlsKey
    var current_scope: NativeTlsKey

    def __init__(out self):
        self.current_worker = NativeTlsKey()
        self.current_task = NativeTlsKey()
        self.current_scope = NativeTlsKey()


def make_worker_tls() raises -> WorkerTls:
    """Create the pool's three TLS key slots.  `current_task` and
    `current_scope` are RESERVED for the E-lanes (spec §22/§69 documents the
    coarse OS-worker-local semantics; A2.1 only exercises current_worker)."""
    var t = WorkerTls()
    t.current_worker = make_tls_key()
    t.current_task = make_tls_key()
    t.current_scope = make_tls_key()
    return t


# ---------------------------------------------------------------------------
# WorkerPool (the spawn loop body lives at module level in
# thread_entry.spawn_all_workers; see the NOTE at the top of this file).
# ---------------------------------------------------------------------------

struct WorkerPool:
    """The M:N worker pool: N NativeThread workers, each owning a Worker
    (id + Runtime + thread handle + TLS key) and an entry cell, one shutdown
    latch, and the pool lifecycle.  start() arms the pool (validate + TLS
    keys + cells + latch), spawn_all_workers() spawns every thread — the
    pool's ONLY spawn surface (H3 fold, PR #104); stopped by
    request_shutdown() and reaped by join_all(); finalize() releases the TLS
    keys and the heap and FINALIZES the pool (a later start() raises loudly —
    M5 fold, PR #104 — instead of re-arming through the address-1 sentinel).

    NOT copyable (owns heap); constructed via make_pool() / make_pool(config).
    """

    var _config: RuntimeConfig
    var _tls: WorkerTls
    var _workers_base: BytePtr        # worker_count * CELL_WORKER heap cells
    var _entries_base: BytePtr        # worker_count * CELL_ENTRY heap cells
    var _latch: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin]
    var _seeded: Bool
    var _started: Bool
    var _spawned: Bool                # spawn_all_workers ran this cycle
    var _tls_armed: Bool              # start() created live TLS keys
    var _joined: Int
    var _freed: Bool
    var _finalized: Bool              # finalize() ran: no further start()

    def __init__(out self):
        self._config = make_pool_config()
        self._tls = WorkerTls()
        self._started = False
        self._seeded = False
        self._spawned = False
        self._tls_armed = False
        self._joined = 0
        self._freed = False
        self._finalized = False
        var n = self._config.worker_count
        self._workers_base = c_malloc(n * CELL_WORKER)
        self._entries_base = c_malloc(n * CELL_ENTRY)
        self._latch = c_malloc(8).bitcast[Scalar[DType.uint8]]()
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 0)

    def __init__(out self, config: RuntimeConfig):
        self._config = config
        self._tls = WorkerTls()
        self._started = False
        self._seeded = False
        self._spawned = False
        self._tls_armed = False
        self._joined = 0
        self._freed = False
        self._finalized = False
        var n = self._config.worker_count
        self._workers_base = c_malloc(n * CELL_WORKER)
        self._entries_base = c_malloc(n * CELL_ENTRY)
        self._latch = c_malloc(8).bitcast[Scalar[DType.uint8]]()
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 0)

    # --- addressing ---------------------------------------------------------

    def worker_at(mut self, i: Int) -> UnsafePointer[Worker, MutAnyOrigin]:
        """Address of worker cell i (stride CELL_WORKER; the same address the
        worker thread holds in TLS as current_worker)."""
        return UnsafePointer[Worker, MutAnyOrigin](
            unsafe_from_address=Int(self._workers_base) + i * CELL_WORKER
        )

    def entry_at(mut self, i: Int) -> UnsafePointer[WorkerEntryCell, MutAnyOrigin]:
        return UnsafePointer[WorkerEntryCell, MutAnyOrigin](
            unsafe_from_address=Int(self._entries_base) + i * CELL_ENTRY
        )

    # --- lifecycle ----------------------------------------------------------

    def start(mut self, entry: BytePtr) raises:
        """Prepare the pool for spawning: validate the config (M6 fold, PR
        #104 — RuntimeConfig.validate() runs FIRST, every start), arm the
        latch, write the worker/entry cells, create the three TLS slots.
        `entry` is the embedding binary's trampoline address
        (@export("mjs_pool_entry"); thread_entry.mojo EMBEDDING RULE).
        The EMBEDDER then spawns every thread with ONE
        spawn_all_workers(entry) call — the pool's only spawn surface (H3
        fold).  Refuses a second start before join_all(), and refuses ANY
        start after finalize() (M5 fold: the freed heap must never be
        re-armed through the address-1 sentinel latch).  Any seam units
        seeded before start() are drained by the workers on entry, then
        they idle on the latch."""
        self._config.validate()
        if self._finalized:
            raise Error("worker_pool.start: pool finalized (no restart)")
        if self._started:
            raise Error("worker_pool.start: pool already started")
        if self._tls_armed:
            # A previous start() cycle armed keys that were never released
            # (nothing deletes them on the join path): delete them before
            # re-arming so a restarted pool does not leak TLS slots (M5).
            delete_tls_key(self._tls.current_worker)
            delete_tls_key(self._tls.current_task)
            delete_tls_key(self._tls.current_scope)
            self._tls_armed = False
        # M4 (PR #107 fold, issue #112): the heap cell strides must cover
        # the REAL struct sizes — comptime assert + runtime backstop (the
        # 512-byte blind CELL_WORKER once overflowed the pool heap).
        cell_size_gate()
        self._tls = make_worker_tls()
        self._tls_armed = True
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 0)
        self._build_cells()
        self._started = True
        self._joined = 0

    def spawn_all_workers(mut self, entry: BytePtr) raises:
        """Spawn EVERY worker thread in one call — the pool's ONLY spawn
        surface (H3 fold, PR #104).  Requires start() first (cells + TLS
        armed); refuses a second call before join_all() (a missed join
        would orphan the first batch's threads).  mark_started + the
        current_worker TLS-key wiring happen INSIDE the pool, so an
        embedder calling this cannot forget them — the old footgun (a
        driver-side spawn_native_thread loop + manual mark_started) is
        gone.

        The b2 compiler-crash workaround (NOTE at top) keeps the spawn LOOP
        out of this module: the TLS key is hoisted here (one read, outside
        any loop) and thread_entry.spawn_all_workers — the module-level
        loop with the exact driver-side body (spawn_native_thread +
        mark_started per index) — does the spawn.  For every worker,
        mark_started completes BEFORE pthread_create returns, so no worker
        thread can ever run user code before its Worker/TLS wiring is
        visible (happens-before via pthread_create)."""
        if not self._started:
            raise Error("worker_pool.spawn_all_workers: call start() first")
        if self._spawned:
            raise Error("worker_pool.spawn_all_workers: already spawned")
        var key = self._tls.current_worker  # hoisted; NOT read inside a loop
        spawn_all_worker_threads(
            self._workers_base,
            self._entries_base,
            key,
            self._config.worker_count,
            Int(entry),  # B2 constraint (probed on 1.0.0b2): a BytePtr param
            # into the spawn loop is miscompiled — threads start at a garbage
            # address; the trampoline address crosses as an Int and is
            # re-derived at the extern call site (see thread_entry.mojo).
        )
        self._spawned = True

    def _build_cells(mut self):
        """Write the worker cells (id = i) + entry cells (fresh when no seed
        was applied; the TLS slots + worker pointer always).  Entry cells
        must be fully written BEFORE pthread_create publishes them
        (happens-before via pthread_create)."""
        if not self._seeded:
            for i in range(self._config.worker_count):
                var e = self.entry_at(i)
                e[0] = WorkerEntryCell()
                e[].worker_id = i
                e[].latch = self._latch
        for i in range(self._config.worker_count):
            self._workers_reinit(i)

    def _workers_reinit(mut self, i: Int):
        """(Re)write one worker + entry cell: the Worker's id + the entry
        cell's worker pointer + TLS slots.  Called on every start() so a
        restarted pool re-arms a fresh Worker (Runtime reset)."""
        var w = self.worker_at(i)
        w[0] = Worker(i)
        var e = self.entry_at(i)
        e[].worker = w.bitcast[Byte]()
        e[].cw = self._tls.current_worker
        e[].ct = self._tls.current_task
        e[].cs = self._tls.current_scope

    # E2-OWNED seam surface (issue #67 acceptance; issue #68 replaces this
    # with real enqueue once the local queues exist): seed `per_worker` seam
    # units per worker.  Each unit records its worker id into `obs`
    # (driver-owned heap, obs_total slots total; worker i owns slice
    # [i*per_worker, (i+1)*per_worker)).  Must run BEFORE start() — the
    # workers drain their units on entry, then idle on the latch.
    def seed_seam_units(
        mut self,
        per_worker: Int,
        obs: UnsafePointer[Int, MutAnyOrigin],
        obs_total: Int,
    ) raises:
        if self._started:
            raise Error("worker_pool.seed_seam_units: before start() only")
        if self._finalized:
            raise Error("worker_pool.seed_seam_units: pool finalized")
        if per_worker < 1:
            raise Error("worker_pool.seed_seam_units: per_worker must be >= 1")
        var n = self._config.worker_count
        if obs_total < n * per_worker:
            raise Error("worker_pool.seed_seam_units: obs_total too small")
        for i in range(n):
            var e = self.entry_at(i)
            e[0] = WorkerEntryCell()
            e[].worker_id = i
            e[].latch = self._latch
            e[].obs = obs + i * per_worker
            e[].obs_cap = per_worker
            e[].obs_done = 0
            e[].units_left = per_worker
            e[].units_seeded = per_worker
            e[].unit_ok = True
        self._seeded = True

    def request_shutdown(mut self) raises:
        """Latch the shutdown request (RELEASE store); every worker loop
        observes it on its next latch poll and exits.  Idempotent."""
        if not self._started:
            raise Error("worker_pool.request_shutdown: pool not started")
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 1)

    def shutdown(mut self) raises:
        """request_shutdown() + join_all(): the complete stop path (never
        called from inside a worker loop — the main thread owns joins)."""
        self.request_shutdown()
        self.join_all()

    def join_all(mut self) raises:
        """Join every started worker thread.  pthread_join provides the
        happens-before edge for the entry-cell observations.  After this
        returns, no pool thread is alive (no thread leaks by construction:
        every spawned thread either observed the latch and exited, or was
        joined here).  The heap stays until finalize()/deinit, so the
        entry-cell audit accessors remain readable post-join."""
        if not self._started:
            return
        for i in range(self._config.worker_count):
            if self.worker_at(i)[].started():
                join_native_thread(self.worker_at(i)[].thread())
                self._joined += 1
        self._started = False
        self._seeded = False
        self._spawned = False

    def finalize(mut self) raises:
        """Release the pool's resources (idempotent): the three TLS keys
        (M5 fold, PR #104 — pthread_key_delete per slot) and the pool heap.
        Call after join_all() and after reading the entry-cell results;
        refuses a LIVE pool (its threads would touch deleted keys).  After
        this returns the pool is FINALIZED: a later start() raises loudly
        instead of re-arming through the address-1 sentinel latch."""
        if self._freed:
            return
        if self._started:
            raise Error("worker_pool.finalize: join_all() first")
        if self._tls_armed:
            delete_tls_key(self._tls.current_worker)
            delete_tls_key(self._tls.current_task)
            delete_tls_key(self._tls.current_scope)
            self._tls_armed = False
        if Int(self._workers_base) > 1:
            c_free(self._workers_base)
            self._workers_base = BytePtr(unsafe_from_address=1)
        if Int(self._entries_base) > 1:
            c_free(self._entries_base)
            self._entries_base = BytePtr(unsafe_from_address=1)
        if Int(self._latch) > 1:
            c_free(self._latch.bitcast[Byte]())
            self._latch = UnsafePointer[Scalar[DType.uint8], MutAnyOrigin](
                unsafe_from_address=1
            )
        self._freed = True
        self._finalized = True

    # --- observability ------------------------------------------------------

    def worker_count(self) -> Int:
        return self._config.worker_count

    def started(self) -> Bool:
        return self._started

    def threads_joined(self) -> Int:
        return self._joined

    def poll_done(mut self) -> Bool:
        """All seam units drained (every worker's obs_done == its seeded
        count).  Polled by the driver between start() and shutdown()."""
        if not self._started:
            return False
        for i in range(self._config.worker_count):
            if self.entry_at(i)[].obs_done < self.entry_at(i)[].units_seeded:
                return False
        return True

    # entry-cell audit accessors (read AFTER join_all; happens-before via
    # pthread_join):
    def entry_ok(mut self, i: Int) -> Bool:
        return self.entry_at(i)[].entry_ok

    def unit_ok(mut self, i: Int) -> Bool:
        return self.entry_at(i)[].unit_ok

    def loop_ok(mut self, i: Int) -> Bool:
        return self.entry_at(i)[].loop_ok

    def exited(mut self, i: Int) -> Bool:
        return self.entry_at(i)[].exited

    def obs_done(mut self, i: Int) -> Int:
        return self.entry_at(i)[].obs_done





# --- module factories (b2 has no static methods) ---------------------------

def make_pool() -> WorkerPool:
    """Pool with the DEFAULT config (worker_count = cpu_logical_count())."""
    return WorkerPool()


def make_pool(config: RuntimeConfig) -> WorkerPool:
    """Pool with an explicit config (see runtime/config.mojo)."""
    return WorkerPool(config)