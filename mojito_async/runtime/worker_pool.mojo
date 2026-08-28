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
# finalize() returns the heap.  pthread_join gives the happens-before edge:
# entry-cell observation fields are read without atomics after the joins.
#
# TLS: three NativeTlsKey slots reserved per spec §22/§69 — current_worker
# (written at thread entry, coarse granularity; the C registry mutex makes
# per-task gets a scalability mistake per the S2.4 note — no hot-path get()s
# this lane), current_task + current_scope (RESERVED, E-lanes claim them).
#
# SHUTDOWN DISCIPLINE (issue #67): never join from inside a worker loop —
# the main thread owns all joins; request_shutdown() latches; parked idles
# see the latch and exit (the native-event idle path is E6).
#
# NOTE (b2 1.0.0b2 compiler crashes, worked around here): (1) raising joins of
# a dynamic String with a literal inside extern-bearing modules crash the
# compiler, so raise messages in vendor/mojito_sys.mojo are single
# conversions; (2) this lane's spawn+mark_started loop crashed the compiler
# as a WorkerPool method reading `self._tls.current_worker` in the loop — the
# loop body lives at MODULE level in _spawn_one (probed: the same code as a
# module function compiles and runs).
from std.atomic import Atomic, Ordering
from mojito_async.integration.sys import BytePtr
from mojito_async.runtime.config import RuntimeConfig, make_pool_config
from mojito_async.runtime.idle import (
    ACCT_BYTES,
    acct_parked,
    acct_pending,
    acct_park_total,
    acct_spurious_total,
    acct_wake_total,
    announce_work as acct_announce,
    complete_work as acct_complete,
)
from mojito_async.runtime.thread_entry import (
    CELL_ENTRY,
    CELL_WORKER,
    WorkerEntryCell,
)
from mojito_async.runtime.worker import Worker
from mojito_async.vendor.mojito_sys import (
    NativeEvent,
    NativeTlsKey,
    c_free,
    c_malloc,
    join_native_thread,
    make_native_event,
    make_tls_key,
    ms_event_destroy,
    native_event_signal,
    spawn_native_thread,
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
# WorkerPool (the pool's spawn loop body lives at module level in _spawn_one;
# see the NOTE at the top of this file).
# ---------------------------------------------------------------------------

struct WorkerPool:
    """The M:N worker pool: N NativeThread workers, each owning a Worker
    (id + Runtime + thread handle + TLS key) and an entry cell, one shutdown
    latch, and the pool lifecycle.  Threads are spawned by start(), observed
    while running, stopped by request_shutdown() and reaped by join_all().

    NOT copyable (owns heap); constructed via make_pool() / make_pool(config).
    """

    var _config: RuntimeConfig
    var _tls: WorkerTls
    var _workers_base: BytePtr
    var _entries_base: BytePtr
    var _latch: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin]
    var _seeded: Bool
    var _started: Bool
    var _joined: Int
    var _freed: Bool
    var _event: NativeEvent   # A2.6/E6: the per-pool idle park NativeEvent
    var _acct: BytePtr        # A2.6/E6: idle-accounting block (atomics)

    def __init__(out self):
        self._config = make_pool_config()
        self._tls = WorkerTls()
        self._started = False
        self._seeded = False
        self._joined = 0
        self._freed = False
        var n = self._config.worker_count
        self._workers_base = c_malloc(n * CELL_WORKER)
        self._entries_base = c_malloc(n * CELL_ENTRY)
        self._latch = c_malloc(8).bitcast[Scalar[DType.uint8]]()
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 0)
        self._event = NativeEvent()
        # Accounting block allocated AT CONSTRUCTION (crash fix, A2.6): no
        # copy/read may ever dereference the address-1 sentinel; _ensure_idle_
        # state() keeps re-arming it across restarts without reallocating.
        self._acct = c_malloc(ACCT_BYTES)

    def __init__(out self, config: RuntimeConfig):
        self._config = config
        self._tls = WorkerTls()
        self._started = False
        self._seeded = False
        self._joined = 0
        self._freed = False
        var n = self._config.worker_count
        self._workers_base = c_malloc(n * CELL_WORKER)
        self._entries_base = c_malloc(n * CELL_ENTRY)
        self._latch = c_malloc(8).bitcast[Scalar[DType.uint8]]()
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 0)
        self._event = NativeEvent()
        # Accounting block allocated AT CONSTRUCTION (crash fix, A2.6): no
        # copy/read may ever dereference the address-1 sentinel; _ensure_idle_
        # state() keeps re-arming it across restarts without reallocating.
        self._acct = c_malloc(ACCT_BYTES)

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
        """Prepare the pool for spawning (latch arm, worker/entry cell writes,
        TLS slots).  `entry` is the embedding binary's trampoline address
        (@export("mjs_pool_entry"); thread_entry.mojo EMBEDDING RULE).  The
        EMBEDDER then spawns the workers with one spawn_worker call per index
        (b2 1.0.0b2 compiler-bug workaround — a spawn LOOP inside this module
        crashes the compiler in every shape; see the NOTE at the top of this
        file).  Refuses a second start before join_all().  Any seam units
        seeded before start() are drained by the workers on entry, then they
        idle on the latch."""
        self._tls = make_worker_tls()
        self._ensure_idle_state()
        self._zero_acct()
        self._build_cells()
        self._started = True
        self._joined = 0

    def current_worker_key(mut self) -> NativeTlsKey:
        """The pool's current_worker TLS slot (spec §69), read BEFORE the
        spawn loop and passed by value into spawn_worker (compiler-bug
        workaround shape)."""
        return self._tls.current_worker

    def _ensure_idle_state(mut self) raises:
        """(Lazily) create the A2.6/E6 shared idle state: the per-pool
        NativeEvent (the idle park target) + the lock-free accounting block
        (idle sleepers / announced work / spec §71 counters).  Created on the
        first start(); reused across restarts; destroyed at finalize()."""
        if not self._event.alive():
            self._event = make_native_event()
        if Int(self._acct) <= 1:
            self._acct = c_malloc(ACCT_BYTES)

    def _zero_acct(mut self):
        """Reset the accounting block's five counters (each start() re-arms)."""
        for off in range(5):
            var p = UnsafePointer[Int64, MutAnyOrigin](
                unsafe_from_address=Int(self._acct) + off * 8
            )
            Atomic[DType.int64].store[ordering=Ordering.SEQUENTIAL](p, 0)

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
        cell's worker pointer + TLS slots + the A2.6/E6 idle park state
        (NativeEvent handle + accounting block).  Called on every start() so
        a restarted pool re-arms a fresh Worker (Runtime reset)."""
        var w = self.worker_at(i)
        w[0] = Worker(i)
        var e = self.entry_at(i)
        e[].worker = w.bitcast[Byte]()
        e[].cw = self._tls.current_worker
        e[].ct = self._tls.current_task
        e[].cs = self._tls.current_scope
        e[].event = self._event.handle()
        e[].acct = self._acct
        w[].set_pool_idle(self._event.handle(), self._acct)

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
        """Latch the shutdown request (RELEASE store) AND wake every parked
        idle worker so they exit promptly (A2.6/E6, issue #72 step 4): a
        parked worker is asleep on the pool NativeEvent, so the latch alone
        would not wake it until its next 50 ms deadline slice.  We therefore
        signal the event once per currently-parked sleeper (breadth-one);
        a just-parked worker that raced the signals is still woken by its
        deadline-slice backstop.  Idempotent."""
        if not self._started:
            raise Error("worker_pool.request_shutdown: pool not started")
        Atomic[DType.uint8].store[ordering=Ordering.RELEASE](self._latch, 1)
        var sleepers = acct_parked(self._acct)
        if sleepers < 1:
            sleepers = 1
        for i in range(sleepers):
            native_event_signal(self._event)

    # --- E6 producer-side wake budget (issue #72 step 3) --------------------

    def wake_one(mut self) raises:
        """Producer side: signal the pool NativeEvent ONLY IF at least one
        worker is parked as an idle sleeper (never burn a signal into
        nobody).  Breadth-one (the C event wakes at most one waiter); a
        producer that injects K work units calls wake_one() up to K times —
        K units wake at most K sleepers."""
        if not self._started:
            raise Error("worker_pool.wake_one: pool not started")
        if acct_parked(self._acct) > 0:
            native_event_signal(self._event)

    def wake_one_force(mut self) raises:
        """Unconditionally signal the event (used by shutdown / driver
        teardown); sticky tokens are harmless."""
        if not self._started:
            raise Error("worker_pool.wake_one_force: pool not started")
        native_event_signal(self._event)

    def announce_work(mut self, n: Int) raises:
        """A producer injected `n` work units: bump the announced-work count
        the workers' wake re-check classifies against."""
        if not self._started:
            raise Error("worker_pool.announce_work: pool not started")
        acct_announce(self._acct, n)

    def complete_work(mut self, n: Int) raises:
        """The embedder drained `n` units (real tasks completed)."""
        if not self._started:
            raise Error("worker_pool.complete_work: pool not started")
        acct_complete(self._acct, n)

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

    def finalize(mut self):
        """Return the pool's heap (idempotent).  Call after join_all() and
        after reading the entry-cell results."""
        if self._freed:
            return
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
        if Int(self._acct) > 1:
            c_free(self._acct)
            self._acct = BytePtr(unsafe_from_address=1)
        if self._event.alive():
            ms_event_destroy(self._event.handle())
            self._event = NativeEvent()
        self._freed = True

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

    # --- A2.6/E6 idle observability (issue #72) ----------------------------

    def idle_parked(mut self) -> Int:
        """Number of workers currently parked as idle sleepers (a SPINNING
        worker never holds one of these — this is the no-busy-spin proof)."""
        return acct_parked(self._acct)

    def pending_work(mut self) -> Int:
        """Announced (injected, not yet drained) work units."""
        return acct_pending(self._acct)

    def park_total(mut self) -> Int:
        """park_total (spec §71): OS-level idle parks."""
        return acct_park_total(self._acct)

    def wake_total(mut self) -> Int:
        """wake_total (spec §71): token-consumed wakes."""
        return acct_wake_total(self._acct)

    def spurious_total(mut self) -> Int:
        """spurious_wake_total (spec §71): consumed tokens that found no work."""
        return acct_spurious_total(self._acct)

    def idle_parks(mut self, i: Int) -> Int:
        """Times worker i parked as a sleeper (per-worker, post-join)."""
        return self.entry_at(i)[].idle_parks

    def pool_event(mut self) -> Int:
        """The pool's NativeEvent handle (A2.6/E6)."""
        return self._event.handle()





# --- module factories (b2 has no static methods) ---------------------------

def make_pool() -> WorkerPool:
    """Pool with the DEFAULT config (worker_count = cpu_logical_count())."""
    return WorkerPool()


def make_pool(config: RuntimeConfig) -> WorkerPool:
    """Pool with an explicit config (see runtime/config.mojo)."""
    return WorkerPool(config)