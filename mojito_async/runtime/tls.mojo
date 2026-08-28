# mojito_async/runtime/tls.mojo
#
# A2.6 (issue #72) — the three spec §22/§69 OS-worker TLS slots
# (current_worker / current_task / current_scope), the accessors, and the
# NoConcurrencyRuntime error path.
#
# TLS model (spec §69): native TLS is OS-WORKER-local, NEVER task-local.
# The pool (issue #67) creates one NativeTlsKey per slot and the worker-thread
# trampoline (thread_entry.mjs_pool_entry_main) establishes them AT ENTRY and
# clears them AT EXIT (current_worker = the Worker cell; current_task /
# current_scope = null at entry — no task/scope is current when a worker
# enters).  Coarse granularity only: the C TLS layer behind pthread holds one
# global registry mutex per read (S2.4 note), so there are NO per-task hot
# get()s — current_task/current_scope are bound once at entry (null) and the
# E-lanes populate them at task boundaries when they land.
#
# Accessors (spec §22): `_current_worker` / `_current_task` / `_current_scope`
# read THIS OS thread's slot for the given key and return a raw pointer, or
# raise `NoConcurrencyRuntime` when the runtime is absent (the slot reads the
# null address — i.e. no pool trampoline bound this thread).  A concurrency
# primitive invoked outside run() surfaces this explicit error rather than
# dereferencing null.  Typed wraps (`current_worker()` in worker.mojo) cast
# the raw address consumer-side so this module stays import-light (no cycle
# with worker.mojo).
#
# Mojo 1.0.0b2 (def-only): no module mutable globals, no static methods.
from mojito_async.integration.sys import BytePtr
from mojito_async.vendor.mojito_sys import NativeTlsKey, tls_get, tls_set


# ---------------------------------------------------------------------------
# NoConcurrencyRuntime — the spec §22 runtime-absent error model
# ---------------------------------------------------------------------------
# Raised when a concurrency primitive / TLS accessor is invoked where no
# concurrency runtime is bound to the current OS worker (the slot reads the
# null address).  Prefer an explicit run()/pool in Phase 1 (spec §22 chooses
# the clear error over a process-root fallback).
struct NoConcurrencyRuntime:
    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# Binding helpers — established per worker at entry, cleared at exit
# ---------------------------------------------------------------------------

def bind_current_worker(key: NativeTlsKey, value: BytePtr) raises:
    """Write THIS OS thread's current_worker slot (entry: the Worker cell).
    Coarse, entry-only — the pool trampoline calls this once, then never on
    the hot path (S2.4 scalability note)."""
    tls_set(key, value)


def bind_current_task(key: NativeTlsKey, value: BytePtr) raises:
    """Write THIS OS thread's current_task slot (spec §22; E-lanes populate at
    task boundaries; bound to null at entry)."""
    tls_set(key, value)


def bind_current_scope(key: NativeTlsKey, value: BytePtr) raises:
    """Write THIS OS thread's current_scope slot (spec §22; E-lanes populate;
    bound to null at entry)."""
    tls_set(key, value)


def clear_worker_tls(cw: NativeTlsKey, ct: NativeTlsKey, cs: NativeTlsKey) raises:
    """Clear all three slots at worker EXIT (spec §22/§69: the OS-worker-
    local pointers must not dangle past the trampoline).  b2 cannot build a
    NULL BytePtr (non-nullable), so the cleared value is the address-1
    sentinel (<=1 reads as "no runtime bound" in the accessors)."""
    var cleared = BytePtr(unsafe_from_address=1)
    tls_set(cw, cleared)
    tls_set(ct, cleared)
    tls_set(cs, cleared)


# ---------------------------------------------------------------------------
# Accessors (spec §22) — read this OS thread's slot; NoConcurrencyRuntime on
# a null slot (no runtime bound).  Return the RAW address; typed casts happen
# at the consumer (worker.mojo's current_worker() -> UnsafePointer[Worker]).
# ---------------------------------------------------------------------------

def current_worker_addr(key: NativeTlsKey) raises -> BytePtr:
    """Raw address of this OS worker's current_worker slot value.  Raises
    NoConcurrencyRuntime when the slot is absent (<= the address-1 cleared
    sentinel / no pool bound this thread / the worker has exited)."""
    var p = tls_get(key)
    if Int(p) <= 1:
        raise Error("NoConcurrencyRuntime: current_worker has no runtime bound")
    return p


def current_task_addr(key: NativeTlsKey) raises -> BytePtr:
    """Raw address of this OS worker's current_task slot value.  Raises
    NoConcurrencyRuntime when absent (no task is running on this worker)."""
    var p = tls_get(key)
    if Int(p) <= 1:
        raise Error("NoConcurrencyRuntime: current_task has no runtime bound")
    return p


def current_scope_addr(key: NativeTlsKey) raises -> BytePtr:
    """Raw address of this OS worker's current_scope slot value.  Raises
    NoConcurrencyRuntime when absent (no scope is current on this worker)."""
    var p = tls_get(key)
    if Int(p) <= 1:
        raise Error("NoConcurrencyRuntime: current_scope has no runtime bound")
    return p


def tls_is_bound(key: NativeTlsKey) -> Bool:
    """True when THIS OS thread's slot for `key` holds a real value (> the
    address-1 "cleared" sentinel); used by drivers to probe the runtime-
    absent path without raising."""
    return Int(tls_get(key)) > 1
