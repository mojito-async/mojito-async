# mojito_async/runtime/tls.mojo
#
# A2.6 (issue #72) — the three spec §22/§69 OS-worker TLS slots
# (current_worker / current_task / current_scope), the entry/exit bindings
# the worker trampoline drives, and the NoConcurrencyRuntime error model for
# the runtime-absent accessor path.
#
# TLS model (spec §69): native TLS is OS-WORKER-local, NEVER task-local.
# The pool (issue #67) creates one NativeTlsKey per slot; the worker-thread
# trampoline (thread_entry.mjs_pool_entry_main) establishes the slots AT
# ENTRY via bind_current_worker / bind_current_task / bind_current_scope and
# clears them AT EXIT via clear_worker_tls (current_worker = the Worker
# cell; current_task / current_scope = the cleared sentinel at entry — no
# task/scope is current when a worker enters).  Coarse granularity only: the
# C TLS layer behind pthread holds one global registry mutex per read (S2.4
# note), so there are NO per-task hot get()s — current_task/current_scope
# are bound once at entry (null-equivalent) and the E-lanes populate them at
# task boundaries when they land.
#
# The bind/clear helpers are NON-RAISING Bool-returning on purpose: the
# trampoline is an abi("C") def (b2 1.0.0b2 drops calls inside try/except
# inside abi("C") defs), so every failure surfaces as a flag in the entry
# cell (entry_ok / loop_ok), never as a raise through the C boundary.  They
# call the vendor pthread wrappers directly (pthread_setspecific) and keep
# the same key.raw() slice the trampoline's raw path used.
#
# ACCESSOR (spec §22): `current_worker_addr` reads THIS OS thread's slot for
# the key and returns a raw pointer, or raises `NoConcurrencyRuntime` when
# the runtime is absent (the slot reads the null/cleared address — i.e. no
# pool trampoline bound this thread, or the worker already exited).  A
# concurrency primitive invoked outside run() surfaces this explicit error
# rather than dereferencing null.  Typed wraps (e.g. worker.mojo's
# tls_worker_ptr) cast the raw address consumer-side so this module stays
# import-light (no cycle with worker.mojo).  current_task_addr / current_
# scope_addr are NOT exported here: those slots are reserved for the E-lanes
# (spec §22), which will add their accessors when they claim the slots.
#
# Mojo 1.0.0b2 (def-only): no module mutable globals, no static methods.
from mojito_async.integration.sys import BytePtr
from mojito_async.vendor.mojito_sys import NativeTlsKey, pthread_setspecific, tls_get


# ---------------------------------------------------------------------------
# NoConcurrencyRuntime — the spec §22 runtime-absent error model
# ---------------------------------------------------------------------------
# The named error model for TLS accessors invoked where no concurrency
# runtime is bound to the current OS worker.  The accessor raises Error with
# THIS struct's message (the repo's named-error convention, see
# task_control_block.IllegalTransitionError), so the documented "raise
# NoConcurrencyRuntime" is a REAL raised error type, not a string prefix.
# Prefer an explicit run()/pool in Phase 1 (spec §22 chooses the clear error
# over a process-root fallback).
struct NoConcurrencyRuntime:
    var message: String

    def __init__(out self, msg: String):
        self.message = msg


# ---------------------------------------------------------------------------
# Entry/exit bindings — driven by the worker trampoline (thread_entry.mojo);
# established per worker at entry, cleared at exit.  Non-raising (Bool = the
# pthread write returned rc 0) so the abi("C") trampoline can flag failures.
# ---------------------------------------------------------------------------

def bind_current_worker(key: NativeTlsKey, value: BytePtr) -> Bool:
    """Write THIS OS thread's current_worker slot (entry: the Worker cell).
    Coarse, entry-only — the pool trampoline calls this once, then never on
    the hot path (S2.4 scalability note).  True on a successful write."""
    return pthread_setspecific(key.raw(), value) == 0


def bind_current_task(key: NativeTlsKey, value: BytePtr) -> Bool:
    """Write THIS OS thread's current_task slot (spec §22; E-lanes populate
    at task boundaries; bound to the cleared sentinel at entry)."""
    return pthread_setspecific(key.raw(), value) == 0


def bind_current_scope(key: NativeTlsKey, value: BytePtr) -> Bool:
    """Write THIS OS thread's current_scope slot (spec §22; E-lanes
    populate; bound to the cleared sentinel at entry)."""
    return pthread_setspecific(key.raw(), value) == 0


def clear_worker_tls(cw: NativeTlsKey, ct: NativeTlsKey, cs: NativeTlsKey) -> Bool:
    """Clear all three slots at worker EXIT (spec §22/§69: the OS-worker-
    local pointers must not dangle past the trampoline).  b2 cannot build a
    NULL BytePtr (non-nullable), so the cleared value is the address-1
    sentinel (<=1 reads as "no runtime bound" in the accessors).  True only
    when ALL THREE writes succeeded."""
    var cleared = BytePtr(unsafe_from_address=1)
    if pthread_setspecific(cw.raw(), cleared) != 0:
        return False
    if pthread_setspecific(ct.raw(), cleared) != 0:
        return False
    if pthread_setspecific(cs.raw(), cleared) != 0:
        return False
    return True


# ---------------------------------------------------------------------------
# Accessor (spec §22) — read this OS thread's slot; NoConcurrencyRuntime on
# a null slot (no runtime bound / worker exited).  Returns the RAW address;
# typed casts happen at the consumer (worker.mojo's tls_worker_ptr ->
# UnsafePointer[Worker]).
# ---------------------------------------------------------------------------

def current_worker_addr(key: NativeTlsKey) raises -> BytePtr:
    """Raw address of this OS worker's current_worker slot value.  Raises
    the REAL NoConcurrencyRuntime error when the slot is absent (<= the
    address-1 cleared sentinel / no pool bound this thread / the worker has
    exited).  Drives probe the runtime-absent path with this (t35 asserts
    the raise on an unbound main thread)."""
    var p = tls_get(key)
    if Int(p) <= 1:
        var err = NoConcurrencyRuntime(
            "current_worker has no runtime bound (no concurrency runtime on "
            + "this OS thread — run your work inside a pool/run())"
        )
        raise Error(err.message)
    return p