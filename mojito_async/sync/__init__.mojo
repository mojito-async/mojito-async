# mojito_async/sync/__init__.mojo
#
# A1.2 (issue #34) — task-aware synchronization primitives over park/wake.
# Exports the high-level public surface: Mutex + MutexGuard (spec §34),
# Semaphore + Permit (spec §36), RWLock + ReadGuard/WriteGuard (A4.8, issue
# #66 — writer-preference reader/writer exclusion over the same FIFO
# publish+park+handoff pattern), Condvar (A4.6, issue #60, spec §A5) and
# Barrier (A4.5, issue #59, spec Phase A5).  The low-level park/wake seam
# stays in runtime.scheduler / task; this package exposes only user-facing
# primitives.
#
# These compose with the A1.1 single-worker cooperative scheduler: acquire/
# lock/read/write are dispatcher-level operations that park the current
# task on contention and grant it back via FIFO handoff.  Condvar/Barrier
# additionally expose their own winner-cause constants (WINNER_READY/
# CANCELLED/TIMEOUT) so callers can interpret the `cause` cell threaded
# through `wait`.
from mojito_async.sync.mutex import Mutex, MutexGuard
from mojito_async.sync.rwlock import ReadGuard, RWLock, WriteGuard
from mojito_async.sync.semaphore import Permit, Semaphore
from mojito_async.sync.condvar import (
    Condvar,
    WINNER_CANCELLED,
    WINNER_READY,
    WINNER_TIMEOUT,
)
from mojito_async.sync.barrier import Barrier
