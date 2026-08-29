# mojito_async/sync/__init__.mojo
#
# A1.2 (issue #34) — task-aware synchronization primitives over park/wake.
# Exports the high-level public surface: Mutex + MutexGuard (spec §34),
# Semaphore + Permit (spec §36), and RWLock + ReadGuard/WriteGuard (A4.8,
# issue #66 — writer-preference reader/writer exclusion over the same
# FIFO publish+park+handoff pattern).  The low-level park/wake seam stays
# in runtime.scheduler / task; this package exposes only user-facing
# primitives.
#
# These compose with the A1.1 single-worker cooperative scheduler: acquire/
# lock/read/write are dispatcher-level operations that park the current
# task on contention and grant it back via FIFO handoff.
from mojito_async.sync.mutex import Mutex, MutexGuard
from mojito_async.sync.rwlock import ReadGuard, RWLock, WriteGuard
from mojito_async.sync.semaphore import Permit, Semaphore