# mojito_async/sync/__init__.mojo
#
# A1.2 (issue #34) — task-aware synchronization primitives over park/wake.
# Exports the high-level public surface: Mutex + MutexGuard (spec §34) and
# Semaphore + Permit (spec §36).  The low-level park/wake seam stays in
# runtime.scheduler / task; this package exposes only user-facing primitives.
#
# These compose with the A1.1 single-worker cooperative scheduler: acquire/lock
# are dispatcher-level operations that park the current task on contention and
# grant it back via FIFO handoff.
from mojito_async.sync.mutex import Mutex, MutexGuard
from mojito_async.sync.semaphore import Permit, Semaphore