# mojito-async/__init__.mojo
#
# A1.1 runtime (issue #33) — public package surface.
#
# Per spec §7.1 the ROOT package re-exports only high-level concepts: Scope,
# JoinHandle, CancellationToken, CancelFlag, Deadline, sleep, yield_now,
# checkpoint, spawn, run, create, Runtime.  Lower-level machinery
# (TaskControlBlock, ResultValue, WaitNode, Nil, Worker, scheduler_loop,
# _suspend_current, resume_current, claim_running, park_prepare, park_commit,
# wake, abandon, execute, SuspendReason, make_* factories) lives at its
# submodule paths only (mojito_async.runtime.*, mojito_async.task,
# mojito_async.scope, mojito_async.cancellation) — import it from there.
#
# Mutex/Semaphore/Channel/timer_heap are sibling lanes (A1.2-A1.5) and are
# deliberately NOT built or re-exported here.
from mojito_async.cancellation import (
    CancelFlag,
    CancellationToken,
)
from mojito_async.runtime.checkpoint import checkpoint
from mojito_async.runtime.runtime import Runtime, create, run
from mojito_async.runtime.scheduler import yield_now
from mojito_async.scope import Scope
from mojito_async.task import JoinHandle, spawn
from mojito_async.time.deadline import Deadline
from mojito_async.time.sleep import sleep