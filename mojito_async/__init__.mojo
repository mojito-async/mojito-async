# mojito-async/__init__.mojo
#
# A1.1 runtime (issue #33) — public package surface.
#
# Per spec §7.1 the ROOT package re-exports only high-level concepts.  A1.1
# ships the single-worker runtime surface: Scope, CancellationToken, spawn /
# run / create + Runtime, the scheduler primitives (yield_now,
# _suspend_current, resume_current, scheduler_loop), checkpoint, the Worker,
# and the Deadline / sleep API stubs (A1.4).  Names match the spike
# (TaskControlBlock, JoinHandle, Scope, Runtime, spawn, run, create) so
# sibling lanes and the A0 tests stay coherent; module paths move to
# mojito_async.*.
#
# Mutex/Semaphore/Channel/timer_heap are sibling lanes (A1.2-A1.4) and are
# deliberately NOT re-exported here.
from mojito_async.cancellation import (
    CancelFlag,
    CancellationError,
    CancellationToken,
    make_cancel_flag,
    make_child_flag,
)
from mojito_async.runtime.checkpoint import checkpoint
from mojito_async.runtime.runtime import Nil, Runtime, create, run
from mojito_async.runtime.scheduler import (
    SuspendReason,
    _suspend_current,
    resume_current,
    scheduler_loop,
    yield_now,
)
from mojito_async.runtime.task_control_block import (
    ResultValue,
    TaskControlBlock,
    WaitNode,
)
from mojito_async.runtime.worker import Worker, make_worker
from mojito_async.scope import (
    CancelHook,
    ChildrenStillLive,
    Scope,
    make_nested_scope,
    make_scope,
)
from mojito_async.task import (
    JoinHandle,
    abandon,
    claim_running,
    execute,
    park_commit,
    park_prepare,
    spawn,
    wake,
)
from mojito_async.time.deadline import Deadline
from mojito_async.time.sleep import sleep