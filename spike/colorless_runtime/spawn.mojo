# spike/colorless_runtime/spawn.mojo
#
# A0.6 (issue #15) — spawn + one-shot JoinHandle: TDD-RED SKELETON.
#
# This commit lands the compile surface the A0.6 drivers (t2/t6/t7) need:
# every behavioral entry point raises "not implemented (A0.6)" so drivers
# print RED (never FAIL).  The implementation lands in this same PR; see the
# module header there for the proven split between in-library cooperative
# execution and driver-side fiber orchestration (modular/modular#6971 keeps
# ms_ctx_* externs out of imported modules entirely).
from task import TaskControlBlock, ResultValue
from runtime import Runtime
from mojito_spike import BytePtr


struct JoinHandle[R: ResultValue](ImplicitlyCopyable, ImplicitlyDeletable):
    """One-shot handle to a spawned task's outcome (spec §9.1, INV-4).

    Move-only BY CONVENTION: b2 has no linear types; exactly one handle per
    spawned task drives join/abandon.
    """

    var _tcb: UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin]
    var _id: Int

    def __init__(out self, tcb: UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin], id: Int):
        self._tcb = tcb
        self._id = id

    def id(self) -> Int:
        return self._id

    def tcb(self) -> UnsafePointer[TaskControlBlock[Self.R], MutAnyOrigin]:
        return self._tcb

    def state(self) raises -> Int:
        raise Error("JoinHandle.state: not implemented (A0.6)")

    def is_completed(self) raises -> Bool:
        raise Error("JoinHandle.is_completed: not implemented (A0.6)")

    def is_failed(self) raises -> Bool:
        raise Error("JoinHandle.is_failed: not implemented (A0.6)")

    def error(self) raises -> String:
        raise Error("JoinHandle.error: not implemented (A0.6)")

    def begin_join(mut self) raises:
        raise Error("JoinHandle.begin_join: not implemented (A0.6)")

    def finish_join(mut self) raises -> Self.R:
        raise Error("JoinHandle.finish_join: not implemented (A0.6)")

    def join(mut self) raises -> Self.R:
        raise Error("JoinHandle.join: not implemented (A0.6)")

    def fail(mut self, msg: String) raises:
        _ = msg
        raise Error("JoinHandle.fail: not implemented (A0.6)")

    def mark_abandoned(mut self) raises:
        raise Error("JoinHandle.mark_abandoned: not implemented (A0.6)")


def spawn[R: ResultValue](
    mut rt: Runtime,
    tcb: UnsafePointer[TaskControlBlock[R], MutAnyOrigin],
    parent_id: Int,
) raises -> JoinHandle[R]:
    _ = rt
    _ = tcb
    _ = parent_id
    raise Error("spawn: not implemented (A0.6)")


def execute[R: ResultValue, F: def(BytePtr) raises -> R](
    rt: Runtime,
    mut h: JoinHandle[R],
    thunk: F,
    ud: BytePtr,
) raises:
    _ = rt
    _ = h
    _ = thunk
    _ = ud
    raise Error("execute: not implemented (A0.6)")


def claim_running[R: ResultValue](h: JoinHandle[R]) raises:
    _ = h
    raise Error("claim_running: not implemented (A0.6)")


def park_prepare[R: ResultValue](h: JoinHandle[R]) raises:
    _ = h
    raise Error("park_prepare: not implemented (A0.6)")


def park_commit[R: ResultValue](h: JoinHandle[R]) raises:
    _ = h
    raise Error("park_commit: not implemented (A0.6)")


def wake[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    _ = rt
    _ = h
    raise Error("wake: not implemented (A0.6)")


def abandon[R: ResultValue](mut h: JoinHandle[R]) raises:
    _ = h
    raise Error("abandon: not implemented (A0.6)")
