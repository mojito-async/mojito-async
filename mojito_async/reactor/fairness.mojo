# mojito_async/reactor/fairness.mojo
#
# A7.9 (issue #83) — the policy hook the reactor uses to feed readiness
# wakes into the M:N scheduler's fairness budget, so CPU-bound fibers do
# not starve I/O-bound fibers parked on the reactor.
#
# There is ALREADY a merged, tested fairness-budget worker loop:
# `runtime/scheduler.fair_scheduler_loop` (A2.7, issue #73 — closed,
# t36_fairness_aot.mojo) — after K consecutively locally-sourced task
# slices it runs a caller-supplied `service(mut Runtime, BytePtr) raises`
# callback BEFORE resuming local work (spec §21 "run at most K ready
# tasks then service reactor/timers"). That callback signature is FIXED
# by `fair_scheduler_loop`'s generic parameter `S`, so THIS module cannot
# ship one universal `service` function (every caller's `ud` scene is
# different, exactly like `time.timer_service.service_timers` composes
# into t36's own `service_sweep`, never a canned one) — it ships the
# REUSABLE PIECE a caller's own `service(rt, ud)` calls, plus the
# documented composition contract:
#
#   def my_service(mut rt: Runtime, ud: BytePtr) raises:
#       var sc = ud.bitcast[MyScene]()
#       _ = fair_service_io(rt, sc[].reactor[])         # THIS module
#       _ = service_io_deadlines[R](rt, sc[].heap[], sc[].clock[].now())
#       # or service_timers[R](...) for plain (non-I/O) sleeps
#
#   fair_scheduler_loop(rt, dispatcher, ud, my_service, budget_k=4, worker_id)
#
# `fair_service_io` (issue #83 point 2, "when the reactor enqueues N
# readiness wakes it contributes N units to the budget"): a NONBLOCKING
# reactor sweep (zero timeout) — the budget's service pass must never
# itself stall the worker waiting for I/O (that would defeat the K-slice
# fairness bound the same way a slow timer sweep would), so this wraps
# `reactor.reactor.service_io` with a FIXED `Duration(0)` policy rather
# than exposing a caller-chosen timeout. Returns the ready-op count
# (the "N units" the caller may fold into its own accounting, mirroring
# `service_timers`'s "woken" convention `drive_step` already sums).
#
# `yield_to_reactor` (issue #83 point 3, "a `yield_to_reactor` advisory
# alongside the E7 budget... no hard time-slice change, just fairness
# metadata the scheduler already honors"): a CPU fiber that wants to let
# a burst of I/O wakes drain sooner calls this instead of a bare
# `yield_now` — IDENTICAL mechanics (RUNNING -> PARKING -> RUNNABLE,
# re-enqueued onto this worker's local deque, `rt.note_yield()` resets
# the fair loop's kill-0 starve-watch streak exactly like any other
# cooperative handoff) — the "advisory" is naming/documentation, not a
# new scheduler primitive (issue #83 explicitly rules out a new one).
#
# Affinity (issue #83 point 4): NOT this module's concern — reactor wakes
# already enqueue via `unpark_current`'s owner-routed remote-ready push
# (runtime/park.mojo, spec §19.2), the SAME path every other cross-worker
# wake uses; `wake_target_worker` (runtime/scheduler.mojo, A2.5 issue #71)
# already resolves "only the parked owner's remote-ready queue, never a
# steal candidate" structurally. Nothing here needs to re-route it.
from mojito_async.reactor.reactor import Reactor, service_io
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.scheduler import yield_now
from mojito_async.runtime.task_control_block import ResultValue
from mojito_async.time.deadline import Duration


def fair_service_io(mut rt: Runtime, mut reactor: Reactor) raises -> Int:
    """The reactor's contribution to a `fair_scheduler_loop` budget pass
    (issue #83 point 2): a single NONBLOCKING poll (`Duration(0)` —
    never stalls the worker inside a fairness service pass) that wakes
    every op whose readiness already arrived. Returns the ready-op count,
    matching `time.timer_service.service_timers`'s "woken" return
    convention (`time.timer_service.drive_step` sums both the same way;
    a caller composing this alongside `service_io_deadlines` may do the
    same)."""
    return service_io(rt, reactor, Optional[Duration](Duration(0)))


def yield_to_reactor[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Advisory cooperative handoff (issue #83 point 3): a CPU-bound fiber
    that has run several slices calls this instead of a bare `yield_now`
    to signal "let the reactor's next budget-triggered service pass drain
    sooner" — mechanically IDENTICAL to `runtime.scheduler.yield_now`
    (no new scheduler primitive; the fairness budget's K-slice service
    pass already runs on the very next opportunity regardless of which
    name a caller used to yield). Naming this separately documents INTENT
    at call sites (spec §67's "cooperative" contract) without growing the
    scheduler's primitive set."""
    yield_now(rt, h)
