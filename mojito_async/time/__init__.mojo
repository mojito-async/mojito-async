# mojito_async/time/__init__.mojo
#
# A1.4 timer lane (issue #36) — the time package surface.
#
# Re-exports the A1.1-folded Duration/Deadline data surface plus the A1.4
# timer subsystem: the monotonic min-heap (TimerHeap/TimerEntry), the
# virtual monotonic clock (MonotonicClock), the real timer-based
# sleep/sleep_until parks (sleep_current / sleep_until_current), the
# scheduler-loop servicing hook (service_timers / drive_step) and the §7.1
# surface functions (sleep / sleep_until).  Also re-exports the A6.1
# deadline-scoped timeout surface (issue #84): TimeoutScope/open_timeout
# (a child Scope + one armed deadline), the timeout_scope_driver expiry
# hook, refresh_timeout, and the TimeoutError predicate.
from mojito_async.time.clock import MonotonicClock
from mojito_async.time.deadline import Deadline, Duration, from_millis, from_now
from mojito_async.time.sleep import (
    sleep,
    sleep_current,
    sleep_until,
    sleep_until_current,
)
from mojito_async.time.timeout_scope import (
    TimeoutError,
    TimeoutRegistry,
    TimeoutScope,
    is_timeout_error,
    make_timeout_registry,
    open_timeout,
    refresh_timeout,
    timeout_drive_step,
    timeout_scope_driver,
)
from mojito_async.time.timer_heap import TimerEntry, TimerHeap
from mojito_async.time.timer_service import drive_step, service_timers
