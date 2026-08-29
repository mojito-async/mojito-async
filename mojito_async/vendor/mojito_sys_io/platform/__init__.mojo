# mojito_async/vendor/mojito_sys_io/platform/__init__.mojo
#
# A7.1 reactor lane (issue #75) — the three concrete readiness/completion
# backend adapters (kqueue macOS/BSD, epoll Linux, io_uring experimental).
# reactor/poller.mojo's create_poller()/create_completion_poller() pick one
# at runtime via a comptime CompilationTarget branch (spec §27.1/§28,
# ADR-SYS-009); nothing else in this package makes that choice.
from mojito_async.vendor.mojito_sys_io.platform.kqueue import KqueuePoller
from mojito_async.vendor.mojito_sys_io.platform.epoll import EpollPoller
from mojito_async.vendor.mojito_sys_io.platform.iouring import (
    IoUringPoller,
    iouring_available,
    iouring_probe,
)
