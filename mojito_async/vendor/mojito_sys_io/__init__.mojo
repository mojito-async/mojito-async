# mojito_async/vendor/mojito_sys_io/__init__.mojo
#
# A7.1 reactor lane (issue #75) — vendored mojito-sys readiness/completion
# substrate package (S6.3/S6.4/S6.6; see vendor/mojito-sys/VENDORED_AT_S6.
# txt).  The C substrate lives under vendor/mojito-sys/{mjs_poller.c,
# mjs_epoll.c,mjs_iouring.c,include/mojito_sys.h}; this package is the Mojo
# type surface + extern-leaf firewall (errors.mojo, handle.mojo,
# poller.mojo, externs.mojo) plus the three concrete backend adapters
# (platform/{kqueue,epoll,iouring}.mojo).  reactor/poller.mojo is the ONE
# consumer that picks a backend and is otherwise extern-free.
from mojito_async.vendor.mojito_sys_io.errors import (
    ErrorDomain,
    SysError,
    raise_errno,
)
from mojito_async.vendor.mojito_sys_io.handle import (
    BorrowedFd,
    NativeIoHandle,
    OwnedFd,
)
from mojito_async.vendor.mojito_sys_io.poller import (
    EVENT_EOF,
    EVENT_ERROR,
    EVENT_READABLE,
    EVENT_WRITABLE,
    IoEvent,
    IoInterest,
)
