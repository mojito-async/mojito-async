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
#
# A7.3/A7.4 (issues #77/#78) — `socket.mojo` (NativeSocket/SocketAddress/
# IoAttempt) added to this same vendor package (see
# vendor/mojito-sys/VENDORED_AT_S6_SOCKET.txt); the s6-socket externs
# section it needs was added to the already-vendored externs.mojo leaf.
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
from mojito_async.vendor.mojito_sys_io.socket import (
    IoAttempt,
    NativeSocket,
    SocketAddress,
)
