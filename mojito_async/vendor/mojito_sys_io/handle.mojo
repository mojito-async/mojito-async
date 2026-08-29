# mojito_async/vendor/mojito_sys_io/handle.mojo
#
# A7.1 reactor lane (issue #75) — vendored verbatim from mojito-sys
# `mojito_sys/io/handle.mojo` @ commit 0bfb5291b698c833b302feb35c5724d44961f6d5
# (see vendor/mojito-sys/VENDORED_AT_S6.txt).  Zero edits beyond this header
# and the module path.
#
# Spec §27.1: the readiness interface consumes `NativeIoHandle` as the raw
# descriptor value type (`register(mut self, handle: NativeIoHandle, ...)`).
# Spec §25 ownership semantics for the io wrapper family: move transfers,
# borrow never closes, moved-from state detectable in debug.
#
# Scope: NativeIoHandle storage is Int32 — POSIX fd currency ONLY.  The
# Win64 HANDLE/SOCKET values are pointer-sized, not Int32, and belong to
# the §25 OwnedSocket/OwnedHandle family, not here.
#
# NativeIoHandle is deliberately NON-OWNING: it carries a raw Int32 fd
# value and closes NOTHING on destruction (no destructor at all).
# Ownership lives one layer up in the §25 family (OwnedFd/BorrowedFd); a
# poller that receives a NativeIoHandle borrows it for the duration of a
# registration and never closes it.
#
# Blocking behavior (SYS-5): every operation on NativeIoHandle is pure
# integer bookkeeping — no syscalls, never blocks.

# Sentinel: "no descriptor" / moved-from / invalid.
comptime NO_FD: Int32 = -1


# ---------------------------------------------------------------------------
# NativeIoHandle — raw Int32 POSIX fd value type (spec §27.1).
# ---------------------------------------------------------------------------
struct NativeIoHandle(Movable):
    """A raw, non-owning IO handle value: Int32 POSIX fd currency ONLY.

    Contract (spec §25/§27.1):
      - Move (`^`) TRANSFERS the token: the destination keeps the exact
        raw value; the source drops to the NO_FD sentinel.
      - borrow() yields another non-owning view of the same raw value;
        nothing in this type ever issues close(2), so a borrow NEVER
        closes the underlying descriptor.
      - The moved-from / default state is detectable in debug builds:
        is_valid() reports False and get() returns NO_FD (-1) after a
        move-out or on a default-constructed value.

    Blocking behavior (SYS-5): never blocks; no syscalls.
    """

    var fd: Int32

    def __init__(out self):
        self.fd = NO_FD

    def __init__(out self, fd_: Int32):
        self.fd = fd_


    def is_valid(self) -> Bool:
        return self.fd >= 0

    def get(self) -> Int32:
        return self.fd

    def borrow(self) -> Self:
        return Self(self.fd)

    def take(mut self) -> Self:
        var out = Self(self.fd)
        self.fd = NO_FD
        return out^


@extern("close")
def ms_close(fd: Int32) abi("C") -> Int32:
    ...


# ---------------------------------------------------------------------------
# OwnedFd — owns a POSIX file descriptor; move transfers, destroy closes
# exactly once.
# ---------------------------------------------------------------------------
struct OwnedFd(Movable):
    var fd: Int32
    var _disposed: Bool

    def __init__(out self, fd_: Int32):
        self.fd = fd_
        self._disposed = False

    def __init__(out self):
        self.fd = NO_FD
        self._disposed = True

    def is_null(self) -> Bool:
        return self.fd < 0

    def get(self) -> Int32:
        return self.fd

    def is_disposed(self) -> Bool:
        return self._disposed

    def borrow(self) -> BorrowedFd:
        return BorrowedFd(self.fd)

    def dispose(mut self) -> Int32:
        if self._disposed:
            return 0
        var rc = ms_close(self.fd)
        if rc == 0:
            self._disposed = True
            self.fd = NO_FD
        return rc

    def detach(mut self) -> Int32:
        var retained = self.fd
        self.fd = NO_FD
        self._disposed = True
        return retained

    def __del__(deinit self):
        _ = self.dispose()


# ---------------------------------------------------------------------------
# BorrowedFd — references a descriptor it does not own; never closes.
# ---------------------------------------------------------------------------
struct BorrowedFd:
    var fd: Int32

    def __init__(out self, fd_: Int32):
        self.fd = fd_

    def is_null(self) -> Bool:
        return self.fd < 0

    def get(self) -> Int32:
        return self.fd

    # Intentionally NO __del__: a borrow must never close the descriptor.
