# mojito_async/vendor/mojito_sys_io/externs.mojo
#
# A7.1 reactor lane (issue #75) — vendored from mojito-sys
# `mojito_sys/io/externs.mojo` @ commit 0bfb5291b698c833b302feb35c5724d44961f6d5
# (see vendor/mojito-sys/VENDORED_AT_S6.txt), TRIMMED to the s6-poller /
# s6-epoll / s6-ioring sections only — the s6-socket (mjs_socket_*)
# bindings are a SEPARATE vendoring slice owned by the A7 connect/accept
# lane (issues #77/#78) and are deliberately NOT duplicated here.
#
# LEAF MODULE (b2 workaround precedent, mojito-sys #49): this file
# deliberately contains ONLY @extern declarations and the comptime pointer
# aliases they need — no imports, no structs, no raise sites. b2 1.0.0b2's
# cross-module lowering misbinds extern call arguments when the DECLARING
# module also hosts Movable structs and/or raising machinery. The proven
# shape is: pure-extern leaf + same-module probe_* shims + decode/raise
# only in the wrapper AFTER the call returns (mojito_sys_io/platform/
# kqueue.mojo, mojito_sys_io/platform/epoll.mojo, mojito_sys_io/platform/
# iouring.mojo).
#
# NEVER-INLINE INVARIANT: the probe_* shims below are the ONLY sanctioned
# call path into the mjs_poller_*/mjs_epoll_*/mjs_iouring_* bindings and
# MUST stay tiny, non-raising, aggregate-free, and free of @always_inline
# at every call site. These symbols are NOT for caller use; prefer the
# platform/{kqueue,epoll,iouring}.mojo wrappers.
#
# AGGREGATE RULE: no Mojo-side aggregate is ever READ inside an
# extern-reaching frame — the mjs_poll_event batch travels as an opaque
# byte buffer that callers fill/inspect with a bulk memcpy OUTSIDE the
# probe call (see platform/kqueue.mojo's wait()).

# Opaque byte-buffer pointer: raw event-batch payloads handed to/from C.
comptime ByteBuf = UnsafePointer[Byte, MutAnyOrigin]

# ---- s6-poller bindings (issue #75) ------------------------------------------
#
# SCALAR BOUNDARY NOTES (byval-poison class, b2):
#   - `interests` crosses as Int32 (bit-preserving reinterpretation of the
#     neutral UInt32 mask; a UInt32 scalar by value is poisoned);
#   - `cap` crosses as Int32 for the same reason;
#   - `token` is a plain UInt64 scalar (the socket lane proved 64-bit scalars
#     lower cleanly);
#   - out-slots are MutAnyOrigin pointers that pin post-call loads.

# Opaque mjs_poller* C handle, carried as an untyped byte pointer.
comptime PollerPtr = UnsafePointer[Byte, MutAnyOrigin]

# mjs_poller** create/close slots.
comptime PollerSlot = UnsafePointer[PollerPtr, MutAnyOrigin]

# const uint64_t* timeout slot: NULL = infinite, *slot == 0 = immediate.
comptime TimeoutSlot = UnsafePointer[UInt64, MutAnyOrigin]

# unsigned* wait-count out-slot.
comptime WaitCountSlot = UnsafePointer[UInt32, MutAnyOrigin]


@extern("mjs_poller_create")
def mjs_poller_create(out_slot: PollerSlot) abi("C") -> Int32:
    ...


@extern("mjs_poller_register")
def mjs_poller_register(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) abi("C") -> Int32:
    ...


@extern("mjs_poller_modify")
def mjs_poller_modify(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) abi("C") -> Int32:
    ...


@extern("mjs_poller_unregister")
def mjs_poller_unregister(p: PollerPtr, fd: Int32) abi("C") -> Int32:
    ...


@extern("mjs_poller_wait")
def mjs_poller_wait(
    p: PollerPtr,
    events: ByteBuf,
    cap: Int32,
    timeout_ns: TimeoutSlot,
    out_n: WaitCountSlot,
) abi("C") -> Int32:
    ...


@extern("mjs_poller_wake")
def mjs_poller_wake(p: PollerPtr) abi("C") -> Int32:
    ...


@extern("mjs_poller_close")
def mjs_poller_close(p: PollerSlot) abi("C") -> Int32:
    ...


def probe_poller_create(out_slot: PollerSlot) -> Int32:
    return mjs_poller_create(out_slot)


def probe_poller_register(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) -> Int32:
    return mjs_poller_register(p, fd, interests, token)


def probe_poller_modify(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) -> Int32:
    return mjs_poller_modify(p, fd, interests, token)


def probe_poller_unregister(p: PollerPtr, fd: Int32) -> Int32:
    return mjs_poller_unregister(p, fd)


def probe_poller_wait(
    p: PollerPtr,
    events: ByteBuf,
    cap: Int32,
    timeout_ns: TimeoutSlot,
    out_n: WaitCountSlot,
) -> Int32:
    return mjs_poller_wait(p, events, cap, timeout_ns, out_n)


def probe_poller_wake(p: PollerPtr) -> Int32:
    return mjs_poller_wake(p)


def probe_poller_close(p: PollerSlot) -> Int32:
    return mjs_poller_close(p)


# ---- s6-epoll bindings --------------------------------------------------------
#
# Same LEAF discipline as s6-poller above. EpollPoller reuses this leaf's
# PollerPtr/TimeoutSlot/WaitCountSlot aliases and the mjs_poll_event layout
# (same mojito_sys.h struct).

@extern("mjs_epoll_create")
def mjs_epoll_create(out_slot: PollerSlot) abi("C") -> Int32:
    ...


@extern("mjs_epoll_register")
def mjs_epoll_register(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) abi("C") -> Int32:
    ...


@extern("mjs_epoll_modify")
def mjs_epoll_modify(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) abi("C") -> Int32:
    ...


@extern("mjs_epoll_unregister")
def mjs_epoll_unregister(p: PollerPtr, fd: Int32) abi("C") -> Int32:
    ...


@extern("mjs_epoll_wait")
def mjs_epoll_wait(
    p: PollerPtr,
    events: ByteBuf,
    cap: Int32,
    timeout_ns: TimeoutSlot,
    out_n: WaitCountSlot,
) abi("C") -> Int32:
    ...


@extern("mjs_epoll_wake")
def mjs_epoll_wake(p: PollerPtr) abi("C") -> Int32:
    ...


@extern("mjs_epoll_close")
def mjs_epoll_close(p: PollerSlot) abi("C") -> Int32:
    ...


def probe_epoll_create(out_slot: PollerSlot) -> Int32:
    return mjs_epoll_create(out_slot)


def probe_epoll_register(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) -> Int32:
    return mjs_epoll_register(p, fd, interests, token)


def probe_epoll_modify(
    p: PollerPtr, fd: Int32, interests: Int32, token: UInt64
) -> Int32:
    return mjs_epoll_modify(p, fd, interests, token)


def probe_epoll_unregister(p: PollerPtr, fd: Int32) -> Int32:
    return mjs_epoll_unregister(p, fd)


def probe_epoll_wait(
    p: PollerPtr,
    events: ByteBuf,
    cap: Int32,
    timeout_ns: TimeoutSlot,
    out_n: WaitCountSlot,
) -> Int32:
    return mjs_epoll_wait(p, events, cap, timeout_ns, out_n)


def probe_epoll_wake(p: PollerPtr) -> Int32:
    return mjs_epoll_wake(p)


def probe_epoll_close(p: PollerSlot) -> Int32:
    return mjs_epoll_close(p)


# ---- s6-ioring bindings (experimental completion backend, ADR-SYS-009) -------
#
# Same LEAF discipline as s6-poller/s6-epoll above. The mjs_iouring_* ABI
# reuses the shared mjs_poll_event wire format from the s6-poller block, so
# wait() talks the same ByteBuf/TimeoutSlot/WaitCountSlot shape.  Linux-only
# and additionally gated by the MOJITO_IO_URING=1 capability flag inside
# mjs_iouring_create (native/posix/mjs_iouring.c): every host without both
# returns exactly -ENOSYS from every entry point that needs a live ring.

# Opaque mjs_uring* C handle, carried as an untyped byte pointer.
comptime UringPtr = UnsafePointer[Byte, MutAnyOrigin]

# mjs_uring** create/close slots.
comptime UringSlot = UnsafePointer[UringPtr, MutAnyOrigin]


@extern("mjs_iouring_probe")
def mjs_iouring_probe() abi("C") -> Int32:
    ...


@extern("mjs_iouring_available")
def mjs_iouring_available() abi("C") -> Int32:
    ...


@extern("mjs_iouring_create")
def mjs_iouring_create(out_slot: UringSlot) abi("C") -> Int32:
    ...


@extern("mjs_iouring_register")
def mjs_iouring_register(
    p: UringPtr, fd: Int32, interests: Int32, token: UInt64
) abi("C") -> Int32:
    ...


@extern("mjs_iouring_modify")
def mjs_iouring_modify(
    p: UringPtr, fd: Int32, interests: Int32, token: UInt64
) abi("C") -> Int32:
    ...


@extern("mjs_iouring_unregister")
def mjs_iouring_unregister(p: UringPtr, fd: Int32) abi("C") -> Int32:
    ...


@extern("mjs_iouring_wait")
def mjs_iouring_wait(
    p: UringPtr,
    events: ByteBuf,
    cap: Int32,
    timeout_ns: TimeoutSlot,
    out_n: WaitCountSlot,
) abi("C") -> Int32:
    ...


@extern("mjs_iouring_wake")
def mjs_iouring_wake(p: UringPtr) abi("C") -> Int32:
    ...


@extern("mjs_iouring_close")
def mjs_iouring_close(p: UringSlot) abi("C") -> Int32:
    ...


def probe_uring_probe() -> Int32:
    return mjs_iouring_probe()


def probe_uring_available() -> Int32:
    return mjs_iouring_available()


def probe_uring_create(out_slot: UringSlot) -> Int32:
    return mjs_iouring_create(out_slot)


def probe_uring_register(
    p: UringPtr, fd: Int32, interests: Int32, token: UInt64
) -> Int32:
    return mjs_iouring_register(p, fd, interests, token)


def probe_uring_modify(
    p: UringPtr, fd: Int32, interests: Int32, token: UInt64
) -> Int32:
    return mjs_iouring_modify(p, fd, interests, token)


def probe_uring_unregister(p: UringPtr, fd: Int32) -> Int32:
    return mjs_iouring_unregister(p, fd)


def probe_uring_wait(
    p: UringPtr,
    events: ByteBuf,
    cap: Int32,
    timeout_ns: TimeoutSlot,
    out_n: WaitCountSlot,
) -> Int32:
    return mjs_iouring_wait(p, events, cap, timeout_ns, out_n)


def probe_uring_wake(p: UringPtr) -> Int32:
    return mjs_iouring_wake(p)


def probe_uring_close(p: UringSlot) -> Int32:
    return mjs_iouring_close(p)
