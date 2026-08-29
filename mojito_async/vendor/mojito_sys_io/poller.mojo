# mojito_async/vendor/mojito_sys_io/poller.mojo
#
# A7.1 reactor lane (issue #75) — vendored verbatim from mojito-sys
# `mojito_sys/io/poller.mojo` @ commit 0bfb5291b698c833b302feb35c5724d44961f6d5
# (see vendor/mojito-sys/VENDORED_AT_S6.txt).  Zero edits beyond this header
# and the module path.
#
# IoInterest + IoEvent: the platform-neutral value types that cross the
# readiness-poller boundary. The bitmask spelling mirrors the frozen header
# (vendor/mojito-sys/include/mojito_sys.h, s6-poller block):
#   interests: READABLE | WRITABLE                      (registerable)
#   events:    READABLE | WRITABLE | EOF | ERROR        (EOF/ERROR event-only)
#
# Blocking behavior (SYS-5): pure value plumbing — no syscalls, never
# blocks, never allocates.
#
# b2 note: the flags are module-level comptime Ints (not struct members)
# because the harness and backends compare them against `events` fields
# directly; IoInterest carries the same values as a typed wrapper for the
# register/modify call boundary.

# Registerable interests (frozen header MJS_POLL_* spellings).
comptime INTEREST_READABLE = UInt32(0x1)
comptime INTEREST_WRITABLE = UInt32(0x2)

# Delivered-event flags. READABLE/WRITABLE reuse the interest bits; EOF
# (0x4) and ERROR (0x8) are EVENT-ONLY: backends reject them as interests.
comptime EVENT_READABLE = UInt32(0x1)
comptime EVENT_WRITABLE = UInt32(0x2)
comptime EVENT_EOF = UInt32(0x4)
comptime EVENT_ERROR = UInt32(0x8)


# Typed interests carrier for the register/modify call boundary:
#   poller.register(handle, IoInterest(IoInterest.READABLE), token)
struct IoInterest(ImplicitlyCopyable):
    """Registerable readiness interests (spec §27.1 `interests`).

    Pure value type; construction and inspection never block (SYS-5) and
    allocate nothing (SYS-4).
    """

    var bits: UInt32

    comptime READABLE = IoInterest(INTEREST_READABLE)
    comptime WRITABLE = IoInterest(INTEREST_WRITABLE)
    comptime BOTH = IoInterest(INTEREST_READABLE | INTEREST_WRITABLE)

    def __init__(out self, bits_: UInt32):
        self.bits = bits_

    def __copyinit__(out self, existing: Self):
        self.bits = existing.bits

    # True iff EVERY bit of `other` is present.
    def has(self, other: Self) -> Bool:
        return (self.bits & other.bits) == other.bits


# One delivered readiness event (mirrors C struct mjs_poll_event).
struct IoEvent(ImplicitlyCopyable):
    """A single readiness delivery from a native poller's wait().

    `token` is the EXACT opaque value handed to register()/modify() (§31
    MUST preserve accurately); `fd` is the ready descriptor;
    `events` is an OR of the EVENT_* flags above.

    Pure value type; never blocks (SYS-5), allocates nothing (SYS-4).
    Task-aware: no.
    """

    var token: UInt64
    var fd: Int32
    var events: UInt32

    def __init__(out self):
        self.token = 0
        self.fd = -1
        self.events = 0


    def is_readable(self) -> Bool:
        return (self.events & EVENT_READABLE) != 0

    def is_writable(self) -> Bool:
        return (self.events & EVENT_WRITABLE) != 0

    def is_eof(self) -> Bool:
        return (self.events & EVENT_EOF) != 0

    def is_error(self) -> Bool:
        return (self.events & EVENT_ERROR) != 0
