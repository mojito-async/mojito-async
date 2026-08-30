# mojito_async/vendor/mojito_sys_io/socket.mojo
#
# A7.3/A7.4 (issues #77/#78) — vendored from mojito-async/mojito-sys @
# commit a26e9a3550511738252174278af0794f6cbfd104
# (mojito_sys/io/socket.mojo), adapted ONLY at the import lines below
# (mojito_async.vendor.mojito_sys_io.* instead of the upstream
# mojito_sys.io.*/mojito_sys.abi.* namespace) plus TWO additions kept at
# the bottom of the `NativeSocket` struct: `set_reuseaddr()` (issue #78,
# SO_REUSEADDR for `bind_and_listen`) and `connect_error()` (issue #77,
# getsockopt(SO_ERROR) — the only way to decode a pending non-blocking
# `connect()`'s eventual outcome once write-readiness fires; the frozen
# ABI has no other way to express this). See
# mojito_async/vendor/mojito-sys/VENDORED_AT_S6_SOCKET.txt for the C-side
# half of this deviation (mjs_socket_set_reuseaddr/mjs_socket_connect_error
# in mjs_socket.c) and the matching extern additions in
# vendor/mojito_sys_io/externs.mojo. Everything else below is upstream
# verbatim.
#
# mojito-sys S6.2 — non-blocking NativeSocket + IoAttempt (issue #74).
#
# Spec §26 surface bound to the frozen C ABI (native/include/mojito_sys.h,
# s6-socket block):
#   NativeSocket.tcp_v4()            — create a TCPv4 socket;
#   set_nonblocking(mut, enabled)    — O_NONBLOCK read-modify-write;
#   bind(mut, address)               — bind(SocketAddress);
#   listen(mut, backlog)             — mark passive;
#   connect(mut, address) -> Bool    — True established / False pending
#                                      (-EINPROGRESS on a non-blocking
#                                      socket: a STATUS like try_lock's
#                                      -EBUSY; other errnos RAISE);
#   accept_nonblocking(mut)          — IoAttempt[NativeSocket];
#   recv_nonblocking(mut, buffer)    — IoAttempt[Int] over Span[UInt8];
#   send_nonblocking(mut, buffer)    — IoAttempt[Int] over Span[UInt8];
#   shutdown(mut, how)               — one/both halves;
#   close(mut)                       — exactly once; double close raises.
#
# IoAttempt distinguishes Ready / WouldBlock / Interrupted / Error / Closed
# exactly per spec L1363–1369. mojito-sys NEVER parks a task (L1371): no
# method here ever waits for readiness — WouldBlock returns to the caller
# immediately.
#
# DOCUMENTED b2 ADAPTATIONS:
#   - def-only members; construction via @staticmethod factories +
#     _adopt(), mirroring the s3 mutex / s2 thread lanes where the
#     extern-reaching path must stay non-raising until the rc is decoded.
#   - The spec's parameterized `IoAttempt[NativeSocket]` /
#     `IoAttempt[Int]` ships as ONE concrete carrier `IoAttempt` with an
#     Int count cell and an fd cell. b2 1.0.0b2 cannot lower generic
#     struct parameters over Movable payloads (struct values fail the
#     trait-conformance check at instantiation; verified by probe in this
#     lane). Accessors keep the two payload shapes distinct:
#     ready_count() for byte counts, take_ready_fd()/_adopt() for accepted
#     sockets. Semantics are identical to the spec spelling.
#   - Span parameters are spelled `Span[UInt8, _]` (unbound origin): b2
#     requires the origin parameter to be pinned or explicitly unbound,
#     and pinning it would leak the caller's allocation origin into the
#     API. The spec's `Span[UInt8]` surface is otherwise preserved
#     verbatim; recv writes through the span ONLY via the C layer.
#
# EINTR/EAGAIN doctrine (spec §38.11, exact — downstream reactors inherit
# this mapping):
#   - accept/recv/send NEVER auto-retry and NEVER wait: raw C -EINTR maps
#     to IoAttempt.Interrupted (caller retries), -EAGAIN/-EWOULDBLOCK maps
#     to WouldBlock (EWOULDBLOCK == EAGAIN on both hosts), every other
#     negative rc maps to Error carrying the POSITIVE errno.
#   - recv EOF (rc == 0, n == 0) maps to Closed — never Ready(0).
#   - send/recv partial transfers are normal: Ready(count) with
#     count < len(buffer); callers loop over the remainder. On Interrupted
#     assume NOTHING was transferred this attempt.
#   - One-shot entry points (create/set_nonblocking/bind/listen/connect/
#     shutdown/close) decode errnos by raising; they cannot be interrupted
#     mid-operation (see the header block contract in mojito_sys.h).
#
# b2 conventions (matching mojito_sys/sync/mutex.mojo):
#   - @extern bindings + probe shims live in the pure-extern leaf
#     mojito_sys/io/externs.mojo; this module decodes/raises only AFTER
#     each call has returned.
#   - Out-slots are UnsafePointer[..., MutAnyOrigin]; the 136-byte neutral
#     sockaddr travels as an opaque byte buffer filled/inspected with
#     SCALAR stores/loads only — no aggregate reads in extern-reaching
#     frames.

from std.memory import Span, UnsafePointer, stack_allocation
from std.sys import CompilationTarget

import mojito_async.vendor.mojito_sys_io.externs as _externs
from mojito_async.vendor.mojito_sys_io.errors import raise_errno

# Deterministic consumed-handle misuse code (frozen ABI: -errno). darwin
# AND Linux spell EINVAL 22 — host-portable.
comptime EINVAL_RC = Int32(-22)

# EINTR is 4 on BOTH hosts; EAGAIN differs (35 darwin / 11 Linux);
# EWOULDBLOCK == EAGAIN on both, so one spelling covers the pair.
comptime EINTR_RAW = Int32(4)

# Moved-from / closed descriptor sentinel.
comptime NO_FD = Int32(-1)


def _host_eagain() -> Int32:
    if CompilationTarget().is_macos():
        return Int32(35)
    return Int32(11)


def _host_af_inet6() -> Int32:
    # AF_INET6: 30 darwin / 10 Linux (C maps the neutral MJS_SOCK_INET6).
    if CompilationTarget().is_macos():
        return Int32(30)
    return Int32(10)


def _connect_pending_rc() -> Int32:
    # EINPROGRESS: 36 darwin / 115 Linux — the connect() pending STATUS.
    if CompilationTarget().is_macos():
        return Int32(36)
    return Int32(115)


def _host_enametoolong() -> Int32:
    # ENAMETOOLONG: 63 darwin / 36 Linux — overlong Unix path in to_buffer().
    if CompilationTarget().is_macos():
        return Int32(63)
    return Int32(36)


# ---- neutral sockaddr buffer geometry --------------------------------------
# Byte layout of struct mjs_sockaddr (frozen in the header): int32 family@0,
# uint16 port(host order)@4, pad@6, uint32 flowinfo@8, uint32 scope_id@12,
# octets[16]@16..31, path[104]@32..135.
comptime SOCKADDR_BYTES = 136
comptime SOCKADDR_WORDS = 17  # 136 / 8 Int64 cells for stack slots

comptime UNIX_PATH_MAX = 103  # portable floor: 104-byte sun_path incl. NUL

# Neutral address-family / shutdown constants re-exported from the frozen
# header (values match native/include/mojito_sys.h s6-socket block).
comptime MJS_SOCK_STREAM = Int32(1)
comptime MJS_SOCK_DGRAM = Int32(2)
comptime MJS_SOCK_INET = Int32(2)
comptime MJS_SOCK_UNIX = Int32(1)

comptime SHUT_READ = Int32(0)
comptime SHUT_WRITE = Int32(1)
comptime SHUT_BOTH = Int32(2)


def _buf_of(cell: UnsafePointer[Int64, MutAnyOrigin]) -> UnsafePointer[Byte, MutAnyOrigin]:
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(cell))


def _byte_buf(p: UnsafePointer[Byte, MutUntrackedOrigin]) -> UnsafePointer[Byte, MutAnyOrigin]:
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(p))


# ---- scalar byte helpers (no aggregates cross the ABI) ---------------------
def _ld8(p: UnsafePointer[Byte, MutAnyOrigin], off: Int) -> UInt8:
    return p[off]


def _st8(p: UnsafePointer[Byte, MutAnyOrigin], off: Int, v: UInt8):
    p[off] = v


def _st_i32(p: UnsafePointer[Byte, MutAnyOrigin], off: Int, v: Int32):
    var u = UInt32(v)
    _st8(p, off, UInt8(u & 0xFF))
    _st8(p, off + 1, UInt8((u >> 8) & 0xFF))
    _st8(p, off + 2, UInt8((u >> 16) & 0xFF))
    _st8(p, off + 3, UInt8((u >> 24) & 0xFF))


def _st_u16(p: UnsafePointer[Byte, MutAnyOrigin], off: Int, v: UInt16):
    _st8(p, off, UInt8(v & 0xFF))
    _st8(p, off + 1, UInt8((v >> 8) & 0xFF))


def _ld_u16(p: UnsafePointer[Byte, MutAnyOrigin], off: Int) -> UInt16:
    return UInt16(_ld8(p, off)) | (UInt16(_ld8(p, off + 1)) << 8)


def _st_u32(p: UnsafePointer[Byte, MutAnyOrigin], off: Int, v: UInt32):
    _st8(p, off, UInt8(v & 0xFF))
    _st8(p, off + 1, UInt8((v >> 8) & 0xFF))
    _st8(p, off + 2, UInt8((v >> 16) & 0xFF))
    _st8(p, off + 3, UInt8((v >> 24) & 0xFF))


def _ld_u32(p: UnsafePointer[Byte, MutAnyOrigin], off: Int) -> UInt32:
    return (
        UInt32(_ld8(p, off))
        | (UInt32(_ld8(p, off + 1)) << 8)
        | (UInt32(_ld8(p, off + 2)) << 16)
        | (UInt32(_ld8(p, off + 3)) << 24)
    )


# ---------------------------------------------------------------------------
# IoAttempt kind tags (spec L1363–1369). Plain comptime words: b2 has no
# enum sugar and generic payloads don't lower (see module docblock).
comptime ATTEMPT_READY = UInt8(0)
comptime ATTEMPT_WOULD_BLOCK = UInt8(1)
comptime ATTEMPT_INTERRUPTED = UInt8(2)
comptime ATTEMPT_ERROR = UInt8(3)
comptime ATTEMPT_CLOSED = UInt8(4)
# IoAttempt READY-payload flavor (panel fold — #76): the single carrier
# ships TWO distinct READY payload shapes, an accepted socket descriptor
# and a byte count. Cross-kind access must fail LOUD (decoded -EINVAL),
# never silently read the wrong cell as 0 / a bogus fd.
comptime PAYLOAD_FD = UInt8(0)     # READY payload is take_ready_fd()
comptime PAYLOAD_COUNT = UInt8(1)  # READY payload is ready_count()


# Classify a raw C rc from accept/recv/send into its IoAttempt kind tag.
# rc == 0 classifies READY here; recv refines a zero count to Closed via
# attempt_from_rc's eof_at_zero flag. Pure function; never raises; no
# syscalls (SYS-5); task-aware: no.
def classify_rc(rc: Int32) -> UInt8:
    if rc == 0:
        return ATTEMPT_READY
    if rc == -_host_eagain():
        return ATTEMPT_WOULD_BLOCK
    if rc == -EINTR_RAW:
        return ATTEMPT_INTERRUPTED
    return ATTEMPT_ERROR


# Mapping core shared by recv/send_nonblocking (and exercised directly by
# the conformance suite's fault-injection checks): build the attempt from
# the raw C results. eof_at_zero selects the recv flavor, where rc == 0
# with a zero count is Closed (EOF) rather than Ready(0).
# Blocking: none (SYS-5). Allocation: none (SYS-4). Task-aware: no.
def attempt_from_rc(rc: Int32, count: Int, eof_at_zero: Bool) -> IoAttempt:
    if rc == 0:
        if eof_at_zero and count == 0:
            return IoAttempt.closed()
        return IoAttempt.ready_count(count)
    var kind = classify_rc(rc)
    if kind == ATTEMPT_WOULD_BLOCK:
        return IoAttempt.would_block()
    if kind == ATTEMPT_INTERRUPTED:
        return IoAttempt.interrupted()
    return IoAttempt.error(rc)


struct IoAttempt(Movable):
    """Outcome of one non-blocking IO attempt (spec L1360–1371).

    Exactly one of five kinds:
      Ready(value)      — completed THIS attempt (byte count or adopted fd);
      WouldBlock        — not ready now (EAGAIN/EWOULDBLOCK): re-poll later;
      Interrupted       — interrupted (EINTR): retry immediately;
      Error             — failed: errno_code() carries the POSITIVE errno;
      Closed            — orderly EOF (recv only).

    Never parks: constructing or inspecting an attempt performs no syscall
    and never waits (SYS-5). Allocation: none (SYS-4). Task-aware: no.
    """

    var kind: UInt8
    var err: Int32    # positive errno when kind == ERROR, else 0
    var count: Int    # READY byte-count payload (recv/send)
    var fd: Int32     # READY accepted-descriptor payload; NO_FD after take
    var payload: UInt8  # READY payload flavor: PAYLOAD_FD or PAYLOAD_COUNT

    def __init__(out self):
        self.kind = ATTEMPT_ERROR
        self.err = 0
        self.count = 0
        self.fd = NO_FD
        self.payload = PAYLOAD_FD

    @staticmethod
    def ready_count(n: Int) -> IoAttempt:
        var a = IoAttempt()
        a.kind = ATTEMPT_READY
        a.payload = PAYLOAD_COUNT
        a.count = n
        return a^

    @staticmethod
    def ready_fd(accepted_fd: Int32) -> IoAttempt:
        var a = IoAttempt()
        a.kind = ATTEMPT_READY
        a.payload = PAYLOAD_FD
        a.fd = accepted_fd
        return a^

    @staticmethod
    def would_block() -> IoAttempt:
        var a = IoAttempt()
        a.kind = ATTEMPT_WOULD_BLOCK
        return a^

    @staticmethod
    def interrupted() -> IoAttempt:
        var a = IoAttempt()
        a.kind = ATTEMPT_INTERRUPTED
        return a^

    @staticmethod
    def error(raw_rc: Int32) -> IoAttempt:
        # raw_rc arrives as the frozen-contract negative errno.
        var a = IoAttempt()
        a.kind = ATTEMPT_ERROR
        a.err = -raw_rc
        return a^

    @staticmethod
    def closed() -> IoAttempt:
        var a = IoAttempt()
        a.kind = ATTEMPT_CLOSED
        return a^

    def is_ready(self) -> Bool:
        return self.kind == ATTEMPT_READY

    def is_would_block(self) -> Bool:
        return self.kind == ATTEMPT_WOULD_BLOCK

    def is_interrupted(self) -> Bool:
        return self.kind == ATTEMPT_INTERRUPTED

    def is_error(self) -> Bool:
        return self.kind == ATTEMPT_ERROR

    def is_closed(self) -> Bool:
        return self.kind == ATTEMPT_CLOSED

    # The positive errno carried by an Error attempt (0 otherwise).
    def errno_code(self) -> Int32:
        return self.err

    # The READY byte-count payload (recv/send attempts).
    #
    # Cross-kind misuse is LOUD (panel fold — #76): reading a count off a
    # socket-fd (or any non-ready) attempt raises decoded -EINVAL instead
    # of silently returning 0.
    def ready_count(self) raises -> Int:
        if (not self.is_ready()) or (self.payload != PAYLOAD_COUNT):
            raise_errno(EINVAL_RC)
        return self.count

    # Consume the READY accepted-descriptor payload (accept attempts).
    # Single-take: the fd cell resets to NO_FD afterwards.
    #
    # Cross-kind misuse is LOUD (panel fold — #76): taking an fd off a
    # count (or any non-ready) attempt raises decoded -EINVAL instead of
    # silently returning NO_FD.
    def take_ready_fd(mut self) raises -> Int32:
        if (not self.is_ready()) or (self.payload != PAYLOAD_FD):
            raise_errno(EINVAL_RC)
        var taken = self.fd
        self.fd = NO_FD
        return taken


# ---------------------------------------------------------------------------
# SocketAddress — platform-neutral value mirroring struct mjs_sockaddr
# (fixed 136-byte layout; see header block). Port is HOST byte order;
# conversion to real OS sockaddr storage happens inside the C layer.
#
# Blocking: none (SYS-5) — pure value type plus scalar buffer math.
# Allocation: parse/format allocate scratch cells on the stack and one
# result String (format only). Task-aware: no.
struct SocketAddress(Movable):
    var family: Int32       # MJS_SOCK_INET / host AF_INET6 value / MJS_SOCK_UNIX
    var port: Int32         # host byte order
    var flowinfo: UInt32    # IPv6 only
    var scope_id: UInt32    # IPv6 only
    var w0: UInt32          # octets packed big-endian: octets 4k..4k+3 per word
    var w1: UInt32
    var w2: UInt32
    var w3: UInt32
    var path: String        # unix family only

    def __init__(out self):
        self.family = 0
        self.port = 0
        self.flowinfo = 0
        self.scope_id = 0
        self.w0 = 0
        self.w1 = 0
        self.w2 = 0
        self.w3 = 0
        self.path = String("")

    def __moveinit__(mut self, mut existing: Self):
        self.family = existing.family
        self.port = existing.port
        self.flowinfo = existing.flowinfo
        self.scope_id = existing.scope_id
        self.w0 = existing.w0
        self.w1 = existing.w1
        self.w2 = existing.w2
        self.w3 = existing.w3
        self.path = existing.path^

    # The k-th big-endian-packed octet word (k in [0,4)).
    def word(self, k: Int) -> UInt32:
        if k == 0:
            return self.w0
        if k == 1:
            return self.w1
        if k == 2:
            return self.w2
        return self.w3

    # The k-th address octet (k in [0,16)).
    def octet(self, k: Int) -> UInt8:
        var w = self.word(k // 4)
        var shift = 24 - 8 * (k % 4)
        return UInt8((w >> shift) & 0xFF)

    @staticmethod
    def ipv4(
        o0: UInt8, o1: UInt8, o2: UInt8, o3: UInt8, port_: Int32
    ) -> SocketAddress:
        var a = SocketAddress()
        a.family = MJS_SOCK_INET
        a.port = port_
        a.w0 = (
            (UInt32(o0) << 24)
            | (UInt32(o1) << 16)
            | (UInt32(o2) << 8)
            | UInt32(o3)
        )
        return a^

    @staticmethod
    def ipv6_from_words(
        v0: UInt32, v1: UInt32, v2: UInt32, v3: UInt32, port_: Int32
    ) -> SocketAddress:
        var a = SocketAddress()
        a.family = _host_af_inet6()
        a.port = port_
        a.w0 = v0
        a.w1 = v1
        a.w2 = v2
        a.w3 = v3
        return a^

    @staticmethod
    def ipv6_loopback(port_: Int32) -> SocketAddress:
        # ::1
        return SocketAddress.ipv6_from_words(
            UInt32(0), UInt32(0), UInt32(0), UInt32(1), port_
        )

    @staticmethod
    def unix(path_: String) -> SocketAddress:
        var a = SocketAddress()
        a.family = MJS_SOCK_UNIX
        var copied = String("")
        var src = path_.unsafe_ptr()
        var sbp = UnsafePointer[Byte, MutUntrackedOrigin](
            unsafe_from_address=Int(src)
        )
        var i = 0
        while i < len(path_):
            copied += chr(Int(sbp[i]))
            i += 1
        a.path = copied^
        return a^

    # Parse dotted-quad IPv4 text (delegates to mjs_sockaddr_ipv4 so libc
    # inet_pton semantics are THE definition of "well-formed").
    #
    # Raises (decoded errno): -EFAULT/-EINVAL per the frozen contract.
    # Blocking: none (SYS-5). Allocation: stack scratch cells only.
    # Task-aware: no.
    # parse_ipv4 / format_dotted_ipv4 live at MODULE level below
    # (socket_address_parse_ipv4 / socket_format_ipv4): b2 crashes when a
    # struct-member body mixes helper calls with an extern shim call in an
    # extern-importing module hosting Movable structs (verified by
    # bisection; see module docblock ADAPTATIONS).

    # Format the IPv4 address as dotted-quad text (mjs_sockaddr_format4,
    # i.e. libc inet_ntop semantics).
    #
    # Raises (decoded errno): -EFAULT/-EINVAL per the frozen contract.
    # Blocking: none (SYS-5). Allocation: one result String (diagnostic-
    # grade path, not hot). Task-aware: no.
    # Serialize into a 136-byte neutral buffer (scalar stores ONLY — this
    # pointer becomes the opaque const mjs_sockaddr* across the ABI).
    def to_buffer(self, cell: UnsafePointer[Int64, MutAnyOrigin]) raises:
        var p = _buf_of(cell)
        var z = 0
        while z < SOCKADDR_BYTES:
            p[z] = Byte(0)
            z += 1
        _st_i32(p, 0, self.family)
        # Port is a 16-bit field: reject out-of-range up front (decoded
        # -EINVAL) rather than silently wrapping through UInt16() (panel
        # fold — #76).
        if self.port < 0 or self.port > 0xFFFF:
            raise_errno(EINVAL_RC)
        _st_u16(p, 4, UInt16(self.port))
        _st_u32(p, 8, self.flowinfo)
        _st_u32(p, 12, self.scope_id)
        # Octet words travel BIG-ENDIAN on the wire layout: octet k of the
        # address lands at buffer byte 16+k (C memcpy's [0..3]/[0..15] into
        # sin_addr/sin6_addr, which are network byte order in memory).
        var k = 0
        while k < 4:
            var word = self.word(k)
            var base = 16 + k * 4
            _st8(p, base, UInt8((word >> 24) & 0xFF))
            _st8(p, base + 1, UInt8((word >> 16) & 0xFF))
            _st8(p, base + 2, UInt8((word >> 8) & 0xFF))
            _st8(p, base + 3, UInt8(word & 0xFF))
            k += 1
        if self.family == MJS_SOCK_UNIX:
            var src = self.path.unsafe_ptr()
            var sbp = UnsafePointer[Byte, MutUntrackedOrigin](
                unsafe_from_address=Int(src)
            )
            var n = len(self.path)
            # Panel fold — #76: an overlong Unix path must FAIL LOUD (decoded
            # -ENAMETOOLONG) rather than silently truncate into the buffer.
            if n > UNIX_PATH_MAX:
                raise_errno(-_host_enametoolong())
            var i = 0
            while i < n:
                _st8(p, 32 + i, sbp[i])
                i += 1
            # buf pre-zeroed: the NUL terminator is already in place.

    @staticmethod
    def from_buffer(cell: UnsafePointer[Int64, MutAnyOrigin]) -> SocketAddress:
        return _sockaddr_from_buffer(cell)



# ---------------------------------------------------------------------------
# NativeSocket — non-blocking-capable POSIX socket wrapper (SYS-3 opaque fd).
#
# Ownership (spec §25 family semantics):
#   - move (^) TRANSFERS the descriptor token; the source drops inert;
#   - close()/the destructor release the descriptor EXACTLY ONCE; any
#     further use raises decoded -EINVAL deterministically without
#     re-entering C (double-close prevention);
#   - accepted children arrive through IoAttempt.take_ready_fd() and are
#     owned by the caller (adopt them with NativeSocket._adopt(fd)).
#
# Non-blocking doctrine: NOTHING here parks a task (spec L1371). Whether an
# individual operation can wait for readiness is decided solely by the
# O_NONBLOCK flag the caller installs via set_nonblocking(); the wrapper
# performs single syscall attempts and reports WouldBlock/Interrupted
# instead of waiting. See per-method SYS-5 notes.
struct NativeSocket(Movable):
    var fd: Int32
    var closed: Bool

    def __init__(out self):
        self.fd = NO_FD
        self.closed = True

    def __moveinit__(mut self, mut existing: Self):
        self.fd = existing.fd
        self.closed = existing.closed
        existing.fd = NO_FD
        existing.closed = True

    def is_valid(self) -> Bool:
        return (not self.closed) and (self.fd >= 0)

    def is_closed(self) -> Bool:
        return self.closed

    # The raw descriptor, or the -1 sentinel once closed/moved-from.
    def get(self) -> Int32:
        return self.fd

    # Adopt an owned descriptor minted by the C layer (accept results).
    # Public because conformance tests pin the adoption path; treat as
    # internal plumbing otherwise.
    @staticmethod
    def _adopt(owned_fd: Int32) -> NativeSocket:
        var s = NativeSocket()
        s.fd = owned_fd
        s.closed = False
        return s^

    # Guard shared by every live-handle method: deterministic misuse code,
    # raised WITHOUT crossing back into C.
    def _require_live(mut self) raises:
        if self.closed:
            raise_errno(EINVAL_RC)

    # Create a TCP/IPv4 stream socket.
    #
    # Raises (decoded errno): -EFAULT/-EINVAL/-EMFILE... per the contract.
    #
    # Blocking: no (SYS-5) — socket(2) does not wait for readiness.
    # Allocation: none beyond one kernel descriptor (SYS-4).
    # Task-aware: no.
    @staticmethod
    def tcp_v4() raises -> NativeSocket:
        var slot = stack_allocation[1, Int32]()
        var rc = _externs.probe_socket(MJS_SOCK_INET, MJS_SOCK_STREAM, slot)
        if rc != 0:
            raise_errno(rc)
        return NativeSocket._adopt(slot[0])

    # Create a TCP/IPv6 stream socket where the host supports IPv6.
    # Same contract as tcp_v4(); unsupported hosts surface their own
    # decoded errno (e.g. -EAFNOSUPPORT).
    #
    # Blocking: no (SYS-5). Allocation: none beyond one kernel descriptor.
    # Task-aware: no.
    @staticmethod
    def tcp_v6() raises -> NativeSocket:
        var slot = stack_allocation[1, Int32]()
        var rc = _externs.probe_socket(_host_af_inet6(), MJS_SOCK_STREAM, slot)
        if rc != 0:
            raise_errno(rc)
        return NativeSocket._adopt(slot[0])

    # Create an AF_UNIX stream socket.
    #
    # Blocking: no (SYS-5). Allocation: none beyond one kernel descriptor.
    # Task-aware: no.
    @staticmethod
    def unix_stream() raises -> NativeSocket:
        var slot = stack_allocation[1, Int32]()
        var rc = _externs.probe_socket(MJS_SOCK_UNIX, MJS_SOCK_STREAM, slot)
        if rc != 0:
            raise_errno(rc)
        return NativeSocket._adopt(slot[0])

    # Set/clear O_NONBLOCK on this socket (read-modify-write of F_GETFL).
    #
    # Raises (decoded errno): -EINVAL on a consumed/moved-from handle; any
    # unexpected negative rc from the C layer.
    #
    # Blocking: no (SYS-5) — fcntl F_GETFL/F_SETFL never wait for readiness.
    # Allocation: none (SYS-4).
    # Task-aware: no.
    def set_nonblocking(mut self, enabled: Bool) raises:
        self._require_live()
        var want: Int32 = 0
        if enabled:
            want = 1
        var rc = _externs.probe_set_nonblocking(self.fd, want)
        if rc != 0:
            raise_errno(rc)

    # Bind to a platform-neutral address (port in HOST byte order).
    #
    # Raises (decoded errno): -EINVAL misuse; -EFAULT/-EINVAL/
    # -ENAMETOOLONG/-EADDRINUSE/-EACCES from bind(2), decoded.
    #
    # Blocking: no (SYS-5).
    # Allocation: one 136-byte stack scratch cell (SYS-4 net zero).
    # Task-aware: no.
    def bind(mut self, address: SocketAddress) raises:
        self._require_live()
        var cell = stack_allocation[SOCKADDR_WORDS, Int64]()
        address.to_buffer(cell)
        var rc = _externs.probe_bind(self.fd, _buf_of(cell))
        if rc != 0:
            raise_errno(rc)

    # Mark the socket passive with `backlog` pending connections.
    #
    # Raises (decoded errno): -EINVAL misuse/negative backlog; listen(2)
    # errors pass through decoded.
    #
    # Blocking: no (SYS-5). Allocation: none (SYS-4). Task-aware: no.
    def listen(mut self, backlog: Int) raises:
        self._require_live()
        if backlog < 0:
            raise_errno(EINVAL_RC)
        var rc = _externs.probe_listen(self.fd, Int32(backlog))
        if rc != 0:
            raise_errno(rc)

    # Initiate a connection to `address`.
    #
    # Returns True when ESTABLISHED; False when the attempt is PENDING
    # (non-blocking socket, C reported -EINPROGRESS — a STATUS, not a
    # failure; await writability to learn the outcome). Every other errno
    # RAISES decoded (-ECONNREFUSED, -ECONNRESET, -EINVAL misuse, ...).
    #
    # Blocking (SYS-5): on a BLOCKING socket connect(2) blocks until the
    #   handshake settles — call set_nonblocking(True) first to guarantee
    #   non-parking behavior.
    # Allocation: one 136-byte stack scratch cell (SYS-4 net zero).
    # Task-aware: no — never yields to the mojito scheduler.
    def connect(mut self, address: SocketAddress) raises -> Bool:
        self._require_live()
        var cell = stack_allocation[SOCKADDR_WORDS, Int64]()
        address.to_buffer(cell)
        var rc = _externs.probe_connect(self.fd, _buf_of(cell))
        if rc == 0:
            return True
        if rc == -_connect_pending_rc():
            return False
        raise_errno(rc)
        # Unreachable: raise_errno always raises; b2 still demands a
        # return on every path of a result-bearing def.
        return False

    # Extract ONE pending connection WITHOUT blocking (spec L1344).
    #
    # Returns Ready (adopt the child from take_ready_fd()) when a peer was
    # queued; WouldBlock when none is pending (-EAGAIN); Interrupted on
    # -EINTR (retry; nothing was accepted); Error(+errno_code()) otherwise.
    # The accepted child inherits this listener's O_NONBLOCK state
    # deterministically (enforced in C).
    #
    # Blocking: no (SYS-5) — single non-blocking accept attempt; NEVER
    #   waits, NEVER parks (spec L1371).
    # Allocation: two stack scratch cells (SYS-4 net zero).
    # Task-aware: no.
    def accept_nonblocking(mut self) raises -> IoAttempt:
        self._require_live()
        var child_slot = stack_allocation[1, Int32]()
        var peer_cell = stack_allocation[SOCKADDR_WORDS, Int64]()
        var rc = _externs.probe_accept(self.fd, child_slot, _buf_of(peer_cell))
        if rc == 0:
            return IoAttempt.ready_fd(child_slot[0])
        var kind = classify_rc(rc)
        if kind == ATTEMPT_WOULD_BLOCK:
            return IoAttempt.would_block()
        if kind == ATTEMPT_INTERRUPTED:
            return IoAttempt.interrupted()
        return IoAttempt.error(rc)

    # ONE non-blocking receive attempt into `buffer` (spec L1347–1351).
    #
    # Returns Ready(n) with the PARTIAL count actually received (n <=
    # len(buffer)); Closed on orderly EOF (never Ready(0)); WouldBlock when
    # no data is buffered; Interrupted on EINTR (assume nothing received);
    # Error(+errno_code()) otherwise (e.g. ECONNRESET after peer reset).
    #
    # Blocking: no (SYS-5) — a single recv(2) attempt that never waits;
    #   parking is impossible regardless of the O_NONBLOCK flag.
    # Allocation: two stack scratch cells (SYS-4 net zero).
    # Task-aware: no.
    def recv_nonblocking(mut self, buffer: Span[UInt8, _]) raises -> IoAttempt:
        self._require_live()
        var n_bytes = len(buffer)
        if n_bytes == 0:
            raise_errno(EINVAL_RC)
        var n_slot = stack_allocation[1, UInt64]()
        var bp = UnsafePointer[Byte, MutAnyOrigin](
            unsafe_from_address=Int(buffer.unsafe_ptr())
        )
        var rc = _externs.probe_recv(self.fd, bp, UInt64(n_bytes), n_slot)
        return attempt_from_rc(rc, Int(n_slot[0]), True)

    # ONE non-blocking send attempt from `buffer`.
    #
    # Returns Ready(n) with the count the kernel ACCEPTED — possibly a
    # PARTIAL prefix of len(buffer); callers loop over the remainder.
    # WouldBlock when the send buffer is full; Interrupted on EINTR (assume
    # nothing sent); Error(+errno_code()) otherwise.
    #
    # Blocking: no (SYS-5) — a single send(2) attempt that never waits.
    # Allocation: two stack scratch cells (SYS-4 net zero).
    # Task-aware: no.
    def send_nonblocking(mut self, buffer: Span[UInt8, _]) raises -> IoAttempt:
        self._require_live()
        var n_bytes = len(buffer)
        if n_bytes == 0:
            raise_errno(EINVAL_RC)
        var n_slot = stack_allocation[1, UInt64]()
        var bp = UnsafePointer[Byte, MutAnyOrigin](
            unsafe_from_address=Int(buffer.unsafe_ptr())
        )
        var rc = _externs.probe_send(self.fd, bp, UInt64(n_bytes), n_slot)
        return attempt_from_rc(rc, Int(n_slot[0]), False)

    # Shut down transfer halves: SHUT_READ / SHUT_WRITE / SHUT_BOTH.
    #
    # Raises (decoded errno): -EINVAL misuse or bad `how`; shutdown(2)
    # errors pass through decoded.
    #
    # Blocking: no (SYS-5). Allocation: none (SYS-4). Task-aware: no.
    def shutdown(mut self, how: Int32) raises:
        self._require_live()
        var rc = _externs.probe_shutdown(self.fd, how)
        if rc != 0:
            raise_errno(rc)

    # Close the descriptor EXACTLY ONCE. Any later operation on this value
    # (including a second close()) raises decoded -EINVAL deterministically
    # WITHOUT re-entering C — double-close prevention. Per the frozen ABI,
    # ANY close(2) return is final (darwin releases the descriptor even
    # when reporting EINTR), so the consume-first order here is deliberate:
    # the wrapper never retries close.
    #
    # Raises (decoded errno): -EINVAL on double close/misuse; close(2)
    # errors pass through decoded — the socket is STILL consumed.
    # ---- issue #78 deviation: SO_REUSEADDR ------------------------------
    #
    # Set SO_REUSEADDR so a restarted listener can rebind a recently-used
    # port immediately (`bind_and_listen`'s deliverable). Not part of the
    # frozen ABI's original s6-socket block; added alongside connect_error
    # below (see module docblock).
    #
    # Blocking: no (SYS-5). Allocation: none (SYS-4). Task-aware: no.
    def set_reuseaddr(mut self, enabled: Bool) raises:
        self._require_live()
        var want: Int32 = 0
        if enabled:
            want = 1
        var rc = _externs.probe_set_reuseaddr(self.fd, want)
        if rc != 0:
            raise_errno(rc)

    # ---- issue #77 deviation: getsockopt(SO_ERROR) ----------------------
    #
    # Decode a PENDING non-blocking connect()'s eventual outcome once the
    # reactor delivers write-readiness (A7.3 connect_current's redrive
    # step). Returns True when the connection is ESTABLISHED (SO_ERROR ==
    # 0); raises the decoded errno otherwise (e.g. -ECONNREFUSED). Not
    # expressible via the existing frozen entry points (see module
    # docblock) — mjs_socket_connect_error is a documented deviation.
    #
    # Blocking: no (SYS-5) — getsockopt(2) never waits for readiness.
    # Allocation: none (SYS-4). Task-aware: no.
    def connect_error(mut self) raises -> Bool:
        self._require_live()
        var slot = stack_allocation[1, Int32]()
        var rc = _externs.probe_connect_error(self.fd, slot)
        if rc != 0:
            raise_errno(rc)
        var so_err = slot[0]
        if so_err == 0:
            return True
        raise_errno(-so_err)
        # Unreachable: raise_errno always raises; b2 still demands a
        # return on every path of a result-bearing def.
        return False

    # ---- issue #182 deviation: SO_SNDBUF / SO_RCVBUF --------------------
    #
    # Shrink this socket's kernel send/receive buffer to a known-small size
    # instead of relying on the platform's auto-tuned (host- and
    # run-to-run-variable) default. Not part of the frozen ABI's original
    # s6-socket block; added alongside set_reuseaddr/connect_error above
    # (see module docblock) for t44_tcp_read_write_aot scenario B, which
    # needs a 512KB write_all over loopback to GUARANTEE at least one
    # EAGAIN/EWOULDBLOCK rather than depend on the platform default being
    # small enough.
    #
    # Blocking: no (SYS-5). Allocation: none (SYS-4). Task-aware: no.
    def set_send_buffer_size(mut self, bytes: Int32) raises:
        self._require_live()
        var rc = _externs.probe_set_sndbuf(self.fd, bytes)
        if rc != 0:
            raise_errno(rc)

    def set_recv_buffer_size(mut self, bytes: Int32) raises:
        self._require_live()
        var rc = _externs.probe_set_rcvbuf(self.fd, bytes)
        if rc != 0:
            raise_errno(rc)

    # Blocking (SYS-5): close(2) may block inside the kernel only under
    def close(mut self) raises:
        self._require_live()
        var target = self.fd
        self.fd = NO_FD
        self.closed = True  # consumed REGARDLESS of rc: never re-close
        var rc = _externs.probe_close(target)
        if rc != 0:
            raise_errno(rc)

    # Destructor: best-effort release exactly once (skipped for
    # closed/moved-from values). Status deliberately unused here; callers
    # wanting the status must call close().
    #
    # Blocking (SYS-5): see close().
    def __del__(deinit self):
        if (not self.closed) and (self.fd >= 0):
            _ = _externs.probe_close(self.fd)


# Module-level neutral-buffer decoder (b2 WORKAROUND: String accumulation
# through a loop/branch merge crashes the compiler when lowered as a
# Movable-struct member in an extern-importing module — the errors.mojo
# runtime-string precedent lowers cleanly at MODULE level).
def _sockaddr_from_buffer(
    cell: UnsafePointer[Int64, MutAnyOrigin],
) -> SocketAddress:
    var p = _buf_of(cell)
    var a = SocketAddress()
    a.family = Int32(_ld_u32(p, 0))
    a.port = Int32(_ld_u16(p, 4))
    a.flowinfo = _ld_u32(p, 8)
    a.scope_id = _ld_u32(p, 12)
    var k = 0
    while k < 4:
        var base = 16 + k * 4
        var word = (
            (UInt32(_ld8(p, base)) << 24)
            | (UInt32(_ld8(p, base + 1)) << 16)
            | (UInt32(_ld8(p, base + 2)) << 8)
            | UInt32(_ld8(p, base + 3))
        )
        if k == 0:
            a.w0 = word
        if k == 1:
            a.w1 = word
        if k == 2:
            a.w2 = word
        if k == 3:
            a.w3 = word
        k += 1
    if a.family == MJS_SOCK_UNIX:
        a.path = _path_of(p)^
    return a^


def _path_of(p: UnsafePointer[Byte, MutAnyOrigin]) -> String:
    var s = String("")
    var i = 0
    while i < UNIX_PATH_MAX:
        var bch = _ld8(p, 32 + i)
        if bch == 0:
            break
        s += chr(Int(bch))
        i += 1
    return s^


# Fill `dbuf` with the bytes of `dotted` and NUL-terminate. Kept as its own
# tiny function per the thread._fill b2 WORKAROUND: a pointer-store loop
# must not share a function body with the subsequent extern call in this
# module (cross-module lowering misbind, #49 family; verified by
# bisection in this lane).
def _fill_dotted(
    dbuf: UnsafePointer[Byte, MutUntrackedOrigin], dotted: String
) -> Int:
    # Clamps at 62 bytes + NUL: anything longer cannot be a valid dotted
    # quad, and libc inet_pton then rejects it with -EINVAL downstream —
    # which keeps THIS wrapper at exactly one raise site (a second raise
    # before the extern call trips the b2 lowering crash documented in the
    # module docblock).
    var src_ptr = dotted.unsafe_ptr()
    var sbp = UnsafePointer[Byte, MutUntrackedOrigin](
        unsafe_from_address=Int(src_ptr)
    )
    var total = len(dotted)
    var n = 62 if (total > 62) else total
    var i = 0
    while i < n:
        dbuf[i] = sbp[i]
        i += 1
    return n


# Parse dotted-quad IPv4 text into a SocketAddress (libc inet_pton semantics
# via mjs_sockaddr_ipv4).
#
# Raises (decoded errno): -EFAULT/-EINVAL per the frozen contract. Exactly
# ONE raise site: malformed input — including overlong text — decodes as
# -EINVAL from the C layer (see _fill_dotted clamp note).
# Blocking: none (SYS-5). Allocation: stack scratch cells only.
# Task-aware: no.
def socket_address_parse_ipv4(dotted: String, port_: Int32) raises -> SocketAddress:
    var dbuf = stack_allocation[64, Byte]()
    var n = _fill_dotted(dbuf, dotted)
    dbuf[n] = Byte(0)
    var out_cell = stack_allocation[SOCKADDR_WORDS, Int64]()
    var rc = _externs.probe_sockaddr_ipv4(
        _byte_buf(dbuf), port_, _buf_of(out_cell)
    )
    if rc != 0:
        raise_errno(rc)
    return _sockaddr_from_buffer(out_cell)


# Format an IPv4 SocketAddress as dotted-quad text (libc inet_ntop semantics
# via mjs_sockaddr_format4).
#
# Raises (decoded errno): -EFAULT/-EINVAL per the frozen contract.
# Blocking: none (SYS-5). Allocation: one result String (diagnostic path).
# Task-aware: no.
def socket_format_ipv4(address: SocketAddress) raises -> String:
    if address.family != MJS_SOCK_INET:
        raise_errno(EINVAL_RC)
    var in_cell = stack_allocation[SOCKADDR_WORDS, Int64]()
    address.to_buffer(in_cell)
    var obuf = stack_allocation[16, Byte]()  # INET_ADDRSTRLEN
    var len_slot = stack_allocation[1, UInt64]()
    var rc = _externs.probe_sockaddr_format4(
        _buf_of(in_cell), obuf, UInt64(16), len_slot
    )
    if rc != 0:
        raise_errno(rc)
    var out_s = String("")
    var i = 0
    while i < Int(len_slot[0]):
        out_s += chr(Int(obuf[i]))
        i += 1
    return out_s
