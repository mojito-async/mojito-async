# mojito_async/vendor/mojito_sys_io/errors.mojo
#
# A7.1 reactor lane (issue #75) — vendored verbatim from mojito-sys
# `mojito_sys/abi/errors.mojo` @ github.com/mojito-async/mojito-sys
# commit 0bfb5291b698c833b302feb35c5724d44961f6d5 (see
# vendor/mojito-sys/VENDORED_AT_S6.txt).  Zero edits beyond this header and
# the module path (kept in the `mojito_sys_io` vendor package rather than
# `mojito_sys.abi` so this tree stays self-contained under
# `mojito_async.vendor`).
#
# A compact platform-neutral error carrier: an OS/generic domain plus a raw
# code within that domain.  `SysError.to_string()` is a diagnostic aid ONLY
# (off the hot path, not a stable API).  POSIX errno names are HOST-
# SELECTED: darwin numbering on macOS targets, Linux numbering on Linux
# targets (`errno_name`).
#
# `raise_errno` is the ONLY sanctioned way callers on the mjs_* ABI raise a
# decoded return code: b2 SIGSEGVs when a String LITERAL reaches a `raise`
# payload through ANY control-flow merge while lowering a raising member of
# a Movable struct in a module that also lowers @extern bindings (mojito-
# sys issue #29, panel H6) — every name here is built from an Int-packed
# table at RUNTIME instead, straight-line, to sidestep that lowering bug.


from std.sys import CompilationTarget


def _unpack(v: Int) -> String:
    # Little-endian packed ASCII: byte 0 is the first character; the string
    # ends at the first zero byte. Straight-line construction only.
    var s = String("")
    var rest = v
    while rest != 0:
        s += chr(rest & 0xFF)
        rest >>= 8
    return s


# Darwin-table scan. Returns "" for codes outside the darwin numbering.
def _errno_name_darwin(code: Int32) -> String:
    var packed = Int(0)
    var packed2 = Int(0)
    if code == 2:
        packed = 0x544E454F4E45  # "ENOENT"
    elif code == 13:
        packed = 0x534543434145  # "EACCES"
    elif code == 35:
        packed = 0x4E4941474145  # "EAGAIN" (EDEADLK = 11 here)
    elif code == 12:
        packed = 0x4D454D4F4E45  # "ENOMEM"
    elif code == 4:
        packed = 0x52544E4945  # "EINTR"
    elif code == 22:
        packed = 0x4C41564E4945  # "EINVAL"
    elif code == 14:
        packed = 0x544C55414645  # "EFAULT"
    elif code == 45:
        packed = 0x50555354_4F4E45  # "ENOTSUP"
    elif code == 63 or code == 36:
        # ENAMETOOLONG: darwin spells it 63, Linux 36 (both accepted here
        # so the decoded NAME is host-independent for this code; the
        # numeric value in to_string() still shows the raw host spelling).
        # 12 chars > one Int word, so the name travels as two aligned
        # words ("ENAMETOO" + "LONG"), still runtime-built from Ints.
        packed = 0x4F4F54454D414E45  # "ENAMETOO"
        packed2 = 0x474E4F4C  # "LONG"
    elif code == 78:
        packed = 0x5359534F4E45  # "ENOSYS"
    if packed2 != 0:
        return _unpack(packed) + _unpack(packed2)
    return _unpack(packed)


# Linux-table scan. Returns "" for codes outside the Linux numbering.
def _errno_name_linux(code: Int32) -> String:
    var packed = Int(0)
    if code == 2:
        packed = 0x544E454F4E45  # "ENOENT"
    elif code == 13:
        packed = 0x534543434145  # "EACCES"
    elif code == 11:
        packed = 0x4E4941474145  # "EAGAIN" (35 on darwin)
    elif code == 12:
        packed = 0x4D454D4F4E45  # "ENOMEM"
    elif code == 4:
        packed = 0x52544E4945  # "EINTR"
    elif code == 22:
        packed = 0x4C41564E4945  # "EINVAL"
    elif code == 14:
        packed = 0x544C55414645  # "EFAULT"
    elif code == 35:
        packed = 0x4B4C4441454445  # "EDEADLK" (11 on darwin)
    elif code == 95:
        packed = 0x50555354_4F4E45  # "ENOTSUP" (45 on darwin)
    elif code == 38:
        packed = 0x5359534F4E45  # "ENOSYS"
    return _unpack(packed)


# Host-selected errno name: the table follows the errno NUMBERING of the
# compilation target — darwin on macOS targets, Linux on Linux targets — so
# a colliding code such as 35 prints the name that is correct FOR THE HOST
# libc instead of a fixed numbering. Codes outside the selected table return
# "" and callers fall back to the deterministic numeric form ("POSIX errno
# N"); unsupported targets always take the numeric form. Diagnostic-only —
# see SysError.to_string().
#
# Blocking behavior (SYS-5): none — pure table scan, no syscalls.
# Allocation: builds one String; diagnostic-only path, never hot.
# Task-aware: no async or task interaction.
def errno_name(code: Int32) -> String:
    if CompilationTarget().is_macos():
        return _errno_name_darwin(code)
    if CompilationTarget().is_linux():
        return _errno_name_linux(code)
    return ""


def domain_name(value: Int32) -> String:
    var packed = Int(0)
    if value == 0:
        packed = 0x5849534F50  # "POSIX"
    elif value == 1:
        packed = 0x4843414D  # "MACH"
    elif value == 2:
        packed = 0x4C414E5245544E49  # "INTERNAL"
    elif value == 3:
        packed = 0x32334E4957  # "WIN32"
    elif value == 4:
        packed = 0x415357  # "WSA"
    elif value == 5:
        packed = 0x495041  # "API"
    return _unpack(packed)


# An error-domain discriminator. The domain is the `value` field (compared
# via `==`), kept unique so domains stay pairwise-distinguishable. A code of
# zero means "no failure" in EVERY domain; no domain may use 0 for a failure.
struct ErrorDomain(ImplicitlyCopyable):
    var value: Int32

    comptime POSIX = ErrorDomain(0)
    comptime MACH = ErrorDomain(1)
    comptime INTERNAL = ErrorDomain(2)
    comptime WIN32 = ErrorDomain(3)
    comptime WSA = ErrorDomain(4)
    comptime API = ErrorDomain(5)

    def __init__(out self, value: Int32):
        self.value = value

    def __eq__(self, other: ErrorDomain) -> Bool:
        return self.value == other.value


# A platform-neutral error value: `domain` names the source, `code` the raw
# error number within that domain (a POSIX errno for POSIX). Construction
# never raises and allocates nothing, so `SysError` is safe to build on hot
# paths. A code of 0 in ANY domain means "no failure".
struct SysError(ImplicitlyCopyable):
    var domain: ErrorDomain
    var code: Int32

    def __init__(out self, domain: ErrorDomain, code: Int32):
        self.domain = domain
        self.code = code

    @staticmethod
    def from_posix(errno: Int32) -> SysError:
        return SysError(ErrorDomain.POSIX, errno)

    # Absorbs the frozen contract's return-code sign convention used by the
    # mjs_* C-ABI helpers: rc == 0 success; rc < 0 error, |rc| the POSIX
    # errno; rc > 0 a positive informational value (kept as a positive
    # POSIX code, not itself a failure).
    @staticmethod
    def from_rc(rc: Int32) -> SysError:
        if rc < 0:
            return SysError(ErrorDomain.POSIX, -rc)
        return SysError(ErrorDomain.POSIX, rc)

    def ok(self) -> Bool:
        return self.code == 0

    def to_string(self) -> String:
        if self.domain == ErrorDomain.POSIX:
            var name = errno_name(self.code)
            if name != "":
                return "POSIX(" + name + ") errno " + String(self.code)
            return "POSIX errno " + String(self.code)
        var dname = domain_name(self.domain.value)
        if dname == "":
            dname = "domain" + String(self.domain.value)
        return dname + " code " + String(self.code)


# Raise a frozen-contract return code (mjs_* ABI convention: 0 success,
# negative errno = POSIX errno on failure) as a decoded `Error`.  See the
# module docstring (H6) for why this must stay the ONLY raise site for
# mjs_* return codes rather than a hand-rolled `raise Error(err.to_string())`
# at each call site.
def raise_errno(rc: Int32) raises:
    var err = SysError.from_rc(rc)
    var msg = (
        "mojito-sys error: POSIX errno "
        + String(err.code)
        + " ("
        + errno_name(err.code)
        + ")"
    )
    raise Error(msg)
