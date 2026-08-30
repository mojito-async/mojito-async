# mojito_async/test/linux/t61_reactor_iouring_completion_aot.mojo
#
# Issue #171 (EPIC #140, split from the already-closed #168) — nothing in
# this repo's own test history has ever driven create_completion_poller()
# to SUCCESS. t39 (mojito_async/test/unit/t39_reactor_aot.mojo:98-106)
# only ever asserts it RAISES, which is correct on every host that driver
# has ever run on (Darwin has no io_uring at all; Linux without
# MOJITO_IO_URING=1 is gated off by design) — so the one configuration
# that must succeed has never executed once. That is exactly how #168's
# defect (uring_submit_poll_add's docstring said "0 or negative errno";
# the code returned the SQE count, so mjs_iouring_create misread success
# as failure and tore the ring down) went unnoticed: the re-vendor that
# fixed it was verified by mojito-sys's OWN drivers, never this repo's.
#
# This driver is the missing lane. On a REAL Linux host with io_uring AND
# MOJITO_IO_URING=1 set, it drives Reactor(create_completion_poller())
# through the exact register -> write -> poll -> deliver -> unregister ->
# wake cycle t39_reactor_aot.mojo already proves for kqueue — same
# assertions, this time over the completion backend — so "
# create_completion_poller() succeeded" means a REAL registration produced
# a REAL delivered event, not merely "did not raise".
#
# THE GUARD — the part the issue calls out as mattering most: mojito-sys
# #167 (the upstream root cause #168 re-vendored the fix for) was very
# nearly missed because a lane without a real capability guard reports
# "unsupported platform" and reads exactly like a pass. Under Rosetta or
# `qemu linux/amd64` emulation, and under Docker's default seccomp
# profile, `io_uring_setup` returns/is refused with ENOSYS.
# vendor/mojito-sys/mjs_iouring.c's mjs_iouring_probe() (wired through
# this file's iouring_probe()/iouring_available()) is a REAL
# io_uring_setup(2) + immediate teardown, not a compile-time `__linux__`
# check, so it reports False in exactly those emulated/sandboxed
# environments — same as it would on a genuine pre-5.1 kernel. This
# driver checks CompilationTarget().is_linux(), then iouring_probe()
# (kernel support), then iouring_available() (support AND the flag)
# BEFORE ever calling create_completion_poller(), and if any of the three
# is false it prints UNSUPPORTED-PLATFORM naming exactly which check
# failed and exits 2 — a THIRD outcome, distinct from both the exit-0 PASS
# and the exit-1 RED a real assertion failure prints, and distinct from
# the ambiguous "raised, therefore the one assertion I make passed" shape
# t39 settles for on its completion-backend line. A CI/gate consumer must
# never fold exit 2 into "green": it means nothing was verified, positive
# or negative, not that nothing is wrong.
#
# Kept OUT of mojito_async/test/unit (test/run.sh's normal JIT/AOT globs)
# ON PURPOSE: this is a Linux-host-and-flag-gated lane, not part of the
# suite every commit runs everywhere, mirroring mojito-sys's own
# tests/s6/iouring_submit — a lane invoked by its OWN dedicated runner
# (here: mojito_async/test/linux/run.sh), not folded into the ambient
# suite. precommit/run-suite.sh does not call it; .github/workflows/
# ci.yml's `suite-linux-iouring` job does, with MOJITO_IO_URING=1 set.
#
# AOT-only, same reasons as its sibling t39_reactor_aot.mojo: imports the
# reactor package (Reactor/create_completion_poller), which
# mojito_async/test/run.sh's AOT_O0_DRIVERS note documents as a b2 1.0.0b2
# default-optimization compiler CRASH once compiled alongside its own
# dependency graph (t39/t40/t41/t42/... all hit the identical crash for
# the identical reason) — mojito_async/test/linux/run.sh builds this
# driver at `-O 0` for the same reason. One local libc extern (`pipe`) is
# declared directly here, matching the "local libc externs run AOT-only"
# house convention; `_exit` is declared the same way
# t34_two_phase_aot.mojo does, to get a real distinguishable exit code
# that a plain `raise` cannot give (mojo's raise path here always reports
# exit 1, which would collapse RED and UNSUPPORTED-PLATFORM together).
#
# Exit codes (mirrors precommit/run-suite.sh's own convention, and
# mojito-sys's tests/s6/iouring_submit lane):
#   0  PASS — create_completion_poller() succeeded on a REAL io_uring
#      host and a registration + delivered event round-tripped through it
#      (plus unregister + wake, matching t39's full battery).
#   1  RED — the guard says this host genuinely supports it, and
#      something the driver actually checked came back wrong.
#   2  UNSUPPORTED-PLATFORM — the guard tripped; nothing was verified,
#      positively or negatively. Never scored as PASS or RED by any
#      caller.
from std.memory import stack_allocation
from std.sys import CompilationTarget

from mojito_async.reactor.io_token import IoOpKind
from mojito_async.reactor.poller import create_completion_poller
from mojito_async.reactor.reactor import Reactor
from mojito_async.runtime.runtime import create
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.vendor.mojito_sys import monotonic_now_ns
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.platform.iouring import (
    iouring_available,
    iouring_probe,
)
from mojito_async.vendor.mojito_sys_io.poller import IoInterest


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


def red(what: String) -> None:
    print("T61 reactor io_uring completion: RED (" + what + ")")
    _iso_exit(1)


def unsupported(what: String) -> None:
    print("T61 reactor io_uring completion: UNSUPPORTED-PLATFORM (" + what + ")")
    _iso_exit(2)


def main() raises -> None:
    # --- the guard: three real, runtime capability checks, weakest-to-
    # strongest, each naming itself in the exit-2 message so a CI log
    # says exactly why nothing was verified rather than leaving a reader
    # to guess. -------------------------------------------------------
    if not CompilationTarget().is_linux():
        unsupported("not a Linux host (io_uring is Linux-only)")
    if not iouring_probe():
        unsupported(
            "kernel does not support io_uring (a real io_uring_setup(2) "
            "probe failed) -- expected under Rosetta/qemu emulation, "
            "Docker's default seccomp profile, or a pre-5.1 kernel"
        )
    if not iouring_available():
        unsupported(
            "kernel supports io_uring but MOJITO_IO_URING=1 is not set"
        )

    # --- from here the guard says this host can really do it: every
    # remaining exit MUST be 0 (verified PASS) or 1 (a real RED), never
    # 2 -- the whole point of this lane is that this is the ONE
    # configuration allowed to actually prove something. -------------
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]

    var reactor = Reactor(create_completion_poller())
    var rt = create()
    var handle = NativeIoHandle(rfd)
    var token = reactor.register_op(handle, IoInterest.READABLE, IoOpKind.READ)
    if reactor.live_count() != 1:
        red("register_op did not occupy exactly one table slot")

    # --- timeout expiry is success-with-zero (nothing written yet) ----
    var ready0 = reactor.poll(rt, Optional[Duration](from_millis(50)))
    if len(ready0) != 0:
        red("expected zero ready ops before any write")

    # --- a real write flips the fd readable; poll must observe it via a
    # REAL io_uring completion, not a stub. ----------------------------
    var wf = FileDescriptor(Int(wfd))
    wf.write("A")
    var ready1 = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready1) != 1:
        red(
            "expected exactly one ready op after the write, got "
            + String(len(ready1))
        )
    if ready1[0].slot != token.slot or ready1[0].generation != token.generation:
        red("delivered token does not match the registered token")
    if ready1[0].op_kind != IoOpKind.READ:
        red("op kind not preserved through drain_ready")

    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("unregister did not release the table slot")

    # --- manual wake() ends a blocking wait PROMPTLY with zero events --
    reactor.wake()
    var t0 = monotonic_now_ns()
    var ready2 = reactor.poll(rt, Optional[Duration](from_millis(5000)))
    var elapsed_ms = (monotonic_now_ns() - t0) // 1000000
    if len(ready2) != 0:
        red("wake() must not fabricate a ready op")
    if elapsed_ms > 1000:
        red(
            "wake() did not end the blocking wait promptly (waited "
            + String(elapsed_ms) + "ms against a 5000ms bound)"
        )

    print(
        "T61 reactor io_uring completion: PASS (real io_uring ring: "
        "register -> write -> delivered event -> unregister -> wake, "
        "all verified)"
    )
    _iso_exit(0)
