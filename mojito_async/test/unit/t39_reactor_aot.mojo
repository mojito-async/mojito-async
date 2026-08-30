# mojito_async/test/unit/t39_reactor_aot.mojo
#
# A7.1 reactor lane (issue #75) — native poller adapter acceptance driver.
#
# Exercises the issue's exit criteria directly against the real kqueue
# backend (Reactor.register_op/poll/unregister/wake, reactor/poller.mojo's
# create_poller/create_completion_poller):
#   - register an op on a real fd (a pipe(2) read end), feed a REAL
#     IoEvent from an actual poll() (no write yet -> zero ready), then
#     write a byte and observe the correct slot marked ready with the
#     registered op kind intact;
#   - a poll() bounded by a short timeout with nothing ready returns zero
#     tokens (timeout is success-with-zero, never a raise);
#   - a manual wake() ends a blocking wait PROMPTLY with zero events (no
#     spurious ready tokens, no long block);
#   - the completion backend (create_completion_poller, ADR-SYS-009) is
#     UNREACHABLE without the MOJITO_IO_URING flag/host support and raises
#     a decoded error (this darwin host has neither).
#
# The line above is the ONLY assertion this driver makes about the
# completion backend, and it is correct on every host this driver has
# ever run on -- which means the one configuration that must SUCCEED
# (Linux, real io_uring, MOJITO_IO_URING=1) has never executed here.
# mojito_async/test/linux/t61_reactor_iouring_completion_aot.mojo (issue
# #171) is that lane: same register/write/poll/deliver/unregister/wake
# battery as this driver, over the completion backend, guarded so it can
# only report success on a host that genuinely has it.
#
# AOT-only: imports the vendor/mojito_sys_io externs (dylib mjs_poller_*
# symbols), so this runs via the run.sh unit AOT loop (`*_aot.mojo`,
# modular/modular#6971 — the b2 JIT cannot resolve dylib symbols through
# an imported module).  One local libc extern (`pipe`) is declared here
# directly, matching the "local libc externs run AOT-only" house
# convention (see mojito-shared-context.md); writes to the pipe use the
# builtin `FileDescriptor` (declaring a second `write` extern collides
# with the stdlib's own internal `write` binding at LLVM lowering).
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from std.memory import stack_allocation

from mojito_async.reactor.io_token import IoOpKind
from mojito_async.reactor.poller import create_completion_poller
from mojito_async.reactor.reactor import Reactor, make_reactor
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.time.deadline import Duration, from_millis
from mojito_async.vendor.mojito_sys import monotonic_now_ns
from mojito_async.vendor.mojito_sys_io.handle import NativeIoHandle
from mojito_async.vendor.mojito_sys_io.poller import IoInterest


@extern("pipe")
def c_pipe(fds: UnsafePointer[Int32, MutAnyOrigin]) abi("C") -> Int32:
    ...

def red(what: String) raises -> None:
    print("T39 reactor bound: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var fds = stack_allocation[2, Int32]()
    if c_pipe(fds) != 0:
        red("setup: pipe(2) failed")
    var rfd = fds[0]
    var wfd = fds[1]

    var reactor = make_reactor()
    var rt = create()
    var handle = NativeIoHandle(rfd)
    var token = reactor.register_op(handle, IoInterest.READABLE, IoOpKind.READ)
    if reactor.live_count() != 1:
        red("register_op did not occupy exactly one table slot")

    # --- timeout expiry is success-with-zero (nothing written yet) --------
    var ready0 = reactor.poll(rt, Optional[Duration](from_millis(50)))
    if len(ready0) != 0:
        red("expected zero ready ops before any write")

    # --- a real write flips the fd readable; poll must observe it ---------
    var wf = FileDescriptor(Int(wfd))
    wf.write("A")
    var ready1 = reactor.poll(rt, Optional[Duration](from_millis(2000)))
    if len(ready1) != 1:
        red("expected exactly one ready op after the write, got "
            + String(len(ready1)))
    if ready1[0].slot != token.slot or ready1[0].generation != token.generation:
        red("delivered token does not match the registered token")
    if ready1[0].op_kind != IoOpKind.READ:
        red("op kind not preserved through drain_ready")

    reactor.unregister(token)
    if reactor.live_count() != 0:
        red("unregister did not release the table slot")

    # --- manual wake() ends a blocking wait PROMPTLY with zero events -----
    reactor.wake()
    var t0 = monotonic_now_ns()
    var ready2 = reactor.poll(rt, Optional[Duration](from_millis(5000)))
    var elapsed_ms = (monotonic_now_ns() - t0) // 1000000
    if len(ready2) != 0:
        red("wake() must not fabricate a ready op")
    if elapsed_ms > 1000:
        red("wake() did not end the blocking wait promptly (waited "
            + String(elapsed_ms) + "ms against a 5000ms bound)")

    # --- completion backend is unreachable without MOJITO_IO_URING --------
    var completion_raised = False
    try:
        _ = create_completion_poller()
    except e:
        completion_raised = True
    if not completion_raised:
        red("create_completion_poller must raise a decoded error on a host "
            "without io_uring / the MOJITO_IO_URING flag")

    print("T39 reactor bound: PASS")
