# mojito_async/test/unit/t16_cancellation.mojo
#
# A1.1 (issue #33) — basic cooperative cancellation surface:
# CancellationToken + CancelFlag + checkpoint.
#
# Acceptance (A0-T9 carried forward):
#   - request() is idempotent; is_cancellation_requested() observes local OR
#     any ancestor flag (downward child propagation);
#   - checkpoint() is silent when not requested; raises CancellationError-as-
#     Error (message tagged "CancellationError") when requested, stamping the
#     observation;
#   - child propagation: cancelling a parent flag makes a child
#     (make_child_flag) observe at is_cancellation_requested / checkpoint;
#   - reset() clears a pre-observation request; reset after observe raises.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async.cancellation import (
    CancelFlag,
    CancellationToken,
    make_cancel_flag,
    make_child_flag,
)


def red(what: String) raises -> None:
    print("T16 cancellation: RED (" + what + ")")
    raise Error(what)


def main() raises:
    # 1. root flag + token: request is idempotent, observed locally.
    var flag = make_cancel_flag()
    var fp = UnsafePointer[CancelFlag, MutAnyOrigin](to=flag)
    var token = CancellationToken(fp)
    if token.is_cancellation_requested():
        red("fresh token must not be requested")
    token.request()
    token.request()  # idempotent
    if not token.is_cancellation_requested():
        red("token did not observe its own request")

    # 2. checkpoint raises when requested and stamps the observation.
    var raised = False
    try:
        token.checkpoint()
    except e:
        raised = True
        var m = String(e)
        if "CancellationError" not in m:
            red("checkpoint error lacks CancellationError tag: " + m)
    if not raised:
        red("checkpoint did not raise when requested")
    if not token.observed():
        red("observation not stamped after checkpoint")

    # 3. reset after observe raises (spike policy).
    var no_reset = False
    try:
        token.flag()[].reset()
    except Error:
        no_reset = True
    if not no_reset:
        red("reset after observe did not raise")

    # 4. child propagation: cancelling the parent cancels the child.
    var parent_flag = make_cancel_flag()
    var pf = UnsafePointer[CancelFlag, MutAnyOrigin](to=parent_flag)
    var child = make_child_flag(pf)
    if child.is_requested():
        red("child must start unrequested")
    parent_flag.request()
    if not child.is_requested():
        red("child did not observe the parent request (downward propagation)")
    var child_tok = CancellationToken(
        UnsafePointer[CancelFlag, MutAnyOrigin](to=child)
    )
    var child_raised = False
    try:
        child_tok.checkpoint()
    except Error:
        child_raised = True
    if not child_raised:
        red("child checkpoint did not raise through an ancestor")

    # 5. reset clears a pre-observation request.
    var unobserved = make_cancel_flag()
    var t2 = CancellationToken(UnsafePointer[CancelFlag, MutAnyOrigin](to=unobserved))
    t2.request()
    var cleared = False
    try:
        unobserved.reset()
        cleared = True
    except Error:
        cleared = False
    if not cleared:
        red("reset of a pre-observation request raised")
    if t2.is_cancellation_requested():
        red("reset did not clear the pending request")

    print("T16 cancellation: PASS")