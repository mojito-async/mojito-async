# mojito_async/channel/__init__.mojo
#
# A1.3 channel (issue #35) — public channel surface.
#
# Spec §39-41: bounded Channel[T] with Sender/Receiver split, ring buffer +
# sender/receiver wait queues + closed flags; send/recv may park, try_send/
# try_recv never block; close-last-sender drains receivers, close-last-receiver
# wakes blocked senders and fails subsequent sends.
#
# Lower-level pieces (WaitRecord, the park/wake seam) live in
# mojito_async.channel.channel; this module re-exports the public names so
# consumers import from mojito_async.channel (spec §6 layout).
from mojito_async.channel.channel import (
    Channel,
    Receiver,
    Sender,
    WaitRecord,
    make_channel,
    make_receiver,
    make_sender,
)
from mojito_async.channel.select import (
    SelectBranch,
    SelectOutcome,
    SelectState,
    branch_ready,
    classify_branch,
    deadline_branch,
    recv_branch,
    rescan,
    select,
    select_fast,
    send_branch,
    timeout_branch,
)