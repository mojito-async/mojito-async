# mojito_async/channel/__init__.mojo
#
# A1.3 channel (issue #35) / A5.2 unbounded channel (issue #90) — public
# channel surface.
#
# Spec §39-41: bounded Channel[T] with Sender/Receiver split, ring buffer +
# sender/receiver wait queues + closed flags; send/recv may park, try_send/
# try_recv never block; close-last-sender drains receivers, close-last-receiver
# wakes blocked senders and fails subsequent sends.  UnboundedChannel[T]
# mirrors this model with no capacity bound: a sender never blocks, only a
# receiver waits on an empty channel.  Its Sender/Receiver handles are
# aliased to UnboundedSender/UnboundedReceiver here — the bounded and
# unbounded modules each define their own `Sender`/`Receiver` struct, and a
# bare re-export of both under the same name is ambiguous (Mojo rejects it
# at import time), so the unbounded pair is re-exported under distinct
# public names.
#
# Lower-level pieces (WaitRecord, the park/wake seam) live in
# mojito_async.channel.channel; this module re-exports the public names so
# consumers import from mojito_async.channel (spec §6 layout).
from mojito_async.channel.channel import (
    Channel,
    Receiver,
    RecvOutcome,
    SendOutcome,
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
    never,
    recv_branch,
    rescan,
    select,
    select_fast,
    send_branch,
    timeout_branch,
)
from mojito_async.channel.unbounded import (
    UnboundedChannel,
    Receiver as UnboundedReceiver,
    Sender as UnboundedSender,
    make_unbounded,
    make_unbounded_receiver,
    make_unbounded_sender,
)

# A5.1 rendezvous + oneshot (issue #89): capacity-0 handoff and
# single-value/single-delivery channels re-exported from
# mojito_async.channel.rendezvous / mojito_async.channel.oneshot so
# consumers import them the same way as the bounded Channel above.
from mojito_async.channel.rendezvous import (
    RendezvousChannel,
    RendezvousReceiver,
    RendezvousSender,
    SendWait,
    make_rendezvous,
    make_rendezvous_receiver,
    make_rendezvous_sender,
)
from mojito_async.channel.oneshot import (
    Oneshot,
    OneshotReceiver,
    OneshotSender,
    make_oneshot,
    make_oneshot_receiver,
    make_oneshot_sender,
)