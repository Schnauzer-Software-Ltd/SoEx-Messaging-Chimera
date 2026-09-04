# Why a log and not a queue

A queue is read destructively. One consumer takes a message and it is gone. That works until two components
need the same message, at which point the queue has to hold a copy per consumer, and every one of those
copies is a thing that can be delivered, retried and lost separately.

A log is not read destructively. The topic is an append-only sequence and each subscriber remembers its own
position in it. Adding a reader costs nothing, because nothing about the existing readers changes. A slow
reader inconveniences only itself.

The obvious objection is that a log gives up the things a queue is good at: taking a message out of the way
because you cannot deal with it now, parking one that has failed too often, and not being blocked at the head
by either. Those come back on the reader's side rather than the log's. A subscriber keeps a position plus a
small set of exceptions above it, so it can say "this one is finished with" out of order and carry on. That
is Apache Pulsar's model: a cumulative acknowledgement with individual ones above it.

## What that buys, concretely

Two subscribers on one topic each get every message, keep independent positions, and cannot affect each
other's progress. A poison message becomes one subscriber's dead letter rather than a blockage everybody
shares.

Messages are removed on a retention policy rather than when they have been read, so nothing depends on
gathering positions from every subscriber before anything can be tidied up. That is what stops a stopped
subscriber from stopping the publisher.

## What it costs

A message can be removed before a subscriber has read it. A queue cannot do that. This is a real trade and
[Limits, and how you find out](limits.md) is about it.
