# The log model

A Chimera topic is a log. The log has these rules:

1. The topic is an append-only sequence of records.
2. Each subscriber keeps a separate position in the topic.
3. A read leaves the record in the log.
4. Retention removes records by a policy. It does not use the positions of the subscribers.

## Subscribers are independent

A new subscriber changes nothing for the existing subscribers. A slow subscriber delays only itself.

Two subscribers on one topic each receive every message. Each keeps an independent position. Neither
can affect the progress of the other.

## Settling out of order

A subscriber keeps a position and a small set of exceptions above it. It can thus settle a message out of
order and continue. This is the Apache Pulsar model: a cumulative acknowledgement with individual
acknowledgements above it.

With this model, each subscriber can:

- Set aside a message that it cannot handle now.
- Dead-letter a message that failed too many times.
- Continue past these messages. They do not block the head of the topic.

A poison message becomes a dead letter of one subscriber. The other subscribers continue.

## Retention

Retention removes messages by policy, read or unread. It does not collect the positions of the
subscribers first. Thus a stopped subscriber cannot stop the publisher.

Retention can remove a message before a subscriber reads it. [Limits](limits.md)
describes this loss and how Chimera reports it.
