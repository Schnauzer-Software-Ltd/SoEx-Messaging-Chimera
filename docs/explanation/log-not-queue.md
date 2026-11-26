# The log model

A Chimera topic is an append-only log of records.

- Each subscriber has a separate position in the log.
- Reading a record leaves it in the log.
- Retention removes records by age, size or count.

## Independent subscribers

Every subscriber receives every message. A slow or stopped subscriber delays only itself.

## Settling out of order

A subscriber stores its position and the settled messages above it. It can settle messages in any order. This
is the Apache Pulsar model: a cumulative acknowledgement with individual acknowledgements above it.

Each subscriber can defer a message, dead-letter a message that failed too often, and read past both. A poison
message becomes a dead letter for one subscriber. The other subscribers continue to read.

## Retention

Retention removes records by policy, read or unread. The publisher never waits for a subscriber.
[Limits](limits.md) describes how Chimera reports unread messages removed by retention.
