# Files on disk

All Chimera files are in the configured root directory. Chimera uses two kinds of file:

- `<topic>.topic.db` holds one topic. Its only writer is the topic's `SqliteTopicLog`.
- `<subscriber>.subscriber.db` holds the positions, in-flight deliveries and set-aside messages of one
  subscriber, across all its topics. Its only writer is that subscriber.

## File names

Chimera sanitises each name into a filename. Two different names always get two different files.

Letter case is part of the name. Windows and macOS default to filesystems that ignore case. Chimera
escapes each upper-case letter, and `Orders` and `orders` stay two topics on all platforms.

A name of only lower-case ASCII letters, digits, `-` and `_` is its own filename. Each other character becomes
an escape of up to five characters. Allow for this length if you have long names and a path limit.

## Writers and readers

The whole design depends on this rule: each file has one writer. SQLite's single-writer limit never
applies. No lease or visibility timeout is necessary. Only one subscriber in one process can hold a lock. When
that process stops, its lock goes with it.

### Enforcing the rule

You must keep to the one-writer rule. Chimera does not refuse a second writer. This is by design.

These methods can hold one writer to a file:

- a lock file in the root directory
- a named system mutex
- a lease in the system that your application already coordinates with
- a supervisor that runs one instance

These methods differ in these properties:

- whether the lock survives a kill
- the result of a stale lock
- whether the lock works across containers or only within one host

Choose the method that fits your system. The measurement below shows the result of a broken rule.

### Two writers on one file

In a measurement, two processes read one topic through the *same* subscriber file. They handled 200 messages
in about 210 deliveries. No message was lost, and the file was readable after the test.

The result is duplicate handling, with no corruption and no loss. At-least-once delivery already permits
duplicates, and a handler must tolerate them. The error is easy to make by accident. No crash occurs. The
only sign is work done twice.

### Paths

Chimera treats two spellings of one path as one file. A relative root and an absolute root, or `/var/events`
and `/var/./events`, cannot become two writers by accident. A symbolic link can still give two writers. Chimera
resolves paths as text, and it does not follow links.

### Read-only access

**Any process, in any language, can open these files read-only while the system runs.** A read-only reader
does not disturb the system. It cannot corrupt a file, move the position of a subscriber or block a writer.

```
sqlite3 'file:orders.topic.db?mode=ro' 'SELECT seq, enqueued_at, size FROM log ORDER BY seq DESC LIMIT 20;'
```

Keep read transactions short. A long read transaction stops the checkpoint of the write-ahead log. The log then
grows for as long as you hold the transaction. In a test, two thousand small appends against one open snapshot
grew a `-wal` file to 22 MB. A truncating checkpoint reclaimed none of it until the reader finished.

Read-only mode applies only to the database file. SQLite still creates the `-wal` and
`-shm` sidecar files. The process needs write permission on the directory, though it never writes the
database. A read-only mount does not work.

## The schema

Treat the schema as an interface. The `meta` table holds the schema version. Each change to the schema also
changes that version.

A version of Chimera older than the version that wrote a file refuses to open that file. This applies to
both kinds of file, for writing and for reading. An older version would use a layout that has changed:

- In a topic file, it would misread records.
- In a subscriber file, it would misread positions. That misread gives no error. It gives an answer, and the
  answer redelivers all messages or skips them.

```sql
-- <topic>.topic.db
meta(key, value)                          -- schema_version, topic, log_uuid
log(seq, msg_id, enqueued_at, size, payload)
scheduled(id, msg_id, enqueued_at, due_at, size, payload)
```

```sql
-- <subscriber>.subscriber.db
meta(key, value)                          -- schema_version, subscriber
cursor(topic, log_uuid, mark, due_floor, id_floor)
entry(topic, source, id, state, attempts, updated)
set_aside(aside_id, kind, topic, origin, msg_id, enqueued_at, aside_at, delivery, attempts, reason, payload)
```

### Positions

A subscriber holds two positions per topic, because the topic file has two tables with different orders.

- `mark` is the log sequence at or below which all records are settled.
- `due_floor` and `id_floor` are the position in the scheduled table. That table is in order of due time, then
  id.
- `entry` holds only the exceptions: the deliveries in flight now, and the finished messages that a position
  has not absorbed yet.

### Metadata

Chimera keeps metadata in columns, separate from the payload. Encrypt sensitive payloads before publication.
With encrypted payloads, you can still measure lag, count messages and triage a backlog with `sqlite3`.

### Log identity

`log_uuid` identifies a log across a restore. A subscriber records the `log_uuid` of the log that its position
belongs to. It refuses to continue against a different log. A log restored to an earlier point reissues
sequences that its readers have already finished. A read past that point would skip those messages silently.
