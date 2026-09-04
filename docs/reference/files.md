# Files on disk

Everything lives in the root directory you configure. There are two kinds of file.

`<topic>.topic.db` holds one topic. It is written by the one `SqliteTopicLog` you opened for that topic and
by nothing else.

`<subscriber>.subscriber.db` holds one subscriber's positions, its in-flight deliveries and what it has set
aside, across every topic it reads. It is written by that subscriber and by nothing else.

Names are sanitised into filenames, and no two names ever share a file. Letter case is part of that: Windows
and macOS default to filesystems that ignore case, so an upper-case letter is escaped rather than written
through, and `Orders` and `orders` stay two topics wherever you run. A name that is all lower-case ASCII
letters, digits, `-` and `_` is its own filename; anything else picks up escapes and gets longer, by up to
five characters per character, which is worth knowing if you have both long names and a path limit.

## One writer, any number of readers

This is the rule the whole design rests on. Because each file has exactly one writer, SQLite's single-writer
limit never binds, and no lease or visibility timeout is needed anywhere: a lock can only be held by one
subscriber in one process, and if that process is gone the lock is gone with it.

It also means **you can open any of these files read-only, from any language, while the system is running**,
without disturbing anything. You cannot corrupt them, advance anybody's position, or block a writer.

```
sqlite3 'file:orders.topic.db?mode=ro' 'SELECT seq, enqueued_at, size FROM log ORDER BY seq DESC LIMIT 20;'
```

Two things to know before you do.

Keep read transactions short. A long-lived one stops the write-ahead log being checkpointed, and it grows for
as long as you hold it. Two thousand small appends against one open snapshot grew a `-wal` to 22 MB in
testing, and a truncating checkpoint could not reclaim any of it until the reader finished.

Read-only is read-only for the database, not for the directory. SQLite still creates the `-wal` and `-shm`
sidecars, so the process needs write permission on the directory even though it never writes the database. A
read-only mount will not work.

## The schema

Treat it as an interface. It is versioned in the `meta` table, and it will not change without that version
changing.

Both kinds of file refuse to be opened by a version of this library older than the one that wrote them, for
writing as well as for reading, rather than working under a layout that has moved. For a topic that would misread records; for a
subscriber it would misread positions, which does not fail, it answers, and the answer redelivers everything
or skips it.

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

A subscriber holds two positions per topic, because the topic file holds two tables that are ordered
differently. `mark` is the log sequence at or below which everything is settled. `due_floor` and `id_floor`
are the same idea for the scheduled table, whose order is due time and then id. `entry` is only the exception
set: what is in flight now, and what is finished with but not yet absorbed into a position.

Metadata is in columns, never inside the payload. If you encrypt your payloads — which is where encryption
belongs, not here — you can still measure lag, count messages and triage a backlog with `sqlite3`.

`log_uuid` identifies this log across a restore. A subscriber records the one its position belongs to and
refuses to carry on against a different one, because a log restored to an earlier point reissues sequences
its readers already consider finished, and reading on would skip those messages silently.
