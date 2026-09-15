# SoEx.Messaging.Chimera

A durable event log that runs inside your process, with no broker, no container and no outside
dependency of any kind. Topics are ordinary SQLite files on local disk.

It exists so a new SoEx project can start publishing and subscribing on day one, and so the thing
you start with does not lose your messages the moment the process restarts.

## What it is for

Use it while you have **one machine, one writer per file, and a publisher and subscribers that stop
and start together**. That is the whole boundary. Inside it, messages survive a crash, each
subscriber keeps its own position, and nothing is lost quietly. Outside it — a second node,
anything that has to scale horizontally — you want a real broker such as NATS or Azure Service Bus.

Usually that means one process, and it is what the design is aimed at. It does not have to: a
publisher in one process and its subscribers in another is a supported shape and is tested that
way, because those are separate files with one writer each. Two processes writing the *same* file
is the thing to avoid.

Behaviour is deliberately held to what those brokers also do, so code written against this keeps
working when you move to one.

## Shape of the API

The library is standalone. It has no dependency on SoEx, on a container, or on a host, and it does
not know what your messages are: you hand it bytes and it hands them back.

Topics and subscribers are strings, the same as a queue name in the Service Bus SDK or a subject in
NATS. If you want them derived from a contract type, that belongs in the binding you write on top,
which is where every client library puts it. One caution for whoever writes that binding: a topic
name is a filename here, so deriving it from a fully-qualified type means a namespace refactor
points at an empty file and leaves the old backlog stranded on disk. Pin the name, don't compute it
from something a refactor can move.

Keeping the surface this small is deliberate: wiring it into a messaging framework should be a short
piece of code you write and can read, not an integration you take on trust.

## Model

A topic is an append-only log. Subscribers do not consume from it destructively; each keeps its own
position, so adding a subscriber costs nothing and a slow one does not hold anyone else up.

Delivery is at least once. Append order is total and stable. **Delivery order is not guaranteed** —
a message that fails comes back after later ones, and handlers run concurrently — which is what
NATS and Service Bus do too.

If a handler cannot deal with a message now, it can set it aside, and it comes back when your code
asks for it. If it fails repeatedly it becomes a dead letter, which is a row you can look at,
replay or discard. Nothing is a copy into another topic.

Because a topic is a SQLite file, you can open it read-only with any tool in any language while the
system is running, without disturbing it.

## Limits, stated plainly

- One writer per file, one machine. Processes may differ — a publisher here, its subscribers there — but two
  writers on one file is a rule you keep, by whatever means suits your system: nothing here enforces it, and
  the docs say why. Not a network broker and not a cluster.
- The log is trimmed on retention, not on consumption. A subscriber that stops for longer than the
  retention window loses messages — it will tell you exactly how many, and it warns you before it
  happens, but it does not stop the publisher to save you.
- Timers are not its job. There is no backoff, no delayed retry, no scheduler. Anything that needs
  to wait durably belongs in `SoEx.Workflow`.
- Payloads are capped at 256 KB by default, per topic. That is Service Bus's standard-tier limit and
  SQS's, and it is the smallest of the places you would move to. Note that it is not an exact match:
  Service Bus counts its whole encoded message against that number, envelope and properties included,
  while this counts the payload, which is all it has to count. So a message that fits here can still
  be refused there. If you publish near the cap, lower it rather than discover that after moving.
- **It stores the bytes you give it, as given.** There is no encryption at rest, deliberately. Any
  key it held would sit on the same disk as the data, in config it has to be able to read, which
  buys nothing against anyone who can already reach the file. Encrypt above it, per message, where
  a key can be scoped to a subject and destroying that key can erase one person's data without
  rewriting the store. Full-disk encryption covers the stolen-disk case better than the library
  could. If the bytes reaching it are plaintext, the file is plaintext, and dead letters keep their
  copy after the log has moved on.
- **Deleting a row does not reliably erase it.** Retention removes records from the database, but on
  an SSD or a copy-on-write filesystem the old content survives underneath — a new physical page is
  written and the previous one lingers until wear levelling or a snapshot releases it. No setting
  here changes that. If bytes must be unrecoverable, they need to be unreadable before they are
  written, and the key destroyed when the time comes.

## Documentation

Start at [docs/README.md](docs/README.md). If you only read one page, make it
[Limits, and how you find out](docs/explanation/limits.md). If you are about to write a test over a system
built on this, read [Testing a system built on this](docs/how-to/testing.md) first: the affordance for
knowing when an event-driven system has come to rest ships in `SoEx.Messaging.Chimera.Testing`.

## Status

Alpha, and nothing has been published before this. The API and the on-disk format can both still
change, and a format change would strand an existing topic's files. Read
[Limits, and how you find out](docs/explanation/limits.md) before you depend on it.
