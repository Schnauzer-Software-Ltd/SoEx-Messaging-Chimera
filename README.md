# SoEx.Messaging.Chimera

[![NuGet](https://img.shields.io/nuget/vpre/SoEx.Messaging.Chimera.svg)](https://www.nuget.org/packages/SoEx.Messaging.Chimera)

An in-process durable publish/subscribe event log for .NET.

## Features

- Durable append. `AppendAsync` returns when the record is on disk.
- Independent subscribers. Each subscriber keeps a separate position in the topic.
- At-least-once delivery, with retries, deferral, dead letters, replay and scheduled messages.
- Retention by age, size or count, with lost messages counted per subscriber.
- Topic files open read-only in any SQLite tool while the system runs.
- Targets .NET 10 and .NET 11. Depends on `Microsoft.Data.Sqlite`.

## Install

```shell
dotnet add package SoEx.Messaging.Chimera --prerelease
```

## Quick start

```csharp
using System.Text;
using SoEx.Messaging.Chimera;

var options = new ChimeraOptions { RootDirectory = "/var/lib/myapp/events" };

// Publish. Share one writer per topic per process.
await using var orders = SqliteTopicLog.Open("orders", options);
await orders.AppendAsync(Encoding.UTF8.GetBytes("order-1234 placed"));

// Subscribe. The subscriber name identifies the stored position.
await using var billing = SqliteSubscription.Open("billing", "orders", options);

await foreach (EventEnvelope message in billing.ReadAsync(cancellationToken))
{
    try
    {
        await Handle(message.Payload);
        await billing.CompleteAsync(message);
    }
    catch (Exception ex)
    {
        await billing.FailAsync(message, ex.Message);  // dead-lettered after MaxAttempts failures
    }
}
```

Settle every message you read: call `CompleteAsync`, `FailAsync`, `DeferAsync` or `DeadLetterAsync`.
The [getting started](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/tutorials/getting-started.md) tutorial continues from here.

## When to use it

Chimera runs on one machine. For more than one node, use a network broker such as NATS JetStream or Azure
Service Bus.

Rules and limits:

- Each file has one writer. Open each `SqliteTopicLog` and each subscriber's `SqliteSubscription` in one
  process only. A publisher and its subscribers can run in different processes.
- Retention removes records by age, size or count (7 days by default), read or unread. `MessagesLostException`
  reports each loss to the subscriber.
- Append order is total and stable. Delivery order can differ from append order.
- The default payload limit is 256 KB.
- Chimera stores payloads as given. Encrypt sensitive payloads before you publish them.
- Consume throughput is about 800 messages a second with 16 in flight, on a 4-core machine. Publish
  throughput is close to NATS JetStream.

[Limits, and how you find out](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/explanation/limits.md) gives the detail.

## Packages

| Package | Contents |
|---|---|
| `SoEx.Messaging.Chimera` | The SQLite implementation: `SqliteTopicLog` and `SqliteSubscription`. |
| `SoEx.Messaging.Chimera.Abstractions` | The interfaces, options and message types. |
| `SoEx.Messaging.Chimera.Testing` | `ChimeraInspector`: subscriber state and message status, for tests. |

Chimera is a standalone library. The SoEx event transport for Chimera is `SoEx.Transport.Chimera`, in the
[SoEx](https://github.com/BlairMoir/SoEx) repository.

## Documentation

- [Getting started](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/tutorials/getting-started.md)
- [Integrating with a messaging framework](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/how-to/integrating.md)
- [Testing a system built on Chimera](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/how-to/testing.md)
- [API reference](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/reference/api.md)
- [Files on disk](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/reference/files.md)
- [Why a log and not a queue](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/explanation/log-not-queue.md)
- [Limits, and how you find out](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/docs/explanation/limits.md)

## Status

Release candidate for 1.0.0. Report problems in the
[issue tracker](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/issues).

## License

[MIT](https://github.com/Schnauzer-Software-Ltd/SoEx-Messaging-Chimera/blob/main/LICENSE)
