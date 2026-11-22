# TaskHub Go client

Submit immediate or scheduled tasks and process deliveries through the TaskHub v1 WebSocket API. The channel client uses one WebSocket and one credential for both roles, with up to 32 concurrent deliveries. Requires Go 1.23 or newer.

## Quick start

1. In TaskHub, create or select an enabled, unpaused queue. The examples default to the queue slug `linux-builds`.
2. Register a **Worker and producer** credential. Grant it access to the queue, match the queue’s exact capability set, and allow at least two worker slots.
3. Set `TASKHUB_TOKEN` in your shell to that credential’s token, then clone the repository and run the example.

```sh
git clone https://github.com/yuksbg/taskhub-client.git
cd taskhub-client
go run ./examples/channels -queue linux-builds -concurrency 2 -count 4 -id demo-001
```

The example submits four addition tasks, prints their accepted task IDs and recorded delivery IDs, and keeps consuming until you press Ctrl+C. View results under **TaskHub → Tasks**. If other eligible workers are connected, they may receive some of the tasks.

All examples accept `-url` and `-queue`. The default URL is `ws://localhost:5173/api/taskhub/v1/ws`. For a remote deployment, pass its full `wss://` endpoint. Keep tokens in the process environment.

## Choose an example

| Example | Credential | What it shows |
| --- | --- | --- |
| [Channels](examples/channels/main.go) | Worker and producer | `Default.Start`, a batch of submissions, and a configurable pool of goroutines on one socket |
| [Combined session](examples/combined/main.go) | Worker and producer | `Client.Open`, `Session.Submit`, and a single-task `Session.Work` handler on one socket |
| [Producer](examples/producer/main.go) | Producer | One immediate or scheduled submission |
| [Worker](examples/worker/main.go) | Worker | A callback that handles one task at a time until interrupted |

Run the callback-based combined example:

```sh
go run ./examples/combined -id combined-001 -a 5 -b 7
```

To use separate roles, set the worker token in the first terminal and the producer token in the second.

```sh
# Worker terminal
go run ./examples/worker

# Producer terminal
go run ./examples/producer -id addition-001 -a 5 -b 7
```

## Schedule tasks for later

Pass a future RFC 3339 timestamp with a time zone offset or `Z`. With a combined credential, this schedules all four tasks for the same time and keeps the worker connected.

```sh
go run ./examples/channels -queue linux-builds -concurrency 2 -count 4 -id later-batch-001 -run-at 2030-06-15T09:30:00+03:00
```

With a producer credential, this schedules one task and exits after TaskHub accepts it.

```sh
go run ./examples/producer -id addition-002 -run-at 2030-06-15T09:30:00+03:00
```

In Go code, set `Submission.ScheduledAt` to the desired `time.Time`. TaskHub accepts the submission immediately, then makes it eligible for execution at the requested time. If no eligible worker is connected, the task waits.

## Install in your Go project

From your application module, add the dependency.

```sh
go get github.com/yuksbg/taskhub-client@latest
```

Import it as `github.com/yuksbg/taskhub-client`. The package name is `taskhub`.

```go
import "github.com/yuksbg/taskhub-client"
```

## Use the channel client

Call `taskhub.Default.Start(ctx, taskhub.Config{URL: endpoint, Token: token, Queue: slug, Concurrency: 2})`. The call returns once TaskHub confirms the subscription. `Concurrency` must be between 1 and 32 and within the credential’s available slots. It sets the maximum number of outstanding deliveries on the connection. Start that many goroutines reading `Tasks` to process work in parallel.

| Channel | Direction | Use |
| --- | --- | --- |
| `Submit` | Send | Send a `Submission` with a stable request ID, queue slug, and payload. Set `ScheduledAt` to a future `time.Time` for delayed execution. |
| `Submitted` | Receive | Match `SubmissionResult` by request ID. A successful result contains an accepted task ID. |
| `Tasks` | Receive | Process each `Delivery` using its deadline-bound context. |
| `Outcomes` | Send | Send one `Outcome` per delivery with its `DeliveryID`, plus a result map or an error safe to store in task history. |
| `Recorded` | Receive | Receive the delivery ID after TaskHub confirms its outcome. |
| `Done` | Receive | Signals shutdown after the output channels close. Call `Err()` for the reason. |

Drain both reply channels while the client runs to keep submission and outcome reporting moving. Use string-keyed maps with JSON-compatible values for payloads and results. Received numbers are `float64`.

Cancel the context passed to `Start` to shut down, and select on `Done` or a canceled context when sending. Wait for `Done` before restarting the singleton. Connection loss ends the session. Restart it explicitly when appropriate.

## Retries and task outcomes

- Acceptance means TaskHub assigned a task ID. A recorded outcome means TaskHub confirmed the delivery result.
- If acceptance is uncertain, retry with the same request ID, queue, payload, tags, and scheduled time. Use a new request ID for new work.
- Use `errors.As` with `*taskhub.Error` to inspect server rejections. Where available, the error includes the task ID.
- Make handlers idempotent. TaskHub can redeliver work after a failed attempt or connection loss.
