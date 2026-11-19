# Order Processing Capstone

## Overview

This project processes web, mobile and B2B orders through a RabbitMQ-based order workflow. The intake service normalizes and validates orders, the orchestrator coordinates the saga, provider mocks handle payment/inventory/shipping/notification operations, and the observer exposes traces and operational metrics.

The default engine is the hand-rolled saga. An optional Temporal engine runs the `checkout` workflow on the `checkout` task queue.

## Run the system

Requirements: Docker Compose 2.20 or later, Node.js 20 or later, and at least 4 GB of memory available to Docker.

From this directory in PowerShell:

```powershell
$env:SAGA_ENGINE = "handrolled"
docker compose up -d --build --wait
docker compose ps
```

The default mode is `handrolled`.

To run the advanced Temporal mode:

```powershell
$env:SAGA_ENGINE = "temporal"
docker compose --profile temporal up -d --build --wait
```

To return to hand-rolled mode:

```powershell
docker compose --profile temporal down
$env:SAGA_ENGINE = "handrolled"
docker compose up -d --build --wait
```

Do not use `docker compose down -v` for normal restarts; it removes persistent volumes and project data.

## Services and useful URLs

| Service | URL | Purpose |
|---|---|---|
| Intake | `http://localhost:8800` | Accepts and normalizes orders; exposes order status and DLQ operations |
| Observer | `http://localhost:8801` | Trace lookup, operational metrics and alerts |
| Orchestrator | `http://localhost:8802` | Saga processing and operator endpoints |
| Notifier | `http://localhost:8803` | Delivers failed-order notifications |
| Temporal UI | `http://localhost:8240` | Workflow history and task-queue inspection in Temporal mode |
| RabbitMQ management | `http://localhost:15680` | Exchanges, queues and message inspection |

Submit an order with `POST http://localhost:8800/orders` using one of the fixtures in `tests/fixtures/` and an `Idempotency-Key` header. Use the returned `correlationId` with:

- `GET http://localhost:8800/orders/<correlationId>` for order status.
- `GET http://localhost:8801/trace/<correlationId>` for the trace.
- `GET http://localhost:8801/ops/metrics` for alert and queue metrics.
- `GET http://localhost:8802/sagas/stuck` for hand-rolled sagas requiring operator attention.

The provider mock administration endpoints are available on ports 4801–4804. Their `/admin/config`, `/admin/logs` and `/admin/effects` endpoints are used for controlled failure tests and effect verification.

## Tests

Run the public tests from the project root:

```powershell
npm --prefix tests run test:stage
```

To run through a particular stage, pass its number, for example:

```powershell
npm --prefix tests run test:stage -- 5
```

The public suite covers stages 0–6. The advanced Temporal checks T1–T7 are manual checks described in the original assignment brief and grading document; they are not covered by the public tests.

## Advanced component

Implemented.

The optional advanced engine uses the Temporal workflow type `checkout`, workflow ID equal to the order `businessKey`, and task queue `checkout`. The orchestrator starts the workflow from `saga.queue`. The workflow is implemented in `worker/src/workflows/checkout.ts`; provider calls, trace hops and outcome events are implemented as activities in `worker/src/activities/index.ts`.

The activity retry policy is three attempts, a 200 ms initial interval, a backoff coefficient of 2, and a 2,500 ms start-to-close timeout. HTTP 4xx provider responses are non-retryable. B2B orders skip payment.

## Known limitations

- A worker crash while a shipping request is in flight can cause the shipping activity to be attempted again after Temporal replays the workflow. The payment and inventory mock effects were verified as single effects in the tested recovery case; the shipping mock recorded a repeated create attempt.
- Temporal-owned workflows are not exposed through the hand-rolled saga operator-compensation store. Temporal executes workflow compensation, but the orchestrator's `/sagas/stuck` and `/sagas/:correlationId/compensate` endpoints do not provide operator control over Temporal workflow history.
- The Temporal activity publishes outcome messages with RabbitMQ `channel.publish`; it does not wait for publisher confirms before the activity returns. This is a delivery-guarantee limitation.
- Scenario 3 and Scenario 7 exposed repeated shipping create effects after timed-out requests. These are documented in `docs/failure-analysis.md`; payment and inventory compensation effects were verified separately.
