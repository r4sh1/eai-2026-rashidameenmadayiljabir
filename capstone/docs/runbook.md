# Runbook

This runbook covers the seven alerts returned by `GET /ops/metrics` on the observer. The alert predicates and values match the implementation in `observer/src/index.ts`. Both queue-lag alerts use `LAG_THRESHOLD = 3`.

## First response checklist

1. Read `GET http://localhost:8801/ops/metrics` and note the alert key, value and threshold.
2. Check `GET http://localhost:8801/health` on the observer and the health endpoint of the affected service.
3. Inspect service logs with `docker compose logs --tail=200 <service>`.
4. For a specific order, use `GET /trace/:correlationId` on the observer and inspect the relevant mock's `/admin/logs`.
5. For dead-lettered messages, inspect `GET /dlq` on intake. For stuck sagas, inspect `GET /sagas/stuck` on the orchestrator.
6. Fix the underlying cause before replaying or compensating. Do not repeatedly replay a message while its original failure remains.

## Alert: `dlq_not_empty`

**Meaning**

The alert fires when `dlqDepth > 0`. `dlqDepth` counts ready and unacknowledged messages in `orders.dlq`. The threshold is `null`; any message is worth investigating because it has already exhausted its normal retry path.

**Possible causes**

- A downstream mock or service remained unavailable until the consumer reached its retry limit.
- A message repeatedly failed because of invalid business data, a persistent timeout, or a code/configuration error.

**Diagnose**

```powershell
Invoke-RestMethod http://localhost:8801/ops/metrics
Invoke-RestMethod http://localhost:8800/dlq
docker compose logs --tail=200 orchestrator
docker compose logs --tail=200 notifier
```

Check the DLQ entry's `originQueue`, `attempts`, `lastError` and correlation ID. For an order, inspect `GET /trace/:correlationId` and the relevant mock's `/admin/logs`.

**Fix**

- Restore the failed dependency or correct the configuration/data problem first.
- For a replayable saga or notification entry, use the DLQ replay endpoint exposed by intake: `POST http://localhost:8800/dlq/replay` with the entry identifier expected by that endpoint.
- Do not replay entries from `orders.invalid`; those are quarantined malformed messages and need correction/re-submission rather than blind replay.

**Verify**

Call `/ops/metrics` again. `dlq_not_empty` should show `value: 0` and `firing: false`; `GET /dlq` should no longer list the replayed entry. Confirm the expected downstream effect occurred once.

## Alert: `invalid_not_empty`

**Meaning**

The alert fires when `invalidDepth > 0`. `invalidDepth` counts ready and unacknowledged messages in `orders.invalid`. Its threshold is `null`: any quarantined message needs inspection.

**Possible causes**

- A producer sent malformed JSON or a message that cannot be parsed.
- A message parsed as JSON but failed the required envelope or canonical-order validation.

**Diagnose**

```powershell
Invoke-RestMethod http://localhost:8801/ops/metrics
Invoke-RestMethod http://localhost:8800/dlq
docker compose logs --tail=200 intake
docker compose logs --tail=200 orchestrator
docker compose logs --tail=200 notifier
```

Inspect the invalid entry's original bytes and headers, then compare its shape with the expected canonical order/envelope. Use the correlation ID, if present, to find related trace hops.

**Fix**

Correct the producer or mapping that created the malformed message. Resubmit a corrected order through the normal intake endpoint with a new appropriate idempotency key. Keep the original quarantined message for investigation; do not route malformed bytes into the saga or notification consumer.

**Verify**

The corrected order should be accepted and processed normally. Confirm `invalidDepth` returns to zero after the invalid entry is removed through the project's supported operational procedure; `invalid_not_empty` should then be `firing: false`.

## Alert: `saga_consumer_missing`

**Meaning**

The alert fires when the consumer count for `saga.queue` is zero. Its threshold is `null`. The observer reads the queue's current consumer count from RabbitMQ.

**Possible causes**

- The orchestrator process stopped, crashed, or is restarting.
- The orchestrator cannot connect to RabbitMQ or failed while declaring/starting its consumer.

**Diagnose**

```powershell
docker compose ps orchestrator rabbitmq
docker compose logs --tail=200 orchestrator
docker compose logs --tail=200 rabbitmq
Invoke-RestMethod http://localhost:8802/health
Invoke-RestMethod http://localhost:8801/ops/metrics
```

Check whether the broker is healthy, whether the orchestrator health check reports its broker/consumer status, and whether `saga.queue` has a consumer.

**Fix**

Restore RabbitMQ connectivity and correct any broker URL, credentials, topology or startup error. If the container exited, start it with `docker compose up -d orchestrator`. The service should reconnect and re-declare its topology without relying on a manual message replay.

**Verify**

`GET /health` on orchestrator should return healthy status, `saga.queue` should report at least one consumer, and `saga_consumer_missing` should show `firing: false`.

## Alert: `notifier_consumer_missing`

**Meaning**

The alert fires when the consumer count for `notifications.queue` is zero. Its threshold is `null`.

**Possible causes**

- The notifier process stopped, crashed, or is restarting.
- The notifier cannot connect to RabbitMQ or failed to start its notification consumer.

**Diagnose**

```powershell
docker compose ps notifier rabbitmq
docker compose logs --tail=200 notifier
docker compose logs --tail=200 rabbitmq
Invoke-RestMethod http://localhost:8803/health
Invoke-RestMethod http://localhost:8801/ops/metrics
```

Check the notifier health response and the `notifications.queue` consumer count. Inspect the queue's ready count to see whether events accumulated while no consumer was present.

**Fix**

Restore broker connectivity and resolve the startup/configuration error. If the notifier container exited, start it with `docker compose up -d notifier`. Confirm that the notification mock is reachable before allowing the queue to drain.

**Verify**

The notifier health check should be healthy, `notifications.queue` should have a consumer, and `notifier_consumer_missing` should be `firing: false`. Check the mock logs to confirm notifications were delivered.

## Alert: `saga_consumer_lag`

**Meaning**

The alert fires when the number of ready messages in `saga.queue` is greater than **3**. The configured threshold is `LAG_THRESHOLD = 3` in `observer/src/index.ts`.

**Why the threshold is 3**

A small backlog is normal during short bursts, so alerting on the first waiting message would be noisy. A value greater than three is a practical early warning that accepted work is accumulating beyond a brief burst. This is a fixed queue-depth threshold, not a time-based latency measurement; investigate it together with consumer count, unacknowledged messages and service logs.

**Possible causes**

- Order arrival rate is higher than the orchestrator's processing capacity or configured concurrency.
- A downstream payment, inventory or shipping mock is slow or repeatedly timing out, holding saga work and reducing throughput.

**Diagnose**

```powershell
Invoke-RestMethod http://localhost:8801/ops/metrics
docker compose logs --tail=200 orchestrator
docker compose logs --tail=200 rabbitmq
```

Check `saga.queue` ready, unacked and consumers. For a representative correlation ID, inspect `GET /trace/:correlationId` and the relevant mock's `/admin/logs`.

**Fix**

Restore slow dependencies, correct repeated failures, and remove any resource bottleneck identified in the orchestrator logs. Do not increase concurrency as the first response; first confirm that downstream services can safely handle the extra calls.

**Verify**

The ready count should fall to **3 or fewer**, and `saga_consumer_lag` should show `firing: false`. Confirm that the consumer remains present and orders reach their expected terminal states.

## Alert: `notifier_consumer_lag`

**Meaning**

The alert fires when the number of ready messages in `notifications.queue` is greater than **3**. It uses the same `LAG_THRESHOLD = 3` constant as the saga queue alert.

**Why the threshold is 3**

A short burst of failure/compensation notifications can temporarily create a small queue. Waiting until more than three are ready avoids paging on a single transient event while still identifying a growing backlog. This measures queue depth rather than elapsed delivery time, so check the notifier consumer count and mock response times as well.

**Possible causes**

- The notification mock is slow or unavailable, causing notifier requests to time out or retry.
- The notifier is stopped, unhealthy, or processing more slowly than outcome events are arriving.

**Diagnose**

```powershell
Invoke-RestMethod http://localhost:8801/ops/metrics
docker compose ps notifier
docker compose logs --tail=200 notifier
docker compose logs --tail=200 notification-mock
```

Check `notifications.queue` ready, unacked and consumers. Inspect the notification mock's `/admin/logs` and the DLQ for exhausted notification retries.

**Fix**

Restore the notification mock and notifier connectivity, then allow the notifier to drain the queue. If the notifier process stopped, start it with `docker compose up -d notifier`. Fix the cause of repeated notification failures before replaying any dead-lettered notification.

**Verify**

The ready count should fall to **3 or fewer**, `notifier_consumer_lag` should be `firing: false`, and the notification mock logs should show the expected delivery.

## Alert: `stuck_sagas`

**Meaning**

The alert fires when `stuckSagas > 0`. The value is obtained from the orchestrator's `GET /sagas/stuck` endpoint; the threshold is `null`.

**Possible causes**

- The orchestrator restarted while a saga was in progress and the saga requires operator attention.
- A compensation action failed, leaving the saga in `compensation_failed` and requiring a safe retry of only the unfinished compensations.

**Diagnose**

```powershell
Invoke-RestMethod http://localhost:8801/ops/metrics
Invoke-RestMethod http://localhost:8802/sagas/stuck
docker compose logs --tail=200 orchestrator
```

Review each saga's correlation ID, business key, state, completed steps and compensated steps. Inspect `GET /trace/:correlationId` and the relevant mock logs before taking action.

**Fix**

Do not restart or replay the saga message to make it run from the beginning. For a saga in `needs_attention`, verify the external effects and then use the operator compensation endpoint:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8802/sagas/<correlationId>/compensate"
```

For `compensation_failed`, inspect which compensation already succeeded and retry only after the failing dependency is healthy. The implementation should preserve completed compensation records so successful compensations are not repeated.

**Verify**

The operator endpoint should report the resulting state. Recheck `GET /sagas/stuck`; after all listed sagas are resolved, `stuckSagas` should be zero and `stuck_sagas` should be `firing: false`. Confirm the final trace/outcome and external mock effects.
