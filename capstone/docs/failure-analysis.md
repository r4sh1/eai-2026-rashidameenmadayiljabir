# Failure analysis

This document records ten failure scenarios exercised against the running system. Correlation IDs, states, trace operations, provider effects, queue observations and alerts below are taken from the recorded runs. Where a scenario exposed a defect or the available evidence does not prove a recovery action, that limitation is stated explicitly.

## 1. Payment declined

**Trigger**

Configured payment authorization to fail for order `WEB-PAYMENT-DECLINED-20261119-001` with HTTP 422 and code `payment_declined`, then submitted a web order.

- Correlation ID: `df7ac7dd-6f61-4c5d-8fb7-c118030791cc`

**What the system does**

Intake accepts, translates and publishes the order. The orchestrator starts the saga. The payment provider rejects authorization on the first attempt; the saga records a failed outcome and the notifier sends the order-not-completed notice.

Trace excerpt (`GET /trace/df7ac7dd-6f61-4c5d-8fb7-c118030791cc`):

```text
intake.receive                 success
intake.translate               success
intake.publish                 success
orchestrator.saga.consume      success
orchestrator.workflow.start   success
worker.saga.outcome            processing
worker.payment                 failed   attempt=1 HTTP 422 payment_declined
worker.saga.outcome            failed   code=payment_declined
notifier.notify.consume        success
notifier.notify.send           success
```

**Final state**

`GET /orders/df7ac7dd-6f61-4c5d-8fb7-c118030791cc` returned `state=failed`, `code=payment_declined`. The payment log records the failed authorization, but there is no charge effect for this order. No inventory or shipping step ran. The notification was sent successfully.

**Alert**

`none, by design` for this isolated payment rejection: it creates no DLQ entry, invalid message, missing consumer, queue lag or stuck saga. The `dlq_not_empty` alert can fire independently if another message is already in the DLQ.

**Operator action**

Check the provider and order using `GET http://localhost:4801/admin/logs` and `GET http://localhost:8800/orders/df7ac7dd-6f61-4c5d-8fb7-c118030791cc`. Correct the payment details or provider issue and submit a new order with a new idempotency key; do not treat the rejected authorization as a successful charge.

## 2. Inventory unavailable

**Trigger**

Configured the inventory reserve operation to fail with HTTP 503 for order `WEB-T3-INV-FAIL-20261119-001`, then submitted a web order.

- Correlation ID: `f4133736-7a6d-4f7d-86d9-3e94f3522546`

**What the system does**

Payment authorization succeeds. Inventory reservation fails three times. The orchestrator then compensates the completed payment by issuing a refund, records the saga as compensated, and sends an order-not-completed notification.

Trace excerpt (`GET /trace/f4133736-7a6d-4f7d-86d9-3e94f3522546`):

```text
intake.receive                 success
intake.translate               success
intake.publish                 success
orchestrator.saga.consume      success
orchestrator.workflow.start   success
worker.payment                 success
worker.inventory               failed   attempt=1 HTTP 503 service_unavailable
worker.inventory               failed   attempt=2 HTTP 503 service_unavailable
worker.inventory               failed   attempt=3 HTTP 503 service_unavailable
worker.payment_refund          success
worker.saga.outcome            success   state=compensated
notifier.notify.consume        success
notifier.notify.send           success
```

**Final state**

`GET /orders/f4133736-7a6d-4f7d-86d9-3e94f3522546` returned `state=compensated`, `code=service_unavailable`. The payment mock records one charge followed by one refund. Inventory has no reservation effect; shipping has no shipment effect. The failure notice was sent.

**Alert**

`none, by design` for the completed compensation: the saga is not left stuck and the failed provider call does not itself satisfy a README §4.9 alert predicate. A separate non-empty DLQ would fire `dlq_not_empty`.

**Operator action**

Inspect `GET http://localhost:4802/admin/logs` and `GET http://localhost:4802/admin/config`. Restore inventory availability with `POST http://localhost:4802/admin/config` after the provider issue is corrected. The order has already been compensated; submit a new order if the customer still wants the purchase.

## 3. Shipping timeout

**Trigger**

Configured shipping creation to take 3000 ms, exceeding the 2500 ms request timeout, for order `WEB-T3-SHIPPING-TIMEOUT-20261119-001`.

- Correlation ID: `96366196-bf61-46b0-9aea-86bea6d1b36e`

**What the system does**

Payment and inventory succeed. The shipping request times out from the caller's perspective and is attempted three times. The saga compensates inventory and payment and finishes as compensated. The mock nevertheless records a shipment effect for each delayed request that completed server-side after the client timed out.

Trace excerpt (`GET /trace/96366196-bf61-46b0-9aea-86bea6d1b36e`):

```text
worker.payment                 success
worker.inventory               success
worker.shipping                success   attempt=1 duration≈3005ms
worker.shipping                success   attempt=2 duration≈3005ms
worker.shipping                success   attempt=3 duration≈3005ms
worker.inventory_release       success
worker.payment_refund          success
worker.saga.outcome            success   state=compensated code=timeout
notifier.notify.send           success
```

**Final state**

The order is `compensated` with code `timeout`. The payment mock records one charge and one refund. Inventory records one reservation and one release. **Shipping records three shipment effects** for this order, all with the same idempotency key and causation ID. This is a duplicate-side-effect defect: the client timed out, but the mock completed each create request.

**Alert**

`none, by design` for this completed compensation. The timeout does not itself correspond to a README §4.9 alert key. `stuck_sagas` would fire only if the saga appeared in `GET /sagas/stuck`.

**Operator action**

Check the shipping mock with `GET http://localhost:4803/admin/logs` and `GET http://localhost:4803/admin/effects`. Reconcile the repeated shipment records with the shipping provider before attempting any manual fulfilment. Do not assume a client timeout means the provider did not create a shipment.

## 4. A refund fails, then operator compensation

**Trigger**

For order `WEB-S4-REFUND-FAIL-20261119-001`, configured shipping creation to fail with HTTP 503 (`shipping_unavailable`) and payment refund to fail with HTTP 503 (`refund_failed`).

- Correlation ID: `4b2ae731-83fa-40fd-9a85-5ead37683bd9`

**What the system does**

Payment and inventory reservation succeed. Shipping fails. The first compensation releases inventory successfully, but the refund fails, leaving the saga in `compensation_failed`. After the refund provider is restored, the operator calls the hand-rolled compensation endpoint. The second attempt performs the missing payment refund; it does not repeat the inventory release that already succeeded.

Trace excerpt (`GET /trace/4b2ae731-83fa-40fd-9a85-5ead37683bd9`):

```text
orchestrator.saga.consume      success
orchestrator.payment           success
orchestrator.inventory         success
orchestrator.shipping          failed   attempt=1 shipping_unavailable
orchestrator.inventory_release success  attempt=1
orchestrator.payment_refund    failed   attempt=1 refund_failed
orchestrator.saga.outcome      success  state=compensation_failed
orchestrator.payment_refund    success  attempt=1
orchestrator.saga.operator_compensate success state=compensated
orchestrator.saga.outcome      success  state=compensated
notifier.notify.send           success
```

**Final state**

The final order state is `compensated`, with original failure code `shipping_unavailable`. The payment mock records exactly one charge and one successful refund. Inventory records one reservation and one release. No shipment effect was created.

**Alert**

`stuck_sagas` fires while the saga is listed by `GET /sagas/stuck`; it clears after the operator compensation completes and the saga is no longer stuck. A non-empty DLQ, if present for another scenario, independently fires `dlq_not_empty`.

**Operator action**

Restore the refund provider using `POST http://localhost:4801/admin/config`, then call:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8802/sagas/4b2ae731-83fa-40fd-9a85-5ead37683bd9/compensate"
```

Verify the resulting order and provider effects. The operator endpoint is for hand-rolled sagas; Temporal-owned sagas are rejected by this endpoint with `temporal_operator_compensation_unsupported`.

## 5. A payment outage before step 1: retry, parked, replay

**Trigger**

Configured payment authorization to fail with HTTP 503 and `payment_unavailable` for order `WEB-S5-REPLAY-FIX-20261119-001`. The order was submitted while the provider was unavailable.

- Correlation ID: `a21b629f-f6cd-4f37-822a-d445f037be27`

**What the system does**

The first-step payment operation fails. The orchestrator retries the saga message twice more; after the third failed attempt, it dead-letters the message and records the order as parked. Payment authorization is restored and the DLQ replay endpoint is called with this correlation ID only. Intake republishes the matching message. The updated orchestrator recognizes the parked first-step replay, clears the parked outcome, resumes the saga, and completes payment, inventory, shipping and notification.

Trace excerpt (`GET /trace/a21b629f-f6cd-4f37-822a-d445f037be27`):

```text
orchestrator.payment           failed   payment_unavailable
orchestrator.saga.retry        retried  attempt=1 maxRetries=3
orchestrator.payment           failed   payment_unavailable
orchestrator.saga.retry        retried  attempt=2 maxRetries=3
orchestrator.payment           failed   payment_unavailable
orchestrator.saga.dead_letter  dead_lettered attempt=3
orchestrator.saga.outcome      success  state=parked
intake.dlq.replay              success  originQueue=saga.queue
orchestrator.saga.consume      success
orchestrator.saga.resume       success  reason=parked_first_step_replay
orchestrator.payment           success
orchestrator.inventory         success
orchestrator.shipping          success
orchestrator.notification      success
orchestrator.saga.outcome      success  state=completed
```

**Final state**

The order first reached `parked` with `payment_unavailable`, then `completed`. The payment mock records exactly one charge of EUR 90.30 for this order. The replay response was `replayed=1, remaining=1`. The remaining DLQ entry was the unrelated notification-outage message (`b0b3871a-0bea-4e84-ba82-29796a9004ff`); it was preserved.

**Alert**

`dlq_not_empty` fires while the parked saga message is in `orders.dlq` (and remains firing if any other DLQ entry remains). It clears only when the DLQ is empty. The other alert predicates depend on their own queue/consumer metrics.

**Operator action**

Restore the payment provider, then replay only the affected correlation ID:

```powershell
$cid = "a21b629f-f6cd-4f37-822a-d445f037be27"
$body = @{ correlationId = $cid } | ConvertTo-Json -Compress
$bytes = [System.Text.Encoding]::UTF8.GetBytes($body)

Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8800/dlq/replay" `
  -ContentType "application/json; charset=utf-8" `
  -Body $bytes
```

Verify `GET /orders/$cid`, `GET /trace/$cid`, and `GET http://localhost:4801/admin/effects`. Do not use `{}` when unrelated DLQ messages must remain untouched.

## 6. A failure-notice delivery outage

**Trigger**

For order `WEB-NOTIFICATION-OUTAGE-20261119-001`, payment authorization was configured to fail with `payment_declined`, while notification delivery was configured to fail with HTTP 503 and `notification_unavailable`.

- Correlation ID: `b0b3871a-0bea-4e84-ba82-29796a9004ff`

**What the system does**

The payment rejection fails the order. The notifier then tries to deliver the `order_not_completed` notice three times. All three sends fail, and the notification message is dead-lettered. The trace records the notification retry and dead-letter operations.

Trace excerpt (`GET /trace/b0b3871a-0bea-4e84-ba82-29796a9004ff`):

```text
worker.payment                 failed   HTTP 422 payment_declined
worker.saga.outcome            failed   code=payment_declined
notifier.notify.consume        failed   attempt=1 HTTP 503 notification_unavailable
notifier.notify.send           failed   attempt=1
notifier.notify.retry          retried  attempt=1
notifier.notify.send           failed   attempt=2
notifier.notify.consume        failed   attempt=2 retryScheduled=true
notifier.notify.retry          retried  attempt=2
notifier.notify.send           failed   attempt=3
notifier.notify.consume        dead_lettered attempt=3
notifier.notify.dead_letter    dead_lettered attempt=3
```

**Final state**

The order is `failed` with `payment_declined`. The payment authorization did not create a charge effect. The notification mock records three failed send attempts and no successful notification effect. The DLQ entry has correlation ID `b0b3871a-0bea-4e84-ba82-29796a9004ff`, origin queue `notifications.queue`, three attempts and last error `notification_unavailable`.

**Alert**

`dlq_not_empty` fires because the DLQ depth is greater than zero. In the captured metrics, `notifier_consumer_missing`, `notifier_consumer_lag`, and the other non-DLQ alerts were not firing. The metrics snapshot also showed one ready message in `orders.dlq`.

**Recovery and operator action**

The notification provider was restored (`send.failMode=never`) and the operator inspected `GET http://localhost:8800/dlq`. The DLQ contained exactly one entry matching this correlation ID, with `originQueue=notifications.queue`, `attempts=3` and `lastError=notification_unavailable`. The operator replayed only this message using `POST http://localhost:8800/dlq/replay` with `{"correlationId":"b0b3871a-0bea-4e84-ba82-29796a9004ff"}`.

Recovery trace excerpt (same correlation ID, after the original dead-letter sequence):

```text
intake.dlq.replay          success       originQueue=notifications.queue
notifier.notify.send       success       attempt=1
notifier.notify.consume    success       attempt=1
```

The notification mock then recorded exactly one successful effect for `WEB-NOTIFICATION-OUTAGE-20261119-001`: `kind=order_not_completed`, `reason=payment_declined`, sequence 18. A subsequent `GET /dlq` returned `count=0`. The order itself correctly remained `failed` with `payment_declined`; replay recovered the failure notice and did not retry payment. The `dlq_not_empty` alert applied while the message was dead-lettered and cleared after the DLQ was drained.

The operator should verify the provider configuration, inspect the matching DLQ entry, replay only the intended correlation ID, then confirm the successful `notifier.notify.send` trace, one notification effect and an empty DLQ. Do not replay unrelated DLQ entries as part of this recovery.

## 7. The orchestrator crashes mid-saga

**Trigger**

For order `WEB-T7-ORCH-CRASH-20261119-002`, shipping creation was delayed for 15000 ms. The order was submitted, then the orchestrator container was stopped while the workflow was in progress. The orchestrator was started again afterward.

- Correlation ID: `29ef6cb7-b1a8-4474-85d8-33f6951a396b`

**What the system does**

The orchestrator had already consumed the saga and started the Temporal workflow before it stopped. The worker and Temporal continued processing while the orchestrator was offline. The deliberately slow shipping request exceeded the request timeout; the workflow compensated inventory and payment and published a compensated outcome. After restart, the orchestrator reconnected and consumed the outcome. The persisted saga record showed owner `temporal:...`, state `compensated`, and outcome routing key `order.compensated`.

Trace excerpt (`GET /trace/29ef6cb7-b1a8-4474-85d8-33f6951a396b`):

```text
orchestrator.saga.consume      success
orchestrator.workflow.start   success
worker.payment                 success
worker.inventory               success
worker.shipping                success   attempt=1 duration≈15s
worker.shipping                success   attempt=2 duration≈15s
worker.shipping                success   attempt=3 duration≈15s
worker.inventory_release       success
worker.payment_refund          success
worker.saga.outcome            success   state=compensated code=timeout
notifier.notify.send           success
```

After restart, `GET /health` returned `status=ok`, broker `up`, with consumers on `saga.queue` and `orchestrator.temporal.outcomes.queue`.

**Final state**

The order is `compensated` with code `timeout`. The payment mock records one charge and one refund. Inventory records one reservation and one release. **Shipping records three shipment effects** for this order with the same idempotency key and causation ID. This duplicate side effect is a known limitation of the delayed in-flight shipping request; the crash/restart did not stop the worker from completing the workflow.

**Alert**

`none, by design` after recovery: the orchestrator consumer reconnected and the workflow reached a terminal compensated outcome. `saga_consumer_missing` would fire only while `saga.queue` has zero consumers; `stuck_sagas` would fire only if a hand-rolled stuck saga were reported by `/sagas/stuck`.

**Operator action**

Restart and verify the orchestrator:

```powershell
docker start cap-orchestrator
Invoke-RestMethod "http://localhost:8802/health"
```

Then inspect `GET http://localhost:8801/trace/29ef6cb7-b1a8-4474-85d8-33f6951a396b` and the payment, inventory and shipping mock `/admin/effects` endpoints. Reconcile repeated shipment effects with the shipping provider.

## 8. A broker restart

**Trigger**

Restarted RabbitMQ with `docker restart cap-rabbitmq` while the application stack was running.

**What the system does**

RabbitMQ came back healthy. Intake, orchestrator, worker and notifier containers remained up. The orchestrator health endpoint returned `status=ok`, broker `up`, and consumers on `saga.queue` and `orchestrator.temporal.outcomes.queue`. Before and after the restart, RabbitMQ reported the same 11 durable queues. Queue counts, including the one existing DLQ message and the debug capture queues, persisted across the restart. The observer metrics after restart showed one consumer each on `saga.queue`, `notifications.queue` and `trace.store`.

There is no order-specific `GET /trace/:cid` for the broker restart itself; it is a broker/service recovery event, not an order hop.

**Final state**

No order was submitted as part of the restart verification, so there is no order state or provider effect to report for this scenario. The durable queues remained present with their observed message counts. `orders.dlq` retained one ready message; the debug capture queues also retained their messages.

**Alert**

`dlq_not_empty` was firing because `orders.dlq` still contained one message. The other observed alerts were not firing. The broker restart itself did not produce `saga_consumer_missing` or `notifier_consumer_missing` after recovery because consumers were present.

**Operator action**

Run:

```powershell
docker compose ps rabbitmq orchestrator worker notifier intake
docker exec cap-rabbitmq rabbitmqctl list_queues name durable messages_ready messages_unacknowledged
Invoke-RestMethod "http://localhost:8802/health"
Invoke-RestMethod "http://localhost:8801/ops/metrics"
```

If the broker is healthy but a consumer count remains zero, inspect and restart only the affected application service. Do not purge durable queues to clear an alert.

## 9. A duplicate submission with a new Idempotency-Key

**Trigger**

Submitted the same business order twice using two different `Idempotency-Key` values.

- Order ID: `WEB-T6-DUPLICATE-VERIFY-20261119-001`
- Original correlation ID: `311fa821-036c-45cc-9846-2fec8cbf8b6b`
- Second submission correlation ID: `fea3e994-83f3-482c-9cca-c9f184a6dff6`

**What the system does**

The original submission completed the checkout workflow. The second submission was identified as a duplicate of the original business order and returned duplicate status, referring to the original correlation ID. The trace also records a replayed Temporal outcome for the original workflow. No second business execution was created.

Trace excerpt (original order, `GET /trace/311fa821-036c-45cc-9846-2fec8cbf8b6b`):

```text
intake.receive                 success
intake.translate               success
intake.publish                 success
orchestrator.saga.consume      success
orchestrator.workflow.start   success
worker.payment                 success
worker.inventory               success
worker.shipping                success
worker.notification            success
worker.saga.outcome            completed
orchestrator.saga.outcome      success   replay=true engine=temporal
```

**Final state**

The original order is `completed`. The second submission returned `state=duplicate` and `duplicateOf=311fa821-036c-45cc-9846-2fec8cbf8b6b`. The original workflow had one payment charge, one inventory reservation, one shipment and one notification effect. In particular, the payment mock recorded exactly one charge for this order.

**Alert**

`none, by design`: a duplicate is handled as an idempotency/business-key outcome. It does not create a DLQ entry, invalid message, missing consumer, queue lag or stuck saga.

**Operator action**

Use the returned `duplicateOf` correlation ID to inspect the canonical order:

```powershell
Invoke-RestMethod "http://localhost:8800/orders/311fa821-036c-45cc-9846-2fec8cbf8b6b"
Invoke-RestMethod "http://localhost:8801/trace/311fa821-036c-45cc-9846-2fec8cbf8b6b"
```

Do not fulfil or charge the duplicate submission separately.

## 10. A malformed message

**Trigger**

Published the raw bytes `{ not json` to `orders.exchange` using the manual broker scenario test. The test checked the invalid queue, retry exchange and DLQ exchange.

**What the system does**

The intake consumer cannot parse the body as JSON. It routes the original message to the invalid-message exchange/queue and acknowledges the original delivery. The test verified that the original bytes were preserved, exactly one message arrived in `orders.invalid`, and no retry-exchange or DLQ-exchange publish occurred.

There is no correlated `GET /trace/:cid` excerpt for this malformed body: it cannot be parsed into an order and the test did not establish an order correlation ID. The broker-level test result was:

```json
{
  "published": true,
  "invalidArrived": true,
  "invalidCount": 1,
  "originalBytesPreserved": true,
  "retryMessages": 0,
  "dlqExchangeMessages": 0,
  "dlqCount": 1,
  "expectedInvalidCount": 1,
  "expectedDlqCount": 1
}
```

The DLQ count of one was the pre-existing notification-outage entry; the malformed message did not add a DLQ entry.

**Final state**

No order record or provider effect is created for the malformed message. One raw message is present in `orders.invalid`, with the original bytes preserved. The existing notification DLQ message remains separate.

**Alert**

`invalid_not_empty` fires because `invalidDepth` is greater than zero. `dlq_not_empty` was also firing in the captured environment because the unrelated notification-outage message remained in `orders.dlq`; that DLQ alert was not caused by the malformed message.

**Operator action**

Inspect the invalid queue and its original message using RabbitMQ management or the broker tools. Correct the upstream producer/schema issue, then submit a valid order. Do not replay malformed bytes as an order; the invalid queue is separate from the retry/DLQ path.
