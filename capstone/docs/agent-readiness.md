
# Agent readiness

During this project, I worked on an order-processing system that uses RabbitMQ, an orchestrator, an intake service, a notifier and an observer. I considered how an AI agent could help monitor this system and identify problems.

For example, an agent could check an order's status, find failed messages in the dead-letter queue and identify sagas that need attention. However, I would not give it complete control because some operations can affect payments and inventory.

The tools below are my proposed design for a future AI agent. They are not an agent implementation that I have already deployed.

## Tools

I would provide five tools to the agent. Three would be used only to read information, while two would allow it to request operations.

### Read-only tools

**1. get_order_status**

This tool allows the agent to check the current status of an order.

- Endpoint: `GET /orders/{correlationId}`
- Service: Intake, port 8800.
- Input:

```typescript
type GetOrderStatusInput = {
  correlationId: string;
};
```

The response contains the order's state, business key, order ID and other available information. The agent can use it to understand whether an order is accepted, processing, completed, failed or needs attention.

**2. inspect_dead_letters**

This tool allows the agent to inspect messages that have failed processing.

- Endpoint: `GET /dlq`
- Service: Intake, port 8800.
- Input: No parameters.

It returns the message ID, correlation ID, original queue, number of attempts and last error. I made this a read-only operation because inspecting the DLQ should not remove or change any messages.

**3. list_stuck_sagas**

This tool allows the agent to find orders that need manual attention.

- Endpoint: `GET /sagas/stuck`
- Service: Orchestrator, port 8802.
- Input: No parameters.

It returns sagas in `needs_attention` or `compensation_failed` states. The agent can use this information to explain which steps have already completed and which orders require an operator.

### Write tools

**4. replay_dead_letters**

This tool allows an operator to request another attempt for a failed message.

- Endpoint: `POST /dlq/replay`
- Service: Intake, port 8800.
- Input:

```typescript
type ReplayDeadLettersInput = {
  correlationId?: string;
};
```

The correlation ID is optional. Without it, the endpoint requests replay of all eligible messages. With it, only matching messages are selected.

I would require human confirmation before this operation because replaying a message can start order processing again.

**5. compensate_saga**

This tool allows an operator to request compensation for a stuck saga.

- Endpoint: `POST /sagas/{correlationId}/compensate`
- Service: Orchestrator, port 8802.
- Input:

```typescript
type CompensateSagaInput = {
  correlationId: string;
};
```

The orchestrator performs outstanding compensations in reverse order. For example, it can release reserved inventory and refund a payment when those steps were completed.

I would not allow the agent to perform this operation automatically. It must first show the current saga state and explain the expected effects. A human operator must approve it.

## Injection paths

One important risk is that my system receives order information from external customers. Some of this information contains free text, so an AI agent must not automatically trust everything it reads.

The possible injection paths in my system include:

- Customer information and order fields received through `POST /orders`.
- Customer email addresses used by the notification service.
- Product names and item information received through JSON, XML and other supported order formats.
- Error messages and response data returned by the payment, inventory, shipping and notification services.
- Message bodies and headers received from RabbitMQ queues such as `saga.queue`, `notifications.queue` and `orders.dlq`.

For example, a customer could include unexpected instructions inside an order field. If an agent reads that information, it must treat it as customer data rather than instructions.

The same applies to provider error messages and RabbitMQ message contents. I would keep external data separate from the agent's trusted instructions and only allow it to use approved tools.

## Controls

I would follow a read-first approach. The agent should investigate a problem before requesting any operation that changes the system.

The main controls I would use are:

- The order status, dead-letter inspection and stuck-saga tools are read-only.
- Replaying messages requires human approval.
- Compensating a saga also requires human approval.
- Before compensation, the agent must inspect the saga and explain which completed steps may be reversed.
- The agent must not make payment refunds or inventory releases directly.
- The existing services remain responsible for validation, idempotency, retry handling and state transitions.
- The agent should not have access to RabbitMQ administrator credentials or provider secrets.
- Important operations should record the operator, action, correlation ID, approval and result.

I have already implemented trace recording in the project. The observer stores trace events in `/data/trace-store.jsonl`, and the notifier records sent notifications in `/data/notification.log`. These records can help an operator investigate what happened.

Human approval and agent-specific audit logging are proposed controls for a future integration, not features I claim to have implemented.

## Not exposed

I would never give an AI agent unrestricted access to the internal infrastructure or the following operations:

- Publishing arbitrary messages directly to RabbitMQ exchanges or queues.
- Deleting queues or exchanges or changing RabbitMQ bindings.
- Reading RabbitMQ passwords, API keys or other service secrets.
- Calling provider `/admin/config` or `/admin/reset` endpoints.
- Changing product prices or provider settings.
- Editing the saga store, idempotency store or trace files directly.
- Making refunds or releasing inventory outside the orchestrator's compensation process.
- Replaying messages from the invalid-message queue.
- Changing retry limits, concurrency settings or service configuration.
- Executing arbitrary terminal commands or Docker operations.

I would keep these restrictions because the existing services already contain the business logic for processing orders and handling failures. Giving an AI agent direct access could bypass that logic and cause duplicate payments, incorrect inventory changes or lost messages.

My approach is to use an AI agent as an assistant for monitoring and investigation, while keeping important business operations under human control.
