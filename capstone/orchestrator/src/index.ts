import express from "express";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { randomUUID } from "node:crypto";
import {
  mkdir,
  readFile,
  rename,
  writeFile
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit.js";
import { beginTraceHop } from "../../shared/trace.js";

const app = express();

const PORT = Number(process.env.PORT ?? 8802);
const SAGA_ENGINE = (process.env.SAGA_ENGINE ?? "handrolled").toLowerCase();
const TEMPORAL_ADDRESS = process.env.TEMPORAL_ADDRESS ?? "localhost:7233";
const TEMPORAL_NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? "default";
const TEMPORAL_TASK_QUEUE = "checkout";
const TEMPORAL_WORKFLOW_TYPE = "checkout";
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://eai:eai-capstone@localhost:5680";

const PAYMENT_URL =
  process.env.PAYMENT_URL ?? "http://localhost:4801";
const INVENTORY_URL =
  process.env.INVENTORY_URL ?? "http://localhost:4802";
const SHIPPING_URL =
  process.env.SHIPPING_URL ?? "http://localhost:4803";
const NOTIFICATION_URL =
  process.env.NOTIFICATION_URL ?? "http://localhost:4804";

const REQUEST_TIMEOUT_MS = Number(
  process.env.REQUEST_TIMEOUT_MS ?? 2500
);
const MAX_RETRIES = Math.max(1, Number(process.env.MAX_RETRIES ?? 3));
const SAGA_STUCK_AFTER_MS = Math.max(0, Number(process.env.SAGA_STUCK_AFTER_MS ?? 10000));

const DATA_DIR = process.env.DATA_DIR ?? "./data";
const SAGA_STORE_PATH = join(DATA_DIR, "saga-store.json");

const SAGA_QUEUE = "saga.queue";
const TEMPORAL_OUTCOME_QUEUE = "orchestrator.temporal.outcomes.queue";
const OUTCOME_EXCHANGE = "orders.outcome";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const DLQ_EXCHANGE = "orders.dlq.exchange";

const INSTANCE_ID = randomUUID();

type StepName =
  | "payment"
  | "inventory"
  | "shipping"
  | "notification";

type CompensationName =
  | "payment_refund"
  | "inventory_release";

type SagaState =
  | "processing"
  | "parked"
  | "completed"
  | "failed"
  | "compensated"
  | "compensation_failed"
  | "needs_attention"
  | "duplicate";

interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  unitPrice: string;
  currency: string;
  taxRate: number;
}

interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  receivedAt: string;
  orderDate: string;
  customer: {
    name: string;
    email: string;
    address: {
      street: string;
      city: string;
      postalCode: string;
      country: string;
    };
  };
  items: CanonicalItem[];
  currency: string;
  status: string;
}

interface SagaStepRecord {
  step: StepName;
  status: string;
  at: string;
  [key: string]: unknown;
}

interface SagaRecord {
  businessKey: string;
  state: SagaState;
  steps: SagaStepRecord[];
  compensations: Array<{
    step: CompensationName;
    status: string;
    at: string;
    [key: string]: unknown;
  }>;
  owner: string;
  updatedAt: string;
  order?: CanonicalOrder;
  lastCausationId?: string;
  outcome?: {
    routingKey: string;
    envelope: OutcomeEvent;
  };
  [key: string]: unknown;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
  businessKeys: Record<string, string>;
}

interface OutcomeEvent {
  correlationId: string;
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  recipient: string;
  state: SagaState;
  code?: string;
  duplicateOf?: string;
  occurredAt: string;
}

class ProviderError extends Error {
  status?: number;

  constructor(message: string, status?: number) {
    super(message);
    this.name = "ProviderError";
    this.status = status;
  }
}

let brokerConnected = false;
let consumerReady = false;
let temporalOutcomeConsumerReady = false;
let connection: any = null;
let consumerChannel: any = null;
let publisherChannel: any = null;
let temporalClientPromise: Promise<Client> | undefined;

async function getTemporalClient(): Promise<Client> {
  if (!temporalClientPromise) {
    temporalClientPromise = (async () => {
      const temporalConnection = await Connection.connect({
        address: TEMPORAL_ADDRESS
      });
      return new Client({
        connection: temporalConnection,
        namespace: TEMPORAL_NAMESPACE
      });
    })().catch((error) => {
      temporalClientPromise = undefined;
      throw error;
    });
  }

  return temporalClientPromise;
}

function isWorkflowAlreadyStarted(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const candidate = error as { name?: string; message?: string };
  return (
    candidate.name === "WorkflowExecutionAlreadyStartedError" ||
    candidate.name === "WorkflowExecutionAlreadyStarted" ||
    /workflow execution.*already started/i.test(candidate.message ?? "")
  );
}

let sagaStore: SagaStore = {
  sagas: {},
  businessKeys: {}
};

function now(): string {
  return new Date().toISOString();
}

function isObject(
  value: unknown
): value is Record<string, any> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

function isValidOrder(value: unknown): value is CanonicalOrder {
  if (!isObject(value)) return false;

  if (
    typeof value.orderId !== "string" ||
    value.orderId.length === 0 ||
    !["standard", "express", "b2b"].includes(value.orderType) ||
    !["web", "mobile", "b2b"].includes(value.source) ||
    typeof value.receivedAt !== "string" ||
    typeof value.orderDate !== "string" ||
    typeof value.currency !== "string" ||
    !isObject(value.customer) ||
    typeof value.customer.email !== "string" ||
    !Array.isArray(value.items) ||
    value.items.length === 0
  ) {
    return false;
  }

  if (
    !isObject(value.customer.address) ||
    typeof value.customer.name !== "string"
  ) {
    return false;
  }

  for (const item of value.items) {
    if (
      !isObject(item) ||
      typeof item.productId !== "string" ||
      typeof item.productName !== "string" ||
      !Number.isInteger(item.quantity) ||
      item.quantity < 1 ||
      typeof item.unitPrice !== "string" ||
      !/^\d+\.\d{2}$/.test(item.unitPrice) ||
      typeof item.currency !== "string" ||
      typeof item.taxRate !== "number"
    ) {
      return false;
    }
  }

  return true;
}

async function saveSagaStore(): Promise<void> {
  await mkdir(dirname(SAGA_STORE_PATH), {
    recursive: true
  });

  const temporaryPath = join(
    dirname(SAGA_STORE_PATH),
    `.saga-store-${randomUUID()}.tmp`
  );

  await writeFile(
    temporaryPath,
    JSON.stringify(sagaStore, null, 2),
    "utf8"
  );

  await rename(temporaryPath, SAGA_STORE_PATH);
}

async function loadSagaStore(): Promise<void> {
  try {
    const content = await readFile(
      SAGA_STORE_PATH,
      "utf8"
    );

    const parsed = JSON.parse(content);

    if (
      isObject(parsed) &&
      isObject(parsed.sagas) &&
      isObject(parsed.businessKeys)
    ) {
      sagaStore = parsed as SagaStore;
    }
  } catch (error) {
    if (
      (error as NodeJS.ErrnoException).code !== "ENOENT"
    ) {
      throw error;
    }

    sagaStore = {
      sagas: {},
      businessKeys: {}
    };

    await saveSagaStore();
  }
}

async function publishOutcome(
  event: OutcomeEvent,
  causationId: string
): Promise<void> {
  if (!publisherChannel) {
    throw new Error("RabbitMQ publisher is unavailable");
  }

  const routingKey = `order.${event.state}`;

  publisherChannel.publish(
    OUTCOME_EXCHANGE,
    routingKey,
    Buffer.from(JSON.stringify(event)),
    {
      persistent: true,
      contentType: "application/json",
      messageId: randomUUID(),
      headers: {
        correlationId: event.correlationId,
        causationId
      }
    }
  );

  await publisherChannel.waitForConfirms();
}

async function publishInvalidMessage(
  message: any
): Promise<void> {
  if (!publisherChannel) {
    throw new Error("RabbitMQ publisher is unavailable");
  }

  publisherChannel.publish(
    INVALID_EXCHANGE,
    "",
    message.content,
    {
      persistent: true,
      contentType:
        message.properties.contentType ?? "application/octet-stream",
      messageId: randomUUID(),
      headers: {
        ...(message.properties.headers ?? {}),
        originalMessageId: message.properties.messageId ?? null
      }
    }
  );

  await publisherChannel.waitForConfirms();
}


async function publishDeadLetter(
  message: any,
  correlationId: string,
  causationId: string,
  lastError: string,
  attempts: number
): Promise<void> {
  if (!publisherChannel) {
    throw new Error("RabbitMQ publisher is unavailable");
  }

  publisherChannel.publish(
    DLQ_EXCHANGE,
    "",
    message.content,
    {
      persistent: true,
      contentType: message.properties.contentType ?? "application/json",
      messageId: randomUUID(),
      correlationId,
      headers: {
        ...(message.properties.headers ?? {}),
        correlationId,
        causationId,
        originQueue: SAGA_QUEUE,
        attempts,
        lastError,
        deadLetteredAt: now(),
        originalMessageId: message.properties.messageId ?? null
      }
    }
  );

  await publisherChannel.waitForConfirms();
}

function calculateAmount(order: CanonicalOrder): string {
  let totalCents = 0n;

  for (const item of order.items) {
    const [whole, fraction] = item.unitPrice.split(".");
    const unitCents =
      BigInt(whole) * 100n + BigInt(fraction);

    totalCents += unitCents * BigInt(item.quantity);
  }

  const wholeAmount = totalCents / 100n;
  const fractionAmount = String(totalCents % 100n)
    .padStart(2, "0");

  return `${wholeAmount}.${fractionAmount}`;
}

async function callProvider(
  url: string,
  body: Record<string, unknown>,
  correlationId: string,
  causationId: string,
  idempotencyKey?: string
): Promise<Record<string, unknown>> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Correlation-Id": correlationId,
    "X-Causation-Id": causationId
  };

  if (idempotencyKey) {
    headers["Idempotency-Key"] = idempotencyKey;
  }

  let response: Response;

  try {
    response = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
    });
  } catch (error) {
    if (
      error instanceof Error &&
      (
        error.name === "TimeoutError" ||
        error.name === "AbortError"
      )
    ) {
      throw new ProviderError("timeout");
    }

    throw new ProviderError("unreachable");
  }

  let responseBody: unknown;

  try {
    responseBody = await response.json();
  } catch {
    responseBody = {};
  }

  if (!response.ok) {
    const code =
      isObject(responseBody) &&
      typeof responseBody.code === "string"
        ? responseBody.code
        : `http_${response.status}`;

    throw new ProviderError(code, response.status);
  }

  return isObject(responseBody) ? responseBody : {};
}

async function callTracedProvider(
  operation: "payment" | "inventory" | "shipping" | "notification" |
    "inventory_release" | "payment_refund",
  url: string,
  body: Record<string, unknown>,
  correlationId: string,
  causationId: string,
  idempotencyKey?: string
): Promise<string> {
  const hop = beginTraceHop(publisherChannel, {
    correlationId,
    causationId,
    service: "orchestrator",
    operation
  });

  try {
    await callProvider(
      url,
      body,
      correlationId,
      hop.hopId,
      idempotencyKey
    );

    hop.finish("success");
    return hop.hopId;
  } catch (error) {
    const status =
      error instanceof ProviderError && error.message === "timeout"
        ? "timeout"
        : "failed";

    hop.finish(status, {
      message: error instanceof Error ? error.message : String(error)
    });

    if (typeof error === "object" && error !== null) {
      (error as unknown as Record<string, unknown>).traceHopId = hop.hopId;
    }

    throw error;
  }
}

async function recordStep(
  saga: SagaRecord,
  step: StepName,
  status: string
): Promise<void> {
  saga.steps.push({
    step,
    status,
    at: now()
  });

  saga.updatedAt = now();

  await saveSagaStore();
}

async function recordCompensation(
  saga: SagaRecord,
  step: CompensationName,
  status: string
): Promise<void> {
  saga.compensations.push({
    step,
    status,
    at: now()
  });

  saga.updatedAt = now();

  await saveSagaStore();
}

function createOutcome(
  order: CanonicalOrder,
  correlationId: string,
  businessKey: string,
  state: SagaState,
  extra: {
    code?: string;
    duplicateOf?: string;
  } = {}
): OutcomeEvent {
  return {
    correlationId,
    businessKey,
    orderId: order.orderId,
    source: order.source,
    orderType: order.orderType,
    recipient: order.customer.email,
    state,
    ...extra,
    occurredAt: now()
  };
}

async function publishAndStoreOutcome(
  saga: SagaRecord,
  event: OutcomeEvent,
  causationId: string
): Promise<void> {
  saga.state = event.state;
  saga.updatedAt = now();

  saga.outcome = {
    routingKey: `order.${event.state}`,
    envelope: event
  };

  await saveSagaStore();

  const outcomeHop = beginTraceHop(publisherChannel, {
    correlationId: event.correlationId,
    causationId,
    service: "orchestrator",
    operation: "saga.outcome",
    detail: { state: event.state }
  });

  try {
    await publishOutcome(event, outcomeHop.hopId);
    outcomeHop.finish("success");
  } catch (error) {
    outcomeHop.finish("failed", {
      message: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
}

async function compensate(
  saga: SagaRecord,
  order: CanonicalOrder,
  correlationId: string,
  businessKey: string,
  causationId: string
): Promise<{ succeeded: boolean; causationId: string }> {
  // Compensation is performed in reverse order.
  // Only completed payment and inventory steps need compensation.

  const completedSteps = saga.steps
    .filter((step) => step.status === "completed")
    .map((step) => step.step);
  const alreadyCompensated = new Set(
    saga.compensations
      .filter((item) => item.status === "completed")
      .map((item) => item.step)
  );

  const compensationSteps: CompensationName[] = [];

  if (completedSteps.includes("inventory") && !alreadyCompensated.has("inventory_release")) {
    compensationSteps.push("inventory_release");
  }

  if (completedSteps.includes("payment") && !alreadyCompensated.has("payment_refund")) {
    compensationSteps.push("payment_refund");
  }

  let previousHopId = causationId;

  for (const compensation of compensationSteps) {
    try {
      if (compensation === "inventory_release") {
        previousHopId = await callTracedProvider(
          "inventory_release",
          `${INVENTORY_URL}/inventory/release`,
          { orderId: order.orderId },
          correlationId,
          previousHopId,
          `release:${businessKey}`
        );
      } else {
        previousHopId = await callTracedProvider(
          "payment_refund",
          `${PAYMENT_URL}/payment/refund`,
          { orderId: order.orderId },
          correlationId,
          previousHopId,
          `refund:${businessKey}`
        );
      }

      await recordCompensation(
        saga,
        compensation,
        "completed"
      );
    } catch (error) {
      const code =
        error instanceof Error
          ? error.message
          : "unknown_error";

      const failedHopId =
        typeof error === "object" &&
        error !== null &&
        typeof (error as unknown as Record<string, unknown>).traceHopId === "string"
          ? String((error as unknown as Record<string, unknown>).traceHopId)
          : previousHopId;

      await recordCompensation(
        saga,
        compensation,
        "failed"
      );

      console.error(
        `Compensation failed for ${correlationId}:`,
        compensation,
        code
      );

      return { succeeded: false, causationId: failedHopId };
    }
  }

  return { succeeded: true, causationId: previousHopId };
}

async function runSaga(
  order: CanonicalOrder,
  correlationId: string,
  businessKey: string,
  consumeHopId: string,
  message: any,
  retryCount: number
): Promise<"ack" | "retry"> {
  const existingOwner = sagaStore.businessKeys[businessKey];

  if (existingOwner && existingOwner !== correlationId) {
    const duplicateSaga: SagaRecord = {
      businessKey,
      state: "duplicate",
      steps: [],
      compensations: [],
      owner: INSTANCE_ID,
      updatedAt: now()
    };

    sagaStore.sagas[correlationId] = duplicateSaga;
    await saveSagaStore();

    const duplicateEvent = createOutcome(
      order,
      correlationId,
      businessKey,
      "duplicate",
      { duplicateOf: existingOwner }
    );

    await publishAndStoreOutcome(duplicateSaga, duplicateEvent, consumeHopId);
    return "ack";
  }

  let saga = sagaStore.sagas[correlationId];
  let retryingFirstStep = false;

  if (saga) {
    // A parked saga is the exhausted first-step retry that an operator has
    // replayed from the DLQ. Resume it instead of republishing its old outcome.
    // Keep the persisted steps/compensations so completed work is not repeated.
    const isParkedFirstStepReplay =
      saga.state === "parked" &&
      saga.outcome?.envelope.state === "parked" &&
      saga.firstStepRetry === true &&
      !String(saga.owner).startsWith("temporal:");

    if (isParkedFirstStepReplay) {
      saga.outcome = undefined;
      saga.state = "processing";
      saga.owner = INSTANCE_ID;
      saga.lastCausationId = consumeHopId;
      saga.updatedAt = now();
      await saveSagaStore();

      const resumeHop = beginTraceHop(publisherChannel, {
        correlationId,
        causationId: consumeHopId,
        service: "orchestrator",
        operation: "saga.resume",
        detail: { reason: "parked_first_step_replay" }
      });
      resumeHop.finish("success");
    } else if (saga.outcome) {
      const replayOutcomeHop = beginTraceHop(publisherChannel, {
        correlationId,
        causationId: consumeHopId,
        service: "orchestrator",
        operation: "saga.outcome",
        detail: { replay: true }
      });

      try {
        await publishOutcome(
          { ...saga.outcome.envelope, occurredAt: now() },
          replayOutcomeHop.hopId
        );
        replayOutcomeHop.finish("success");
      } catch (error) {
        replayOutcomeHop.finish("failed");
        throw error;
      }
      return "ack";
    } else {
      // Only a first-step retry owned by this running process may resume.
      // A saga owned by an earlier process is deliberately not restarted.
      if (
        saga.state !== "processing" ||
        saga.owner !== INSTANCE_ID ||
        saga.firstStepRetry !== true
      ) {
        return "ack";
      }

      retryingFirstStep = true;
    }
  } else {
    saga = {
      businessKey,
      state: "processing",
      steps: [],
      compensations: [],
      owner: INSTANCE_ID,
      updatedAt: now(),
      order,
      lastCausationId: consumeHopId,
      firstStepRetry: false
    };

    sagaStore.sagas[correlationId] = saga;
    sagaStore.businessKeys[businessKey] = correlationId;
    await saveSagaStore();

    const processingEvent = createOutcome(
      order,
      correlationId,
      businessKey,
      "processing"
    );
    await publishOutcome(processingEvent, consumeHopId);
  }

  const firstStep: StepName = order.orderType === "b2b" ? "inventory" : "payment";
  let activeStep: StepName = firstStep;
  let lastHopId = consumeHopId;

  try {
    if (order.orderType !== "b2b" && !saga.steps.some((step) => step.step === "payment" && step.status === "completed")) {
      activeStep = "payment";
      lastHopId = await callTracedProvider(
        "payment",
        `${PAYMENT_URL}/payment/authorize`,
        { orderId: order.orderId, amount: calculateAmount(order) },
        correlationId,
        lastHopId,
        `authorize:${businessKey}`
      );
      saga.lastCausationId = lastHopId;
      await recordStep(saga, "payment", "completed");
      saga.firstStepRetry = false;
      await saveSagaStore();
    }

    if (!saga.steps.some((step) => step.step === "inventory" && step.status === "completed")) {
      activeStep = "inventory";
      lastHopId = await callTracedProvider(
        "inventory",
        `${INVENTORY_URL}/inventory/reserve`,
        { orderId: order.orderId, items: order.items },
        correlationId,
        lastHopId,
        `reserve:${businessKey}`
      );
      saga.lastCausationId = lastHopId;
      await recordStep(saga, "inventory", "completed");
      if (firstStep === "inventory") {
        saga.firstStepRetry = false;
        await saveSagaStore();
      }
    }

    if (!saga.steps.some((step) => step.step === "shipping" && step.status === "completed")) {
      activeStep = "shipping";
      lastHopId = await callTracedProvider(
        "shipping",
        `${SHIPPING_URL}/shipping/create`,
        { orderId: order.orderId },
        correlationId,
        lastHopId
      );
      saga.lastCausationId = lastHopId;
      await recordStep(saga, "shipping", "completed");
    }

    if (!saga.steps.some((step) => step.step === "notification" && step.status === "completed")) {
      activeStep = "notification";
      lastHopId = await callTracedProvider(
        "notification",
        `${NOTIFICATION_URL}/notification/send`,
        { orderId: order.orderId, recipient: order.customer.email },
        correlationId,
        lastHopId
      );
      saga.lastCausationId = lastHopId;
      await recordStep(saga, "notification", "completed");
    }

    const completedEvent = createOutcome(order, correlationId, businessKey, "completed");
    await publishAndStoreOutcome(saga, completedEvent, lastHopId);
    console.log(`Saga completed: ${correlationId} (${businessKey})`);
    return "ack";
  } catch (error) {
    const code = error instanceof Error ? error.message : "unknown_error";
    const failedHopId =
      typeof error === "object" && error !== null &&
      typeof (error as unknown as Record<string, unknown>).traceHopId === "string"
        ? String((error as unknown as Record<string, unknown>).traceHopId)
        : lastHopId;

    console.error(`Saga failed: ${correlationId}`, activeStep, code);

    const isFirstStep = activeStep === firstStep &&
      !saga.steps.some((step) => step.step === firstStep && step.status === "completed");
    const isPermanent4xx = error instanceof ProviderError &&
      error.status !== undefined && error.status >= 400 && error.status < 500;
    const isTemporary = error instanceof ProviderError &&
      (error.status === undefined || error.status >= 500);

    // First-step 4xx is permanent and must not be retried or compensated.
    if (isFirstStep && isPermanent4xx) {
      saga.lastCausationId = failedHopId;
    await recordStep(saga, activeStep, "failed");
      const failedEvent = createOutcome(order, correlationId, businessKey, "failed", { code });
      await publishAndStoreOutcome(saga, failedEvent, failedHopId);
      return "ack";
    }

    // Retry only temporary failures of the first provider step.
    if (isFirstStep && isTemporary) {
      const attempt = retryCount + 1;
      if (attempt < MAX_RETRIES) {
        saga.firstStepRetry = true;
        saga.state = "processing";
        saga.updatedAt = now();
        await saveSagaStore();

        const retryHop = beginTraceHop(publisherChannel, {
          correlationId,
          causationId: failedHopId,
          service: "orchestrator",
          operation: "saga.retry",
          attempt,
          messageId: message.properties.messageId ?? undefined,
          detail: { step: activeStep, attempt, maxRetries: MAX_RETRIES, error: code }
        });
        retryHop.finish("retried");
        return "retry";
      }

      const deadLetterHop = beginTraceHop(publisherChannel, {
        correlationId,
        causationId: failedHopId,
        service: "orchestrator",
        operation: "saga.dead_letter",
        attempt,
        messageId: message.properties.messageId ?? undefined,
        detail: { step: activeStep, error: code }
      });
      await publishDeadLetter(message, correlationId, deadLetterHop.hopId, code, attempt);
      deadLetterHop.finish("dead_lettered");

      const parkedEvent = createOutcome(order, correlationId, businessKey, "parked", { code });
      await publishAndStoreOutcome(saga, parkedEvent, deadLetterHop.hopId);
      return "ack";
    }

    // Any failure after the first step follows normal saga compensation.
    saga.lastCausationId = failedHopId;
    await recordStep(saga, activeStep, "failed");
    const compensationResult = await compensate(
      saga,
      order,
      correlationId,
      businessKey,
      failedHopId
    );

    const finalState: SagaState = compensationResult.succeeded
      ? "compensated"
      : "compensation_failed";
    const finalEvent = createOutcome(order, correlationId, businessKey, finalState, { code });
    await publishAndStoreOutcome(saga, finalEvent, compensationResult.causationId);
    return "ack";
  }
}


function getStuckSagaDetails() {
  return Object.entries(sagaStore.sagas)
    .filter(([, saga]) =>
      saga.state === "needs_attention" || saga.state === "compensation_failed"
    )
    .map(([correlationId, saga]) => ({
      correlationId,
      businessKey: saga.businessKey,
      state: saga.state,
      completedSteps: saga.steps
        .filter((step) => step.status === "completed")
        .map((step) => step.step),
      compensatedSteps: saga.compensations
        .filter((item) => item.status === "completed")
        .map((item) => item.step),
      since: String(saga.stuckSince ?? saga.updatedAt)
    }));
}

async function markPreviousProcessSagasAsStuck(): Promise<void> {
  for (const [correlationId, saga] of Object.entries(sagaStore.sagas)) {
    if (
      saga.state !== "processing" ||
      saga.owner === INSTANCE_ID ||
      String(saga.owner).startsWith("temporal:")
    ) {
      continue;
    }

    if (!saga.order) {
      console.error(
        `Cannot mark saga ${correlationId} as stuck: its persisted order is unavailable`
      );
      continue;
    }

    const causationId =
      typeof saga.lastCausationId === "string"
        ? saga.lastCausationId
        : randomUUID();

    const stuckHop = beginTraceHop(publisherChannel, {
      correlationId,
      causationId,
      service: "orchestrator",
      operation: "saga.stuck",
      detail: {
        previousOwner: saga.owner,
        currentOwner: INSTANCE_ID,
        stuckAfterMs: SAGA_STUCK_AFTER_MS
      }
    });

    try {
      saga.stuckSince = now();
      const event = createOutcome(
        saga.order,
        correlationId,
        saga.businessKey,
        "needs_attention"
      );
      await publishAndStoreOutcome(saga, event, stuckHop.hopId);
      stuckHop.finish("success");
      console.log(`Saga marked needs_attention: ${correlationId}`);
    } catch (error) {
      console.error(`Could not mark saga ${correlationId} as stuck:`, error);
    }
  }
}

async function compensateSagaAsOperator(
  correlationId: string
): Promise<{ status: number; body: Record<string, unknown> }> {
  const saga = sagaStore.sagas[correlationId];

  if (!saga) {
    return { status: 404, body: { code: "saga_not_found" } };
  }

  // Temporal workflows must be compensated through Temporal,
  // not through the hand-rolled saga operator endpoint.
  if (
    typeof saga.owner === "string" &&
    saga.owner.startsWith("temporal:")
  ) {
    return {
      status: 409,
      body: {
        code: "saga_not_stuck"
      }
    };
  }

  const operatorRecord = saga.operatorCompensation as
    | { status?: string; response?: Record<string, unknown> }
    | undefined;

  if (operatorRecord?.status === "running") {
    return {
      status: 409,
      body: { code: "compensation_in_progress" }
    };
  }

  if (saga.state === "compensated" && operatorRecord?.status === "completed") {
    return {
      status: 200,
      body: operatorRecord.response ?? {
        correlationId,
        state: "compensated"
      }
    };
  }

  if (saga.state !== "needs_attention" && saga.state !== "compensation_failed") {
    return {
      status: 409,
      body: { code: "saga_not_stuck" }
    };
  }

  if (!saga.order) {
    return {
      status: 409,
      body: { code: "saga_order_unavailable" }
    };
  }

  saga.operatorCompensation = {
    status: "running",
    startedAt: now(),
    requestedBy: "operator"
  };
  await saveSagaStore();

  const operatorHop = beginTraceHop(publisherChannel, {
    correlationId,
    causationId:
      typeof saga.lastCausationId === "string"
        ? saga.lastCausationId
        : randomUUID(),
    service: "orchestrator",
    operation: "saga.operator_compensate"
  });

  try {
    const result = await compensate(
      saga,
      saga.order,
      correlationId,
      saga.businessKey,
      operatorHop.hopId
    );

    const previousFailureCode =
      saga.outcome?.envelope.code ?? "compensation_failed";
    const finalState: SagaState = result.succeeded
      ? "compensated"
      : "compensation_failed";
    const code = result.succeeded
      ? (saga.state === "needs_attention"
          ? "operator_compensated"
          : previousFailureCode)
      : previousFailureCode;

    const event = createOutcome(
      saga.order,
      correlationId,
      saga.businessKey,
      finalState,
      { code }
    );

    operatorHop.finish(result.succeeded ? "success" : "failed", {
      state: finalState,
      code
    });

    await publishAndStoreOutcome(saga, event, result.causationId);

    const body = { correlationId, state: finalState };
    saga.operatorCompensation = {
      status: "completed",
      startedAt: (saga.operatorCompensation as any).startedAt,
      completedAt: now(),
      response: body
    };
    await saveSagaStore();

    return { status: 200, body };
  } catch (error) {
    operatorHop.finish("failed", {
      message: error instanceof Error ? error.message : String(error)
    });

    saga.operatorCompensation = {
      status: "failed",
      failedAt: now(),
      error: error instanceof Error ? error.message : String(error)
    };
    await saveSagaStore();
    throw error;
  }
}

async function runTemporalWorkflow(
  order: CanonicalOrder,
  correlationId: string,
  businessKey: string,
  consumeHopId: string
): Promise<"ack"> {
  const existingOwner = sagaStore.businessKeys[businessKey];

  // Preserve the existing business-key duplicate protection.
  if (existingOwner && existingOwner !== correlationId) {
    const duplicateSaga: SagaRecord = {
      businessKey,
      state: "duplicate",
      steps: [],
      compensations: [],
      owner: INSTANCE_ID,
      updatedAt: now()
    };

    sagaStore.sagas[correlationId] = duplicateSaga;
    await saveSagaStore();

    const duplicateEvent = createOutcome(
      order,
      correlationId,
      businessKey,
      "duplicate",
      { duplicateOf: existingOwner }
    );

    await publishAndStoreOutcome(duplicateSaga, duplicateEvent, consumeHopId);
    return "ack";
  }

  let saga = sagaStore.sagas[correlationId];

  if (saga?.outcome) {
    const replayOutcomeHop = beginTraceHop(publisherChannel, {
      correlationId,
      causationId: consumeHopId,
      service: "orchestrator",
      operation: "saga.outcome",
      detail: { replay: true, engine: "temporal" }
    });

    try {
      await publishOutcome(
        { ...saga.outcome.envelope, occurredAt: now() },
        replayOutcomeHop.hopId
      );
      replayOutcomeHop.finish("success");
    } catch (error) {
      replayOutcomeHop.finish("failed");
      throw error;
    }

    return "ack";
  }

  if (saga && !String(saga.owner).startsWith("temporal:")) {
    // Do not start a second engine for a saga already owned by the
    // hand-rolled orchestrator or another non-Temporal process.
    return "ack";
  }

  if (!saga) {
    saga = {
      businessKey,
      state: "processing",
      steps: [],
      compensations: [],
      owner: `temporal:${INSTANCE_ID}`,
      updatedAt: now(),
      order,
      lastCausationId: consumeHopId,
      firstStepRetry: false
    };

    sagaStore.sagas[correlationId] = saga;
    sagaStore.businessKeys[businessKey] = correlationId;
    await saveSagaStore();
  } else {
    saga.lastCausationId = consumeHopId;
    saga.updatedAt = now();
    await saveSagaStore();
  }

  const client = await getTemporalClient();
  const workflowStartHop = beginTraceHop(publisherChannel, {
    correlationId,
    causationId: consumeHopId,
    service: "orchestrator",
    operation: "workflow.start",
    detail: {
      workflowId: businessKey,
      workflowType: TEMPORAL_WORKFLOW_TYPE,
      taskQueue: TEMPORAL_TASK_QUEUE
    }
  });

  try {
    const handle = await client.workflow.start(TEMPORAL_WORKFLOW_TYPE, {
      workflowId: businessKey,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      taskQueue: TEMPORAL_TASK_QUEUE,
      args: [
        order,
        {
          correlationId,
          businessKey,
          causationId: consumeHopId
        }
      ]
    });

    workflowStartHop.finish("success");
    console.log(
      `Temporal workflow started: ${handle.workflowId} (${correlationId})`
    );
  } catch (error) {
    if (isWorkflowAlreadyStarted(error)) {
      // A RabbitMQ redelivery can happen after Temporal accepted the start
      // but before the original consumer acknowledged the message.
      workflowStartHop.finish("duplicate", {
        workflowId: businessKey,
        reason: "workflow_already_started"
      });
      console.log(
        `Temporal workflow already exists: ${businessKey} (${correlationId})`
      );
      return "ack";
    }

    workflowStartHop.finish("failed", {
      message: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }

  return "ack";
}

function isFinalTemporalOutcomeState(
  state: unknown
): state is Exclude<SagaState, "processing"> {
  return [
    "parked",
    "completed",
    "failed",
    "compensated",
    "compensation_failed",
    "needs_attention",
    "duplicate"
  ].includes(String(state));
}

/**
 * Persists final outcomes emitted by the Temporal worker.
 *
 * This consumer deliberately does not publish another outcome. The worker's
 * original event continues to the notifier/intake consumers; this queue only
 * synchronizes the orchestrator's durable saga store.
 */
async function processTemporalOutcomeMessage(message: any): Promise<void> {
  let parsed: unknown;

  try {
    parsed = JSON.parse(message.content.toString("utf8"));
  } catch {
    console.error("Ignoring malformed Temporal outcome message");
    consumerChannel.ack(message);
    return;
  }

  if (
    !isObject(parsed) ||
    typeof parsed.correlationId !== "string" ||
    typeof parsed.businessKey !== "string" ||
    typeof parsed.orderId !== "string" ||
    typeof parsed.occurredAt !== "string" ||
    typeof parsed.state !== "string"
  ) {
    console.error("Ignoring invalid Temporal outcome message");
    consumerChannel.ack(message);
    return;
  }

  // The worker emits a processing lifecycle event as well as a final event.
  if (parsed.state === "processing") {
    consumerChannel.ack(message);
    return;
  }

  if (!isFinalTemporalOutcomeState(parsed.state)) {
    console.error(
      `Ignoring unsupported Temporal outcome state "${parsed.state}"`
    );
    consumerChannel.ack(message);
    return;
  }

  const correlationId = parsed.correlationId;
  const businessKey = parsed.businessKey;
  const saga = sagaStore.sagas[correlationId];

  if (!saga || !String(saga.owner).startsWith("temporal:")) {
    // This queue is shared with the exchange, but only Temporal-owned sagas
    // are allowed to update this store.
    consumerChannel.ack(message);
    return;
  }

  if (saga.businessKey !== businessKey) {
    console.error(
      `Ignoring Temporal outcome with business-key mismatch for ${correlationId}`
    );
    consumerChannel.ack(message);
    return;
  }

  if (saga.outcome) {
    // RabbitMQ delivery is at-least-once. A previously persisted outcome is
    // authoritative; never replace it with a later duplicate/conflicting event.
    if (saga.outcome.envelope.state !== parsed.state) {
      console.error(
        `Ignoring conflicting Temporal outcome for ${correlationId}: ` +
        `${parsed.state} after ${saga.outcome.envelope.state}`
      );
    }
    consumerChannel.ack(message);
    return;
  }

  const event = parsed as OutcomeEvent;
  saga.state = event.state;
  saga.updatedAt = now();
  saga.outcome = {
    routingKey:
      typeof message.fields?.routingKey === "string"
        ? message.fields.routingKey
        : `order.${event.state}`,
    envelope: event
  };

  const headers = message.properties?.headers ?? {};
  if (typeof headers.causationId === "string") {
    saga.lastCausationId = headers.causationId;
  }

  // Persist before acknowledging. If persistence fails, the caller requeues
  // the message and this handler can safely retry it.
  await saveSagaStore();
  console.log(
    `Temporal saga outcome stored: ${correlationId} (${event.state})`
  );
  consumerChannel.ack(message);
}

async function processMessage(message: any): Promise<void> {
  let parsed: unknown;

  try {
    parsed = JSON.parse(message.content.toString("utf8"));
  } catch {
    await publishInvalidMessage(message);
    consumerChannel.ack(message);
    return;
  }

  if (!isValidOrder(parsed)) {
    await publishInvalidMessage(message);
    consumerChannel.ack(message);
    return;
  }

  const order = parsed as CanonicalOrder;
  const headers = message.properties.headers ?? {};
  const correlationId = typeof headers.correlationId === "string"
    ? headers.correlationId
    : message.properties.correlationId;
  const businessKey = typeof headers.businessKey === "string"
    ? headers.businessKey
    : `${order.source}:${order.orderId}`;
  const causationId = typeof headers.causationId === "string"
    ? headers.causationId
    : message.properties.messageId ?? randomUUID();

  if (
    typeof correlationId !== "string" ||
    !/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(correlationId)
  ) {
    await publishInvalidMessage(message);
    consumerChannel.ack(message);
    return;
  }

  const consumeHop = beginTraceHop(publisherChannel, {
    correlationId,
    causationId,
    service: "orchestrator",
    operation: "saga.consume",
    messageId: message.properties.messageId ?? undefined,
    attempt: getRetryCount(message) + 1
  });

  try {
    const result = SAGA_ENGINE === "temporal"
      ? await runTemporalWorkflow(
          order,
          correlationId,
          businessKey,
          consumeHop.hopId
        )
      : await runSaga(
          order,
          correlationId,
          businessKey,
          consumeHop.hopId,
          message,
          getRetryCount(message)
        );

    if (result === "retry") {
      consumeHop.finish("retried", { step: order.orderType === "b2b" ? "inventory" : "payment" });
      consumerChannel.nack(message, false, false);
      return;
    }

    consumeHop.finish("success");
    consumerChannel.ack(message);
  } catch (error) {
    consumeHop.finish("failed", {
      message: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function maintainRabbitConnection(): Promise<void> {
  while (true) {
    try {
      console.log("Connecting to RabbitMQ...");
      const rabbit = await connectWithRetry(RABBITMQ_URL);

      connection = rabbit.connection;
      consumerChannel = rabbit.channel;
      publisherChannel = await connection.createConfirmChannel();

      await publisherChannel.assertExchange(
        OUTCOME_EXCHANGE,
        "topic",
        { durable: true }
      );

      await consumerChannel.prefetch(
        Math.max(1, Number(process.env.SAGA_CONCURRENCY ?? 5))
      );

      await consumerChannel.consume(
        SAGA_QUEUE,
        async (message: any) => {
          if (!message) return;

          try {
            await processMessage(message);
          } catch (error) {
            console.error("Could not process saga message:", error);
            try {
              if (consumerChannel) {
                consumerChannel.nack(message, false, true);
              }
            } catch (nackError) {
              console.error("Could not requeue saga message:", nackError);
            }
          }
        }
      );

      if (SAGA_ENGINE === "temporal") {
        await consumerChannel.assertQueue(TEMPORAL_OUTCOME_QUEUE, {
          durable: true
        });
        await consumerChannel.bindQueue(
          TEMPORAL_OUTCOME_QUEUE,
          OUTCOME_EXCHANGE,
          "order.*"
        );

        await consumerChannel.consume(
          TEMPORAL_OUTCOME_QUEUE,
          async (message: any) => {
            if (!message) return;

            try {
              await processTemporalOutcomeMessage(message);
            } catch (error) {
              console.error(
                "Could not persist Temporal outcome:",
                error
              );
              try {
                if (consumerChannel) {
                  consumerChannel.nack(message, false, true);
                }
              } catch (nackError) {
                console.error(
                  "Could not requeue Temporal outcome:",
                  nackError
                );
              }
            }
          }
        );

        temporalOutcomeConsumerReady = true;
        console.log(
          `Connected and consuming ${TEMPORAL_OUTCOME_QUEUE}`
        );
      } else {
        temporalOutcomeConsumerReady = false;
      }

      brokerConnected = true;
      consumerReady = true;
      console.log(`Connected and consuming ${SAGA_QUEUE}`);

      await new Promise<void>((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) return;
          settled = true;
          resolve();
        };

        connection.once("close", finish);
        consumerChannel.once("close", finish);
        publisherChannel.once("close", finish);
        connection.on("error", (error: Error) => {
          console.error("RabbitMQ connection error:", error);
        });
      });
    } catch (error) {
      console.error("RabbitMQ connection/consumer setup failed:", error);
    }

    brokerConnected = false;
    consumerReady = false;
    temporalOutcomeConsumerReady = false;
    connection = null;
    consumerChannel = null;
    publisherChannel = null;

    console.error("RabbitMQ disconnected; reconnecting in 2 seconds...");
    await sleep(2000);
  }
}

async function startServer(): Promise<void> {
  try {
    await loadSagaStore();

    if (SAGA_ENGINE !== "temporal" && SAGA_ENGINE !== "handrolled") {
      throw new Error(
        `Unsupported SAGA_ENGINE "${SAGA_ENGINE}". Use "handrolled" or "temporal".`
      );
    }

    console.log(
      `Saga engine: ${SAGA_ENGINE}` +
      (SAGA_ENGINE === "temporal"
        ? ` (Temporal ${TEMPORAL_ADDRESS}, task queue ${TEMPORAL_TASK_QUEUE})`
        : "")
    );

    app.get("/health", (_req, res) => {
      const healthy = brokerConnected && consumerReady &&
        (SAGA_ENGINE !== "temporal" || temporalOutcomeConsumerReady);

      return res.status(healthy ? 200 : 503).json({
        status: healthy ? "ok" : "error",
        checks: {
          broker: brokerConnected ? "up" : "down",
          consumers: {
            [SAGA_QUEUE]: consumerReady ? 1 : 0,
            ...(SAGA_ENGINE === "temporal"
              ? {
                  [TEMPORAL_OUTCOME_QUEUE]:
                    temporalOutcomeConsumerReady ? 1 : 0
                }
              : {})
          }
        }
      });
    });

    app.get("/sagas/stuck", (_req, res) => {
      return res.status(200).json({
        sagas: getStuckSagaDetails()
      });
    });

    app.post("/sagas/:correlationId/compensate", async (req, res) => {
      try {
        const result = await compensateSagaAsOperator(req.params.correlationId);
        return res.status(result.status).json(result.body);
      } catch (error) {
        console.error(
          `Operator compensation failed for ${req.params.correlationId}:`,
          error
        );
        return res.status(503).json({
          code: "operator_compensation_failed"
        });
      }
    });

    // Wait after startup before flagging work owned by the previous process.
    setTimeout(() => {
      void markPreviousProcessSagasAsStuck();
    }, SAGA_STUCK_AFTER_MS);

    app.listen(PORT, "0.0.0.0", () => {
      console.log(`Orchestrator listening on port ${PORT}`);
    });

    // Keep the HTTP server alive while RabbitMQ is unavailable.
    void maintainRabbitConnection();
  } catch (error) {
    console.error("Could not start orchestrator:", error);
    process.exit(1);
  }
}

void startServer();
