
import express from "express";
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import * as AjvModule from "ajv";
import * as AjvFormatsModule from "ajv-formats";

import {
  connectWithRetry,
  withoutRetryHistory
} from "../../shared/rabbit.js";

import { beginTraceHop } from "../../shared/trace.js";

import {
  translateWeb,
  translateMobile,
  translateB2B,
  type CanonicalOrder,
  type TranslateOptions
} from "./transform.js";

import {
  loadIdempotencyStore,
  saveIdempotencyStore,
  loadOrders,
  saveOrders,
  type IdempotencyRecord,
  type OrderRecord,
  type OrderState
} from "./store.js";

const AjvConstructor: any =
  (AjvModule as any).default ?? (AjvModule as any).Ajv;

const addFormats: any =
  (AjvFormatsModule as any).default ?? AjvFormatsModule;

const app = express();

const PORT = Number(process.env.PORT ?? 8800);
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://eai:eai-capstone@localhost:5680";

const PRICING_URL =
  process.env.PRICING_URL ?? "http://localhost:4110";

const PRICING_API_KEY =
  process.env.PRICING_API_KEY ?? "pa4-pricing-key-2026";

const REQUEST_TIMEOUT_MS = Number(
  process.env.REQUEST_TIMEOUT_MS ?? 2500
);

const ORDER_ITEMS_MAX = Number(
  process.env.ORDER_ITEMS_MAX ?? 50
);

const DATA_DIR = process.env.DATA_DIR ?? "./data";

const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

const OUTCOME_EXCHANGE = "orders.outcome";
const OUTCOME_QUEUE = "intake.outcome.queue";

const DLQ_QUEUE = "orders.dlq";
const RETURN_EXCHANGE = "orders.return.exchange";

type StoredResponse = {
  status: number;
  body: unknown;
};

interface OutcomeEvent {
  correlationId: string;
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  state: OrderState;
  code?: string;
  duplicateOf?: string;
  occurredAt?: string;
}

interface DlqEntry {
  messageId: string;
  correlationId: string;
  originQueue: string;
  attempts: number;
  lastError: string;
  deadLetteredAt: string;
}

const VALID_ORDER_STATES = new Set<OrderState>([
  "accepted",
  "processing",
  "parked",
  "completed",
  "failed",
  "compensated",
  "compensation_failed",
  "needs_attention",
  "duplicate"
]);

const TERMINAL_STATES = new Set<OrderState>([
  "completed",
  "failed",
  "compensated",
  "duplicate"
]);

let brokerConnected = false;
let outcomeConsumerReady = false;

let confirmChannel: any = null;
let outcomeChannel: any = null;
let dlqChannel: any = null;
let activeConnection: any = null;
let reconnectPromise: Promise<void> | null = null;

let idempotencyStore: Record<string, IdempotencyRecord> = {};
let orders: Record<string, OrderRecord> = {};

const ajv = new AjvConstructor({
  allErrors: true,
  strict: false
});

addFormats(ajv);

let validateCanonicalOrder:
  | ReturnType<typeof ajv.compile>
  | null = null;

function sendError(
  res: express.Response,
  status: number,
  code: string,
  message?: string,
  extra: Record<string, unknown> = {}
) {
  return res.status(status).json({
    code,
    ...(message ? { message } : {}),
    ...extra
  });
}

function retryAfterSeconds(): number {
  return Math.max(1, Math.ceil(REQUEST_TIMEOUT_MS / 1000));
}

function getContentType(req: express.Request): {
  mediaType: string;
  charset?: string;
} {
  const header = req.headers["content-type"] ?? "";
  const parts = header.split(";").map((part) => part.trim());

  const mediaType = (parts[0] ?? "").toLowerCase();

  const charsetPart = parts.find((part) =>
    part.toLowerCase().startsWith("charset=")
  );

  const charset = charsetPart
    ? charsetPart.slice("charset=".length).replace(/^["']|["']$/g, "")
    : undefined;

  return { mediaType, charset };
}

function getRawBody(req: express.Request): Buffer {
  if (!Buffer.isBuffer(req.body)) {
    return Buffer.alloc(0);
  }

  return req.body;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

function isOrderState(value: unknown): value is OrderState {
  return (
    typeof value === "string" &&
    VALID_ORDER_STATES.has(value as OrderState)
  );
}

function canTransition(
  current: OrderState,
  next: OrderState
): boolean {
  if (current === next) {
    return true;
  }

  if (TERMINAL_STATES.has(current)) {
    return false;
  }

  if (current === "accepted") {
    return next !== "accepted";
  }

  if (current === "processing") {
    return [
      "parked",
      "completed",
      "failed",
      "compensated",
      "compensation_failed",
      "needs_attention"
    ].includes(next);
  }

  if (current === "parked") {
    return [
      "processing",
      "completed",
      "failed",
      "compensated",
      "compensation_failed",
      "needs_attention"
    ].includes(next);
  }

  if (current === "compensation_failed") {
    return next === "compensated";
  }

  if (current === "needs_attention") {
    return (
      next === "compensated" ||
      next === "compensation_failed"
    );
  }

  return false;
}

async function translateOrder(
  rawBody: Buffer,
  mediaType: string,
  charset?: string
): Promise<{
  order: CanonicalOrder | null;
  warnings: Array<{
    code: string;
    productId: string;
    message: string;
  }>;
}> {
  const temporaryDirectory = await mkdtemp(
    path.join(tmpdir(), "cap-intake-")
  );

  try {
    const options: TranslateOptions = {
      pricingBaseUrl: PRICING_URL,
      apiKey: PRICING_API_KEY
    };

    if (
      mediaType === "application/xml" ||
      mediaType === "text/xml"
    ) {
      const filePath = path.join(temporaryDirectory, "order.xml");

      await writeFile(filePath, rawBody);

      return await translateB2B(filePath, options);
    }

    let parsed: unknown;

    try {
      parsed = JSON.parse(rawBody.toString("utf8"));
    } catch {
      throw new Error("The JSON request body is invalid");
    }

    if (!isObject(parsed)) {
      throw new Error("The JSON body must be an object");
    }

    const filePath = path.join(temporaryDirectory, "order.json");

    await writeFile(filePath, JSON.stringify(parsed), "utf8");

    if (
      typeof parsed.orderId === "string" &&
      isObject(parsed.customer)
    ) {
      return await translateWeb(filePath, options);
    }

    if (typeof parsed.oid === "string") {
      return await translateMobile(filePath, options);
    }

    throw new Error(
      "JSON body does not match the web or mobile order format"
    );
  } finally {
    await rm(temporaryDirectory, {
      recursive: true,
      force: true
    });
  }
}

async function saveStores(): Promise<void> {
  await Promise.all([
    saveIdempotencyStore(DATA_DIR, idempotencyStore),
    saveOrders(DATA_DIR, orders)
  ]);
}

async function publishAcceptedOrder(
  order: CanonicalOrder,
  correlationId: string,
  businessKey: string,
  causationId: string,
  messageId: string
): Promise<void> {
  if (!brokerConnected || !confirmChannel) {
    throw new Error("RabbitMQ is not connected");
  }

  confirmChannel.publish(
    "orders.exchange",
    "",
    Buffer.from(JSON.stringify(order)),
    {
      persistent: true,
      contentType: "application/json",
      messageId,
      headers: {
        correlationId,
        causationId,
        businessKey
      }
    }
  );

  await confirmChannel.waitForConfirms();
}

function parseOutcomeEvent(content: Buffer): OutcomeEvent | null {
  let parsed: unknown;

  try {
    parsed = JSON.parse(content.toString("utf8"));
  } catch {
    return null;
  }

  if (!isObject(parsed)) {
    return null;
  }

  if (
    typeof parsed.correlationId !== "string" ||
    typeof parsed.businessKey !== "string" ||
    typeof parsed.orderId !== "string" ||
    typeof parsed.source !== "string" ||
    typeof parsed.orderType !== "string" ||
    !isOrderState(parsed.state) ||
    parsed.state === "accepted"
  ) {
    return null;
  }

  if (
    parsed.code !== undefined &&
    typeof parsed.code !== "string"
  ) {
    return null;
  }

  if (
    parsed.duplicateOf !== undefined &&
    typeof parsed.duplicateOf !== "string"
  ) {
    return null;
  }

  return {
    correlationId: parsed.correlationId,
    businessKey: parsed.businessKey,
    orderId: parsed.orderId,
    source: parsed.source,
    orderType: parsed.orderType,
    state: parsed.state,
    ...(typeof parsed.code === "string"
      ? { code: parsed.code }
      : {}),
    ...(typeof parsed.duplicateOf === "string"
      ? { duplicateOf: parsed.duplicateOf }
      : {}),
    ...(typeof parsed.occurredAt === "string"
      ? { occurredAt: parsed.occurredAt }
      : {})
  };
}

async function handleOutcomeEvent(content: Buffer): Promise<void> {
  const event = parseOutcomeEvent(content);

  if (!event) {
    throw new Error("Invalid order outcome event");
  }

  const existing = orders[event.correlationId];

  if (!existing) {
    console.warn(
      `Ignoring outcome for unknown correlationId ${event.correlationId}`
    );
    return;
  }

  if (existing.businessKey !== event.businessKey) {
    throw new Error(
      `Business key mismatch for ${event.correlationId}`
    );
  }

  if (!canTransition(existing.state as OrderState, event.state)) {
    console.log(
      `Ignoring invalid lifecycle transition: ${existing.state} -> ${event.state} (${event.correlationId})`
    );
    return;
  }

  const updatedRecord: OrderRecord = {
    ...existing,
    state: event.state,
    updatedAt: new Date().toISOString()
  };

  if (event.code !== undefined) {
    updatedRecord.code = event.code;
  } else {
    delete updatedRecord.code;
  }

  if (event.duplicateOf !== undefined) {
    updatedRecord.duplicateOf = event.duplicateOf;
  } else if (event.state !== "duplicate") {
    delete updatedRecord.duplicateOf;
  }

  orders[event.correlationId] = updatedRecord;

  await saveOrders(DATA_DIR, orders);

  console.log(
    `Order ${event.correlationId} updated to ${event.state}`
  );
}

async function startOutcomeConsumer(
  connection: any
): Promise<void> {
  outcomeChannel = await connection.createChannel();

  await outcomeChannel.assertQueue(OUTCOME_QUEUE, {
    durable: true
  });

  await outcomeChannel.bindQueue(
    OUTCOME_QUEUE,
    OUTCOME_EXCHANGE,
    "#"
  );

  await outcomeChannel.prefetch(1);

  await outcomeChannel.consume(
    OUTCOME_QUEUE,
    async (message: any) => {
      if (!message) {
        return;
      }

      try {
        await handleOutcomeEvent(message.content);
        outcomeChannel.ack(message);
      } catch (error) {
        console.error("Could not process outcome event:", error);
        outcomeChannel.nack(message, false, false);
      }
    }
  );

  outcomeConsumerReady = true;

  console.log(
    `Consuming order outcome events from ${OUTCOME_QUEUE}`
  );
}

/**
 * Reads the metadata attached to a dead-lettered message.
 * The original message body is never changed.
 */
function getDlqEntry(message: any): DlqEntry {
  const headers = message.properties.headers ?? {};

  const correlationId =
    message.properties.correlationId ??
    headers.correlationId ??
    "";

  return {
    messageId:
      message.properties.messageId ??
      "",
    correlationId: String(correlationId),
    originQueue:
      typeof headers.originQueue === "string"
        ? headers.originQueue
        : "",
    attempts:
      typeof headers.attempts === "number"
        ? headers.attempts
        : 0,
    lastError:
      typeof headers.lastError === "string"
        ? headers.lastError
        : "",
    deadLetteredAt:
      typeof headers.deadLetteredAt === "string"
        ? headers.deadLetteredAt
        : ""
  };
}

/**
 * Inspects a snapshot of the DLQ without permanently removing messages.
 * Each message is returned to the queue after inspection.
 */
async function inspectDlq(): Promise<DlqEntry[]> {
  if (!dlqChannel) {
    throw new Error("DLQ channel is not connected");
  }

  const queueInfo = await dlqChannel.checkQueue(DLQ_QUEUE);
  const initialCount = queueInfo.messageCount;
  const messages: any[] = [];

  try {
    // Keep inspected messages unacknowledged until the snapshot is complete.
    // This prevents RabbitMQ from immediately returning the same requeued
    // message on the next basic.get call.
    for (let index = 0; index < initialCount; index += 1) {
      const message = await dlqChannel.get(DLQ_QUEUE, {
        noAck: false
      });

      if (!message) {
        break;
      }

      messages.push(message);
    }

    return messages.map((message) => getDlqEntry(message));
  } finally {
    // Inspection must not consume DLQ messages. Requeue them only after
    // all messages in this snapshot have been collected.
    for (const message of messages) {
      dlqChannel.nack(message, false, true);
    }
  }
}

/**
 * Publishes a DLQ message back to its original queue through
 * orders.return.exchange. The original is acknowledged only
 * after RabbitMQ confirms the new publication.
 */
async function replayDlqMessage(message: any): Promise<void> {
  if (!confirmChannel || !dlqChannel) {
    throw new Error("RabbitMQ replay channels are unavailable");
  }

  const entry = getDlqEntry(message);

  if (
    !entry.correlationId ||
    !entry.originQueue ||
    entry.originQueue === "orders.invalid" ||
    !["notifications.queue", "saga.queue"].includes(entry.originQueue)
  ) {
    throw new Error("This DLQ message cannot be replayed");
  }

  const oldHeaders = message.properties.headers ?? {};

  const headers = withoutRetryHistory(oldHeaders);

  // Remove DLQ-only metadata before the message re-enters processing.
  delete headers.originQueue;
  delete headers.attempts;
  delete headers.lastError;
  delete headers.deadLetteredAt;

  const previousCausationId =
    typeof oldHeaders.causationId === "string"
      ? oldHeaders.causationId
      : null;

  const replayMessageId = randomUUID();
  const replayHop = beginTraceHop(confirmChannel, {
    correlationId: entry.correlationId,
    causationId: previousCausationId,
    service: "intake",
    operation: "dlq.replay",
    messageId: replayMessageId,
    detail: {
      originQueue: entry.originQueue,
      originalMessageId: entry.messageId
    }
  });

  headers.causationId = replayHop.hopId;
  headers.correlationId = entry.correlationId;

  try {
    confirmChannel.publish(
      RETURN_EXCHANGE,
      entry.originQueue,
      message.content,
      {
        persistent: true,
        contentType:
          message.properties.contentType ?? "application/json",
        messageId: replayMessageId,
        correlationId: entry.correlationId,
        headers
      }
    );

    await confirmChannel.waitForConfirms();

    replayHop.finish("success");

    // Only remove the original after confirmed publication.
    dlqChannel.ack(message);
  } catch (error) {
    replayHop.finish("failed", {
      message: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
}

app.use(
  express.raw({
    type: () => true,
    limit: "5mb"
  })
);

app.get("/health", (_req, res) => {
  const healthy = brokerConnected && outcomeConsumerReady;

  return res.status(healthy ? 200 : 503).json({
    status: healthy ? "ok" : "error",
    checks: {
      broker: brokerConnected ? "up" : "down",
      consumers: {
        [OUTCOME_QUEUE]: outcomeConsumerReady ? 1 : 0
      }
    }
  });
});

app.post("/orders", async (req, res) => {
  const idempotencyKey = req.header("Idempotency-Key");

  if (
    !idempotencyKey ||
    !IDEMPOTENCY_KEY_PATTERN.test(idempotencyKey)
  ) {
    return sendError(
      res,
      400,
      "validation_error",
      "A valid Idempotency-Key header is required"
    );
  }

  const { mediaType, charset } = getContentType(req);

  const isXml =
    mediaType === "application/xml" ||
    mediaType === "text/xml";

  const isJson = mediaType === "application/json";

  if (!isXml && !isJson) {
    return sendError(
      res,
      415,
      "unsupported_media_type",
      "Use application/json, application/xml or text/xml"
    );
  }

  const rawBody = getRawBody(req);

  if (rawBody.length === 0) {
    return sendError(
      res,
      400,
      "validation_error",
      "The request body is empty"
    );
  }

  const bodySha256 = createHash("sha256")
    .update(rawBody)
    .digest("hex");

  const existing = idempotencyStore[idempotencyKey];

  if (existing) {
    if (existing.bodySha256 !== bodySha256) {
      return sendError(
        res,
        409,
        "idempotency_payload_mismatch"
      );
    }

    if (existing.state === "in_flight") {
      return sendError(
        res,
        409,
        "idempotency_conflict"
      );
    }

    if (existing.response) {
      return res
        .status(existing.response.status)
        .json(existing.response.body);
    }
  }

  const correlationId = randomUUID();

  // Tracing is best-effort: business processing continues even if trace
  // publication is unavailable. The receive hop is the root of this flow.
  const receiveHop = confirmChannel
    ? beginTraceHop(confirmChannel, {
        correlationId,
        causationId: null,
        service: "intake",
        operation: "intake.receive"
      })
    : null;

  const translateHop = confirmChannel
    ? beginTraceHop(confirmChannel, {
        correlationId,
        causationId: receiveHop?.hopId ?? null,
        service: "intake",
        operation: "intake.translate"
      })
    : null;

  receiveHop?.finish("success");

  let translated;

  try {
    translated = await translateOrder(
      rawBody,
      mediaType,
      charset
    );
    translateHop?.finish("success");
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "The order could not be translated";

    translateHop?.finish("failed", { message });

    return sendError(
      res,
      400,
      "validation_error",
      message
    );
  }

  const unknownProducts = translated.warnings
    .filter((warning) => warning.code === "UNKNOWN_PRODUCT")
    .map((warning) => warning.productId);

  if (unknownProducts.length > 0) {
    const uniqueProductIds = [...new Set(unknownProducts)];

    const responseBody = {
      code: "unknown_product",
      productIds: uniqueProductIds
    };

    idempotencyStore[idempotencyKey] = {
      bodySha256,
      state: "done",
      response: {
        status: 422,
        body: responseBody
      },
      createdAt: new Date().toISOString()
    };

    await saveIdempotencyStore(DATA_DIR, idempotencyStore);

    return res.status(422).json(responseBody);
  }

  const pricingFailed = translated.warnings.some(
    (warning) => warning.code === "PRICING_API_ERROR"
  );

  if (pricingFailed) {
    res.setHeader("Retry-After", String(retryAfterSeconds()));

    return sendError(
      res,
      503,
      "pricing_unavailable"
    );
  }

  const order = translated.order;

  if (!order) {
    return sendError(
      res,
      400,
      "validation_error",
      "The order contains no valid items"
    );
  }

  if (order.items.length > ORDER_ITEMS_MAX) {
    return sendError(
      res,
      400,
      "validation_error",
      `The order exceeds the maximum of ${ORDER_ITEMS_MAX} items`
    );
  }

  if (!validateCanonicalOrder) {
    return sendError(
      res,
      503,
      "validation_error",
      "Canonical schema validator is not ready"
    );
  }

  if (!validateCanonicalOrder(order)) {
    return sendError(
      res,
      400,
      "validation_error",
      "The translated order does not match the canonical schema"
    );
  }

  const businessKey = `${order.source}:${order.orderId}`;
  const now = new Date().toISOString();

  idempotencyStore[idempotencyKey] = {
    bodySha256,
    state: "in_flight",
    correlationId,
    createdAt: now
  };

  orders[correlationId] = {
    correlationId,
    businessKey,
    orderId: order.orderId,
    source: order.source,
    orderType: order.orderType,
    state: "accepted",
    updatedAt: now
  };

  // Persist the accepted order before publishing it.
  await saveStores();

  const publishMessageId = randomUUID();
  const publishHop = confirmChannel
    ? beginTraceHop(confirmChannel, {
        correlationId,
        causationId: translateHop?.hopId ?? receiveHop?.hopId ?? null,
        service: "intake",
        operation: "intake.publish",
        messageId: publishMessageId,
        detail: { businessKey }
      })
    : null;

  try {
    if (!publishHop) {
      throw new Error("RabbitMQ tracing channel is unavailable");
    }

    await publishAcceptedOrder(
      order,
      correlationId,
      businessKey,
      publishHop.hopId,
      publishMessageId
    );

    publishHop.finish("success");
  } catch (error) {
    publishHop?.finish("failed", {
      message: error instanceof Error ? error.message : String(error)
    });

    console.error("Order publish failed:", error);

    delete idempotencyStore[idempotencyKey];
    delete orders[correlationId];

    await saveStores();

    res.setHeader("Retry-After", String(retryAfterSeconds()));

    return sendError(
      res,
      503,
      "broker_unavailable"
    );
  }

  const responseBody = {
    correlationId,
    status: "accepted"
  };

  idempotencyStore[idempotencyKey] = {
    bodySha256,
    state: "done",
    response: {
      status: 202,
      body: responseBody
    },
    correlationId,
    createdAt: new Date().toISOString()
  };

  // Do not overwrite the order record here because the saga
  // may already have published an outcome.
  await saveIdempotencyStore(DATA_DIR, idempotencyStore);

  return res.status(202).json(responseBody);
});

app.get("/orders/:correlationId", (req, res) => {
  const record = orders[req.params.correlationId];

  if (!record) {
    return sendError(res, 404, "not_found");
  }

  return res.status(200).json(record);
});

/**
 * GET /dlq
 *
 * Returns:
 * {
 *   count: number,
 *   entries: [...]
 * }
 *
 * Does not permanently remove any messages.
 */
app.get("/dlq", async (_req, res) => {
  try {
    const entries = await inspectDlq();

    return res.status(200).json({
      count: entries.length,
      entries
    });
  } catch (error) {
    console.error("Could not inspect DLQ:", error);

    return sendError(
      res,
      503,
      "dlq_unavailable",
      "Could not inspect the dead-letter queue"
    );
  }
});

/**
 * POST /dlq/replay
 *
 * {} replays all supported DLQ messages.
 *
 * { "correlationId": "..." } replays only matching messages.
 */
app.post("/dlq/replay", async (req, res) => {
  if (!brokerConnected || !dlqChannel || !confirmChannel) {
    return sendError(
      res,
      503,
      "broker_unavailable"
    );
  }

  let requestBody: Record<string, unknown> = {};

  const rawBody = getRawBody(req);

  if (rawBody.length > 0) {
    try {
      const parsed: unknown = JSON.parse(rawBody.toString("utf8"));

      if (!isObject(parsed)) {
        return sendError(
          res,
          400,
          "validation_error",
          "Replay body must be a JSON object"
        );
      }

      requestBody = parsed;
    } catch {
      return sendError(
        res,
        400,
        "validation_error",
        "Replay body contains invalid JSON"
      );
    }
  }

  const requestedCorrelationId = requestBody.correlationId;

  if (
    requestedCorrelationId !== undefined &&
    (
      typeof requestedCorrelationId !== "string" ||
      requestedCorrelationId.length === 0
    )
  ) {
    return sendError(
      res,
      400,
      "validation_error",
      "correlationId must be a non-empty string"
    );
  }

  try {
    const queueInfo = await dlqChannel.checkQueue(DLQ_QUEUE);
    const initialCount = queueInfo.messageCount;

    let replayed = 0;
    const messages: any[] = [];

    // Collect the initial DLQ snapshot before requeueing anything.
    // This prevents an unmatched message from being fetched repeatedly.
    for (let index = 0; index < initialCount; index += 1) {
      const message = await dlqChannel.get(DLQ_QUEUE, {
        noAck: false
      });

      if (!message) {
        break;
      }

      messages.push(message);
    }

    // Process the collected snapshot.
    for (const message of messages) {
      const entry = getDlqEntry(message);

      const matches =
        requestedCorrelationId === undefined ||
        entry.correlationId === requestedCorrelationId;

      if (!matches) {
        dlqChannel.nack(message, false, true);
        continue;
      }

      try {
        await replayDlqMessage(message);
        replayed += 1;
      } catch (error) {
        // Keep this message in the DLQ if replay cannot be confirmed.
        try {
          dlqChannel.nack(message, false, true);
        } catch {
          // The channel may have closed.
        }

        console.error(
          `Could not replay DLQ message ${entry.messageId}:`,
          error
        );
      }
    }

    const remainingInfo = await dlqChannel.checkQueue(DLQ_QUEUE);

    return res.status(200).json({
      replayed,
      remaining: remainingInfo.messageCount
    });
  } catch (error) {
    console.error("DLQ replay failed:", error);

    return sendError(
      res,
      503,
      "dlq_replay_failed",
      "Could not replay dead-letter messages"
    );
  }
});

async function ensureBrokerConnection(): Promise<void> {
  if (reconnectPromise) {
    return reconnectPromise;
  }

  reconnectPromise = (async () => {
    let retryDelay = 1000;

    while (!brokerConnected) {
      let connection: any = null;

      try {
        console.log("Connecting to RabbitMQ...");
        const connected = await connectWithRetry(RABBITMQ_URL);
        connection = connected.connection;

        const newConfirmChannel = await connection.createConfirmChannel();
        const newDlqChannel = await connection.createChannel();
        await newDlqChannel.checkQueue(DLQ_QUEUE);

        activeConnection = connection;
        confirmChannel = newConfirmChannel;
        dlqChannel = newDlqChannel;
        outcomeChannel = null;
        outcomeConsumerReady = false;

        connection.on("close", () => {
          if (activeConnection !== connection) {
            return;
          }

          console.warn("RabbitMQ connection closed; reconnecting...");
          brokerConnected = false;
          outcomeConsumerReady = false;
          activeConnection = null;
          confirmChannel = null;
          outcomeChannel = null;
          dlqChannel = null;
          void ensureBrokerConnection();
        });

        connection.on("error", (error: Error) => {
          console.error("RabbitMQ connection error:", error);
          if (activeConnection === connection) {
            brokerConnected = false;
            outcomeConsumerReady = false;
          }
        });

        await startOutcomeConsumer(connection);
        brokerConnected = true;
        retryDelay = 1000;
        console.log("RabbitMQ connected and intake consumer is ready");
      } catch (error) {
        console.error("Could not connect intake to RabbitMQ:", error);
        brokerConnected = false;
        outcomeConsumerReady = false;
        activeConnection = null;
        confirmChannel = null;
        outcomeChannel = null;
        dlqChannel = null;

        if (connection) {
          try {
            await connection.close();
          } catch {
            // The connection may already be closed.
          }
        }

        console.warn(`Retrying RabbitMQ connection in ${retryDelay}ms`);
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
        retryDelay = Math.min(retryDelay * 2, 30000);
      }
    }
  })().finally(() => {
    reconnectPromise = null;
  });

  return reconnectPromise;
}

async function startServer(): Promise<void> {
  try {
    idempotencyStore = await loadIdempotencyStore(DATA_DIR);
    orders = await loadOrders(DATA_DIR);

    const schemaPath = path.join(
      "/canonical",
      "order.schema.json"
    );

    let schemaText: string;

    try {
      schemaText = await readFile(schemaPath, "utf8");
    } catch {
      schemaText = await readFile(
        path.join(
          process.cwd(),
          "intake",
          "src",
          "order.schema.json"
        ),
        "utf8"
      );
    }

    const schema = JSON.parse(schemaText);
    validateCanonicalOrder = ajv.compile(schema);

    app.listen(PORT, "0.0.0.0", () => {
      console.log(`Intake service listening on port ${PORT}`);
      console.log("DLQ inspection and replay endpoints are ready");
    });

    // Keep the HTTP server alive while RabbitMQ is unavailable. The health
    // endpoint reports 503 until the connection and outcome consumer recover.
    void ensureBrokerConnection();
  } catch (error) {
    console.error("Could not start intake service:", error);
    process.exit(1);
  }
}

void startServer();
