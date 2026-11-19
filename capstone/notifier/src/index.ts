
import express from "express";
import { randomUUID } from "node:crypto";
import {
  appendFile,
  mkdir,
  readFile,
  writeFile,
  rename
} from "node:fs/promises";
import path from "node:path";
import {
  connectWithRetry,
  getRetryCount
} from "../../shared/rabbit.js";
import { beginTraceHop } from "../../shared/trace.js";

const app = express();

const PORT = Number(process.env.PORT ?? 8803);
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://eai:eai-capstone@localhost:5680";

const NOTIFICATION_URL =
  process.env.NOTIFICATION_URL ?? "http://notification:4004";

const REQUEST_TIMEOUT_MS = Number(
  process.env.REQUEST_TIMEOUT_MS ?? 2500
);

const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 3);
const DATA_DIR = process.env.DATA_DIR ?? "./data";

const PROCESSED_IDS_FILE = path.join(DATA_DIR, "processed-ids.json");
const NOTIFICATION_LOG_FILE = path.join(DATA_DIR, "notification.log");

const NOTIFICATIONS_QUEUE = "notifications.queue";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const DLQ_EXCHANGE = "orders.dlq.exchange";

interface NotificationEvent {
  correlationId: string;
  businessKey?: string;
  orderId: string;
  source?: string;
  orderType?: string;
  recipient: string;
  state: string;
  code?: string;
}

interface DeadLetterMetadata {
  originQueue: string;
  attempts: number;
  lastError: string;
  deadLetteredAt: string;
}

let brokerConnected = false;
let consumerReady = false;
let activeConnection: any = null;
let activeChannel: any = null;
let activeConfirmChannel: any = null;
let reconnecting = false;
let reconnectDelayMs = 1000;

let processedIds: Record<string, string> = {};

function isObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

function parseNotificationEvent(
  content: Buffer
): NotificationEvent | null {
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
    parsed.correlationId.length === 0 ||
    typeof parsed.orderId !== "string" ||
    parsed.orderId.length === 0 ||
    typeof parsed.recipient !== "string" ||
    parsed.recipient.length === 0 ||
    typeof parsed.state !== "string" ||
    !["failed", "compensated"].includes(parsed.state)
  ) {
    return null;
  }

  if (
    parsed.code !== undefined &&
    typeof parsed.code !== "string"
  ) {
    return null;
  }

  return {
    correlationId: parsed.correlationId,
    orderId: parsed.orderId,
    recipient: parsed.recipient,
    state: parsed.state,
    ...(typeof parsed.businessKey === "string"
      ? { businessKey: parsed.businessKey }
      : {}),
    ...(typeof parsed.source === "string"
      ? { source: parsed.source }
      : {}),
    ...(typeof parsed.orderType === "string"
      ? { orderType: parsed.orderType }
      : {}),
    ...(typeof parsed.code === "string"
      ? { code: parsed.code }
      : {})
  };
}

async function loadProcessedIds(): Promise<void> {
  await mkdir(DATA_DIR, { recursive: true });

  try {
    const content = await readFile(PROCESSED_IDS_FILE, "utf8");
    const parsed: unknown = JSON.parse(content);

    processedIds = isObject(parsed)
      ? (parsed as Record<string, string>)
      : {};
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      processedIds = {};
      await saveProcessedIds();
      return;
    }

    throw error;
  }
}

async function saveProcessedIds(): Promise<void> {
  await mkdir(DATA_DIR, { recursive: true });

  const temporaryFile = `${PROCESSED_IDS_FILE}.${randomUUID()}.tmp`;

  await writeFile(
    temporaryFile,
    JSON.stringify(processedIds, null, 2),
    "utf8"
  );

  await rename(temporaryFile, PROCESSED_IDS_FILE);
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

async function sendNotification(
  event: NotificationEvent,
  causationId: string
): Promise<void> {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    REQUEST_TIMEOUT_MS
  );

  try {
    const response = await fetch(
      `${NOTIFICATION_URL}/notification/send`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Correlation-Id": event.correlationId,
          "X-Causation-Id": causationId
        },
        body: JSON.stringify({
          orderId: event.orderId,
          recipient: event.recipient,
          kind: "order_not_completed",
          reason: event.code ?? "unknown"
        }),
        signal: controller.signal
      }
    );

    if (!response.ok) {
      let responseText = "";

      try {
        responseText = await response.text();
      } catch {
        // The HTTP status is enough to identify the failure.
      }

      throw new Error(
        `Notification provider returned HTTP ${response.status}` +
          (responseText ? `: ${responseText}` : "")
      );
    }
  } finally {
    clearTimeout(timeout);
  }
}

async function appendNotificationLog(
  event: NotificationEvent
): Promise<void> {
  await mkdir(DATA_DIR, { recursive: true });

  const entry = {
    correlationId: event.correlationId,
    orderId: event.orderId,
    recipient: event.recipient,
    kind: "order_not_completed",
    reason: event.code ?? "unknown",
    sentAt: new Date().toISOString()
  };

  await appendFile(
    NOTIFICATION_LOG_FILE,
    `${JSON.stringify(entry)}\n`,
    "utf8"
  );
}

function getMessageHeaders(
  headers: Record<string, unknown> | undefined
): Record<string, unknown> {
  return { ...(headers ?? {}) };
}

function getCorrelationId(message: any): string {
  const headers = message.properties.headers ?? {};

  const value =
    message.properties.correlationId ??
    headers.correlationId;

  return typeof value === "string" && value.length > 0
    ? value
    : randomUUID();
}

function getOriginalCausationId(message: any): string | null {
  const headers = message.properties.headers ?? {};

  return typeof headers.causationId === "string"
    ? headers.causationId
    : null;
}

async function publishInvalidMessage(
  confirmChannel: any,
  message: any
): Promise<void> {
  confirmChannel.publish(
    INVALID_EXCHANGE,
    "",
    message.content,
    {
      persistent: true,
      contentType: message.properties.contentType,
      messageId: message.properties.messageId ?? randomUUID(),
      correlationId: message.properties.correlationId,
      headers: getMessageHeaders(message.properties.headers)
    }
  );

  await confirmChannel.waitForConfirms();
}

async function publishDeadLetter(
  confirmChannel: any,
  message: any,
  retryCount: number,
  lastError: string,
  causationId: string
): Promise<void> {
  const headers = getMessageHeaders(message.properties.headers);

  const metadata: DeadLetterMetadata = {
    originQueue: NOTIFICATIONS_QUEUE,
    attempts: retryCount + 1,
    lastError,
    deadLetteredAt: new Date().toISOString()
  };

  confirmChannel.publish(
    DLQ_EXCHANGE,
    "",
    message.content,
    {
      persistent: true,
      contentType: message.properties.contentType ?? "application/json",
      messageId: randomUUID(),
      correlationId:
        message.properties.correlationId ??
        (typeof headers.correlationId === "string"
          ? headers.correlationId
          : undefined),
      headers: {
        ...headers,
        ...metadata,
        causationId
      }
    }
  );

  await confirmChannel.waitForConfirms();
}

async function handleNotification(
  channel: any,
  confirmChannel: any,
  message: any
): Promise<void> {
  const correlationId = getCorrelationId(message);
  const originalCausationId = getOriginalCausationId(message);
  const retryCount = getRetryCount(message);
  const attempt = retryCount + 1;

  const consumeHop = beginTraceHop(confirmChannel, {
    correlationId,
    causationId: originalCausationId,
    service: "notifier",
    operation: "notify.consume",
    attempt,
    messageId: message.properties.messageId ?? undefined
  });

  const event = parseNotificationEvent(message.content);

  if (!event) {
    console.error("Invalid notification event; sending to invalid exchange");

    const quarantineHop = beginTraceHop(confirmChannel, {
      correlationId,
      causationId: consumeHop.hopId,
      service: "notifier",
      operation: "invalid.quarantine",
      attempt
    });

    try {
      await publishInvalidMessage(confirmChannel, message);

      quarantineHop.finish("quarantined");
      consumeHop.finish("success");

      channel.ack(message);
    } catch (error) {
      quarantineHop.finish("failed", {
        message: getErrorMessage(error)
      });
      consumeHop.finish("failed");
      throw error;
    }

    return;
  }

  if (processedIds[event.correlationId]) {
    console.log(
      `Notification already processed for ${event.correlationId}; acknowledging duplicate`
    );

    const duplicateHop = beginTraceHop(confirmChannel, {
      correlationId: event.correlationId,
      causationId: consumeHop.hopId,
      service: "notifier",
      operation: "notify.duplicate",
      attempt
    });

    duplicateHop.finish("duplicate");
    consumeHop.finish("success");

    channel.ack(message);
    return;
  }

  const sendHop = beginTraceHop(confirmChannel, {
    correlationId: event.correlationId,
    causationId: consumeHop.hopId,
    service: "notifier",
    operation: "notify.send",
    attempt
  });

  try {
    await sendNotification(event, sendHop.hopId);

    await appendNotificationLog(event);

    processedIds[event.correlationId] = new Date().toISOString();
    await saveProcessedIds();

    sendHop.finish("success");
    consumeHop.finish("success");

    console.log(
      `Notification sent for order ${event.orderId} (${event.correlationId})`
    );

    channel.ack(message);
  } catch (error) {
    const lastError = getErrorMessage(error);

    sendHop.finish("failed", {
      message: lastError
    });

    console.error(
      `Notification failed for ${event.correlationId}. Attempt ${attempt}/${MAX_RETRIES}: ${lastError}`
    );

    if (attempt < MAX_RETRIES) {
      const retryHop = beginTraceHop(confirmChannel, {
        correlationId: event.correlationId,
        causationId: consumeHop.hopId,
        service: "notifier",
        operation: "notify.retry",
        attempt,
        detail: { lastError }
      });

      retryHop.finish("retried");
      consumeHop.finish("failed", {
        message: lastError,
        retryScheduled: true
      });

      // RabbitMQ routes this rejected delivery through the configured
      // retry exchange and retry queue.
      channel.nack(message, false, false);
      return;
    }

    // Publish to the DLQ and wait for broker confirmation before
    // acknowledging the original message.
    const deadLetterHop = beginTraceHop(confirmChannel, {
      correlationId: event.correlationId,
      causationId: consumeHop.hopId,
      service: "notifier",
      operation: "notify.dead_letter",
      attempt,
      detail: { lastError }
    });

    try {
      await publishDeadLetter(
        confirmChannel,
        message,
        retryCount,
        lastError,
        deadLetterHop.hopId
      );

      deadLetterHop.finish("dead_lettered");
      consumeHop.finish("dead_lettered");

      channel.ack(message);
    } catch (publishError) {
      deadLetterHop.finish("failed", {
        message: getErrorMessage(publishError)
      });
      consumeHop.finish("failed");

      throw publishError;
    }
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function markDisconnected(reason: string): void {
  if (!brokerConnected && !consumerReady) {
    return;
  }

  console.warn(`Notifier RabbitMQ connection unavailable: ${reason}`);
  brokerConnected = false;
  consumerReady = false;
  activeConnection = null;
  activeChannel = null;
  activeConfirmChannel = null;
  void reconnectLoop();
}

async function connectAndConsume(): Promise<void> {
  const { connection, channel } = await connectWithRetry(RABBITMQ_URL);
  let confirmChannel: any;

  try {
    confirmChannel = await connection.createConfirmChannel();
    await channel.prefetch(1);

    await channel.consume(
      NOTIFICATIONS_QUEUE,
      async (message) => {
        if (!message) {
          return;
        }

        try {
          await handleNotification(channel, confirmChannel, message);
        } catch (error) {
          console.error(
            "Notification message processing failed:",
            error
          );

          // If publishing failed, leave the message available for redelivery.
          try {
            channel.nack(message, false, true);
          } catch (nackError) {
            console.error("Could not requeue message:", nackError);
          }
        }
      }
    );

    activeConnection = connection;
    activeChannel = channel;
    activeConfirmChannel = confirmChannel;
    brokerConnected = true;
    consumerReady = true;

    connection.on("error", (error: Error) => {
      console.error("RabbitMQ connection error:", error);
    });
    connection.on("close", () => {
      if (activeConnection === connection) {
        markDisconnected("connection closed");
      }
    });
    channel.on("error", (error: Error) => {
      console.error("RabbitMQ consumer channel error:", error);
    });
    channel.on("close", () => {
      if (activeConnection === connection) {
        markDisconnected("consumer channel closed");
      }
    });
    confirmChannel.on("error", (error: Error) => {
      console.error("RabbitMQ publisher channel error:", error);
    });
    confirmChannel.on("close", () => {
      if (activeConnection === connection) {
        markDisconnected("publisher channel closed");
      }
    });

    console.log(`Notifier consuming ${NOTIFICATIONS_QUEUE}`);
  } catch (error) {
    try {
      await connection.close();
    } catch {
      // The connection may already be closed.
    }
    throw error;
  }
}

async function reconnectLoop(): Promise<void> {
  if (reconnecting) {
    return;
  }

  reconnecting = true;

  try {
    while (!brokerConnected) {
      try {
        console.log("Notifier connecting to RabbitMQ...");
        await connectAndConsume();
        reconnectDelayMs = 1000;
        console.log("Notifier RabbitMQ consumer is ready");
      } catch (error) {
        brokerConnected = false;
        consumerReady = false;
        console.error("Notifier RabbitMQ connection failed:", error);
        console.log(`Notifier will retry in ${reconnectDelayMs}ms`);
        await sleep(reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
      }
    }
  } finally {
    reconnecting = false;
  }
}

async function startServer(): Promise<void> {
  try {
    await loadProcessedIds();

    app.get("/health", (_req, res) => {
      const healthy = brokerConnected && consumerReady;

      return res.status(healthy ? 200 : 503).json({
        status: healthy ? "ok" : "error",
        checks: {
          broker: brokerConnected ? "up" : "down",
          consumers: {
            [NOTIFICATIONS_QUEUE]: consumerReady ? 1 : 0
          }
        }
      });
    });

    app.listen(PORT, "0.0.0.0", () => {
      console.log(`Notifier listening on port ${PORT}`);
    });

    // Keep the HTTP health endpoint alive while RabbitMQ is unavailable.
    // The consumer is recreated after every successful reconnection.
    void reconnectLoop();
  } catch (error) {
    console.error("Could not start notifier:", error);
    process.exit(1);
  }
}

void startServer();
