import axios from "axios";
import { ApplicationFailure, Context } from "@temporalio/activity";
import type { Channel, ChannelModel } from "amqplib";
import { connectWithRetry } from "../../../shared/rabbit.js";
import { beginTraceHop } from "../../../shared/trace.js";
import type { CanonicalItem, CanonicalOrder } from "../types";

export interface ActivityMetadata {
  correlationId: string;
  businessKey: string;
  causationId: string | null;
}

export type OutcomeState =
  | "processing"
  | "completed"
  | "failed"
  | "compensated"
  | "compensation_failed";

interface RabbitResources {
  connection: ChannelModel;
  channel: Channel;
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");
const RABBITMQ_URL = requiredEnv("RABBITMQ_URL");

let rabbitPromise: Promise<RabbitResources> | undefined;

async function getRabbit(): Promise<RabbitResources> {
  if (!rabbitPromise) {
    rabbitPromise = connectWithRetry(RABBITMQ_URL).then(
      ({ connection, channel }) => ({ connection, channel }),
    );
  }
  return rabbitPromise!;
}

function idempotencyKey(
  operation: string,
  metadata: ActivityMetadata,
): string {
  return `${operation}:${metadata.businessKey}`;
}

async function post(
  url: string,
  body: unknown,
  fallbackCode: string,
  operation: string,
  metadata: ActivityMetadata,
  providerKey?: string,
): Promise<string> {
  const { channel } = await getRabbit();
  const attempt = Context.current().info.attempt;

  const hop = beginTraceHop(channel, {
    correlationId: metadata.correlationId,
    causationId: metadata.causationId,
    service: "worker",
    operation,
    attempt,
  });

  try {
    await axios.post(url, body, {
      headers: {
        "Content-Type": "application/json",
        "X-Correlation-Id": metadata.correlationId,
        "X-Causation-Id": metadata.causationId ?? hop.hopId,
        ...(providerKey ? { "Idempotency-Key": providerKey } : {}),
      },
    });

    hop.finish("success");
    return hop.hopId;
  } catch (err) {
    if (axios.isAxiosError(err) && err.response) {
      const status = err.response.status;
      const data = err.response.data as
        | { code?: unknown }
        | undefined;
      const code =
        typeof data?.code === "string" ? data.code : fallbackCode;
      const message = `${url} answered ${status} ${code}`;

      hop.finish("failed", { httpStatus: status, code });

      if (status >= 400 && status < 500) {
        throw ApplicationFailure.nonRetryable(message, code);
      }

      throw ApplicationFailure.retryable(message, code);
    }

    const message =
      err instanceof Error ? err.message : String(err);

    hop.finish("failed", { code: "unavailable", message });

    throw ApplicationFailure.retryable(
      `${url} did not answer: ${message}`,
      "unavailable",
    );
  }
}

/** POST {PAYMENT_URL}/payment/authorize. */
export async function authorizePayment(
  orderId: string,
  amount: string,
  metadata: ActivityMetadata,
): Promise<string> {
  return post(
    `${PAYMENT_URL}/payment/authorize`,
    { orderId, amount },
    "payment_failed",
    "payment",
    metadata,
    idempotencyKey("authorize", metadata),
  );
}

/** POST {INVENTORY_URL}/inventory/reserve. */
export async function reserveInventory(
  orderId: string,
  items: CanonicalItem[],
  metadata: ActivityMetadata,
): Promise<string> {
  return post(
    `${INVENTORY_URL}/inventory/reserve`,
    { orderId, items },
    "inventory_failed",
    "inventory",
    metadata,
    idempotencyKey("reserve", metadata),
  );
}

/** POST {SHIPPING_URL}/shipping/create. */
export async function createShipment(
  orderId: string,
  metadata: ActivityMetadata,
): Promise<string> {
  return post(
    `${SHIPPING_URL}/shipping/create`,
    { orderId },
    "shipping_failed",
    "shipping",
    metadata,
    idempotencyKey("create-shipment", metadata),
  );
}

/** POST {NOTIFICATION_URL}/notification/send. */
export async function sendNotification(
  orderId: string,
  recipient: string,
  metadata: ActivityMetadata,
): Promise<string> {
  return post(
    `${NOTIFICATION_URL}/notification/send`,
    { orderId, recipient },
    "notification_failed",
    "notification",
    metadata,
  );
}

/** Compensates authorizePayment. */
export async function refundPayment(
  orderId: string,
  metadata: ActivityMetadata,
): Promise<string> {
  return post(
    `${PAYMENT_URL}/payment/refund`,
    { orderId },
    "payment_refund_failed",
    "payment_refund",
    metadata,
    idempotencyKey("refund", metadata),
  );
}

/** Compensates reserveInventory. */
export async function releaseInventory(
  orderId: string,
  metadata: ActivityMetadata,
): Promise<string> {
  return post(
    `${INVENTORY_URL}/inventory/release`,
    { orderId },
    "inventory_release_failed",
    "inventory_release",
    metadata,
    idempotencyKey("release", metadata),
  );
}

/**
 * Publishes lifecycle and final outcome events from an activity,
 * never directly from workflow code.
 */
export async function publishOutcome(
  order: CanonicalOrder,
  metadata: ActivityMetadata,
  state: OutcomeState,
  code?: string,
): Promise<string> {
  const { channel } = await getRabbit();

  const hop = beginTraceHop(channel, {
    correlationId: metadata.correlationId,
    causationId: metadata.causationId,
    service: "worker",
    operation: "saga.outcome",
    attempt: Context.current().info.attempt,
    detail: { state, ...(code ? { code } : {}) },
  });

  const event = {
    correlationId: metadata.correlationId,
    businessKey: metadata.businessKey,
    orderId: order.orderId,
    source: order.source,
    orderType: order.orderType,
    recipient: order.customer.email,
    state,
    ...(code ? { code } : {}),
    occurredAt: new Date().toISOString(),
  };

  try {
    channel.publish(
      "orders.outcome",
      `order.${state}`,
      Buffer.from(JSON.stringify(event)),
      {
        persistent: true,
        contentType: "application/json",
        messageId: hop.hopId,
        headers: {
          correlationId: metadata.correlationId,
          causationId: metadata.causationId ?? hop.hopId,
        },
      },
    );

    hop.finish("success");
    return hop.hopId;
  } catch (err) {
    const message =
      err instanceof Error ? err.message : String(err);

    hop.finish("failed", { message });

    throw ApplicationFailure.retryable(
      `Could not publish ${state} outcome: ${message}`,
      "outcome_publish_failed",
    );
  }
}
