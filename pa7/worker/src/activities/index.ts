/**
 * The six activities. Each one makes one HTTP call to one mock service and
 * decides whether a failure may be retried (see README.md section 4).
 *
 *   2xx            -> return
 *   4xx            -> the service said no, it will say no again -> non-retryable
 *   5xx / no reply -> temporary -> retryable
 *   too slow       -> not handled here: startToCloseTimeout fails the attempt
 */
import axios from "axios";
import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

// docker-compose.yml sets these. Never hardcode a downstream URL.
const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

/**
 * One POST. No axios timeout on purpose: the 2500 ms budget belongs to the
 * activity (startToCloseTimeout), so a slow call surfaces as a TimeoutFailure
 * in the workflow and not as an ECONNABORTED we would have to translate.
 */
async function post(url: string, body: unknown, fallbackCode: string): Promise<void> {
  try {
    await axios.post(url, body);
  } catch (err) {
    if (axios.isAxiosError(err) && err.response) {
      const status = err.response.status;
      const data = err.response.data as { code?: unknown; message?: unknown } | undefined;
      const code = typeof data?.code === "string" ? data.code : fallbackCode;
      const message = `${url} answered ${status} ${code}`;
      if (status >= 400 && status < 500) {
        throw ApplicationFailure.nonRetryable(message, code);
      }
      throw ApplicationFailure.retryable(message, code);
    }
    // No answer at all (connection refused, reset, DNS): temporary.
    throw ApplicationFailure.retryable(`${url} did not answer: ${(err as Error).message}`, "unavailable");
  }
}

/** POST {PAYMENT_URL}/payment/authorize with { orderId, amount }. */
export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, { orderId, amount }, "payment_failed");
}

/** POST {INVENTORY_URL}/inventory/reserve with { orderId, items }. */
export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, { orderId, items }, "inventory_failed");
}

/** POST {SHIPPING_URL}/shipping/create with { orderId }. */
export async function createShipment(orderId: string): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, { orderId }, "shipping_failed");
}

/** POST {NOTIFICATION_URL}/notification/send with { orderId, recipient }. */
export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, { orderId, recipient }, "notification_failed");
}

/** Compensates authorizePayment: POST {PAYMENT_URL}/payment/refund with { orderId }. */
export async function refundPayment(orderId: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, { orderId }, "payment_refund_failed");
}

/** Compensates reserveInventory: POST {INVENTORY_URL}/inventory/release with { orderId }. */
export async function releaseInventory(orderId: string): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, { orderId }, "inventory_release_failed");
}
