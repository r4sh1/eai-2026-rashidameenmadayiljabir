/**
 * The checkout workflow: the PA6 saga on Temporal.
 *
 * No I/O here. Every call to the outside world is an activity, called
 * through `act`. Only types are imported from ../activities.
 */
import { ActivityFailure, ApplicationFailure, TimeoutFailure, proxyActivities } from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalItem, CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

/** unitPrice x quantity summed in whole cents, as a two-digit decimal string. */
export function sumAmount(items: CanonicalItem[]): string {
  let cents = 0;
  for (const item of items) {
    const [whole = "0", fraction = "00"] = item.unitPrice.split(".");
    const unitCents = Number(whole) * 100 + Number(fraction.padEnd(2, "0").slice(0, 2));
    cents += unitCents * item.quantity;
  }
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

type Outcome = { ok: true } | { ok: false; code: string };

/** Why an activity failed for good: "timeout", or the code the activity threw. */
function codeOf(err: unknown): string {
  if (err instanceof ActivityFailure) {
    if (err.cause instanceof TimeoutFailure) return "timeout";
    if (err.cause instanceof ApplicationFailure && err.cause.type) return err.cause.type;
  }
  return "unknown_error";
}

/** Runs one step, records it in the trace (one entry per step, not per attempt). */
async function runStep(trace: TraceItem[], step: string, call: () => Promise<void>): Promise<Outcome> {
  const startedAt = new Date();
  try {
    await call();
    trace.push(entry(step, "success", startedAt));
    return { ok: true };
  } catch (err) {
    const code = codeOf(err);
    trace.push(entry(step, code === "timeout" ? "timeout" : "failed", startedAt));
    return { ok: false, code };
  }
}

function entry(step: string, status: string, startedAt: Date): TraceItem {
  const finishedAt = new Date();
  return {
    step,
    status,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
  };
}

/**
 * Undoes what completed, newest first. Tries every undo even if one fails,
 * so a broken release does not leave the customer's money taken.
 * Returns true only if every undo worked.
 */
async function compensate(trace: TraceItem[], orderId: string, inventoryReserved: boolean): Promise<boolean> {
  let allOk = true;
  if (inventoryReserved) {
    const released = await runStep(trace, "inventory_release", () => act.releaseInventory(orderId));
    if (!released.ok) allOk = false;
  }
  const refunded = await runStep(trace, "payment_refund", () => act.refundPayment(orderId));
  if (!refunded.ok) allOk = false;
  return allOk;
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];

  // 1) payment. Nothing to undo if it fails.
  const payment = await runStep(trace, "payment", () => act.authorizePayment(orderId, sumAmount(order.items)));
  if (!payment.ok) return { orderId, status: "failed", code: payment.code, trace };

  // 2) inventory. Undo: refund.
  const inventory = await runStep(trace, "inventory", () => act.reserveInventory(orderId, order.items));
  if (!inventory.ok) {
    const undone = await compensate(trace, orderId, false);
    return undone
      ? { orderId, status: "compensated", code: inventory.code, trace }
      : { orderId, status: "failed", code: "compensation_failed", trace };
  }

  // 3) shipping. Undo: release, then refund. Shipping has no undo itself.
  const shipping = await runStep(trace, "shipping", () => act.createShipment(orderId));
  if (!shipping.ok) {
    const undone = await compensate(trace, orderId, true);
    return undone
      ? { orderId, status: "compensated", code: shipping.code, trace }
      : { orderId, status: "failed", code: "compensation_failed", trace };
  }

  // 4) notification. Same undo as shipping; neither shipping nor the notification can be undone.
  const notification = await runStep(trace, "notification", () => act.sendNotification(orderId, order.customer.email));
  if (!notification.ok) {
    const undone = await compensate(trace, orderId, true);
    return undone
      ? { orderId, status: "compensated", code: notification.code, trace }
      : { orderId, status: "failed", code: "compensation_failed", trace };
  }

  return { orderId, status: "completed", trace };
}
