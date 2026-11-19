
import {
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
  proxyActivities,
} from "@temporalio/workflow";

import type * as activities from "../activities/index.js";
import type {
  ActivityMetadata,
  OutcomeState,
} from "../activities/index.js";
import type {
  CanonicalItem,
  CanonicalOrder,
  CheckoutResult,
  TraceItem,
} from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

interface CheckoutMetadata {
  correlationId: string;
  businessKey: string;
  causationId: string | null;
}

type Outcome =
  | { ok: true }
  | { ok: false; code: string };

function sumAmount(items: CanonicalItem[]): string {
  let cents = 0;

  for (const item of items) {
    const [whole = "0", fraction = "00"] =
      item.unitPrice.split(".");

    const unitCents =
      Number(whole) * 100 +
      Number(fraction.padEnd(2, "0").slice(0, 2));

    cents += unitCents * item.quantity;
  }

  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

function codeOf(err: unknown): string {
  if (err instanceof ActivityFailure) {
    if (err.cause instanceof TimeoutFailure) {
      return "timeout";
    }

    if (
      err.cause instanceof ApplicationFailure &&
      err.cause.type
    ) {
      return err.cause.type;
    }
  }

  return "unknown_error";
}

function entry(
  step: string,
  status: string,
  startedAt: Date,
): TraceItem {
  const finishedAt = new Date();

  return {
    step,
    status,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs:
      finishedAt.getTime() - startedAt.getTime(),
  };
}

async function runStep(
  trace: TraceItem[],
  step: string,
  call: () => Promise<string>,
  metadata: CheckoutMetadata,
): Promise<Outcome> {
  const startedAt = new Date();

  try {
    const hopId = await call();
    metadata.causationId = hopId;

    trace.push(entry(step, "success", startedAt));

    return { ok: true };
  } catch (err) {
    const code = codeOf(err);

    trace.push(
      entry(
        step,
        code === "timeout" ? "timeout" : "failed",
        startedAt,
      ),
    );

    return { ok: false, code };
  }
}

async function compensate(
  trace: TraceItem[],
  orderId: string,
  inventoryReserved: boolean,
  paymentAuthorized: boolean,
  metadata: CheckoutMetadata,
): Promise<boolean> {
  let allOk = true;

  // Undo inventory before refunding payment.
  if (inventoryReserved) {
    const released = await runStep(
      trace,
      "inventory_release",
      () => act.releaseInventory(orderId, metadata),
      metadata,
    );

    if (!released.ok) {
      allOk = false;
    }
  }

  // B2B orders do not authorize payment.
  if (paymentAuthorized) {
    const refunded = await runStep(
      trace,
      "payment_refund",
      () => act.refundPayment(orderId, metadata),
      metadata,
    );

    if (!refunded.ok) {
      allOk = false;
    }
  }

  return allOk;
}

async function finish(
  order: CanonicalOrder,
  metadata: CheckoutMetadata,
  state: OutcomeState,
  code?: string,
): Promise<void> {
  await act.publishOutcome(order, metadata, state, code);
}

export async function checkout(
  order: CanonicalOrder,
  metadata: CheckoutMetadata,
): Promise<CheckoutResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];

  const activityMetadata: ActivityMetadata = {
    correlationId: metadata.correlationId,
    businessKey: metadata.businessKey,
    causationId: metadata.causationId,
  };

  // Publish processing lifecycle event.
  const processingHop = await act.publishOutcome(
    order,
    activityMetadata,
    "processing",
  );

  activityMetadata.causationId = processingHop;

  // B2B orders skip payment.
  const isB2B = order.orderType === "b2b";
  let paymentAuthorized = false;

  if (!isB2B) {
    const payment = await runStep(
      trace,
      "payment",
      () =>
        act.authorizePayment(
          orderId,
          sumAmount(order.items),
          activityMetadata,
        ),
      activityMetadata,
    );

    if (!payment.ok) {
      await finish(
        order,
        activityMetadata,
        "failed",
        payment.code,
      );

      return {
        orderId,
        status: "failed",
        code: payment.code,
        trace,
      };
    }

    paymentAuthorized = true;
  }

  // Reserve inventory.
  const inventory = await runStep(
    trace,
    "inventory",
    () =>
      act.reserveInventory(
        orderId,
        order.items,
        activityMetadata,
      ),
    activityMetadata,
  );

  if (!inventory.ok) {
    const undone = await compensate(
      trace,
      orderId,
      false,
      paymentAuthorized,
      activityMetadata,
    );

    const state: OutcomeState = undone
      ? "compensated"
      : "compensation_failed";

    const code = undone
      ? inventory.code
      : "compensation_failed";

    await finish(order, activityMetadata, state, code);

    return {
      orderId,
      status: undone ? "compensated" : "failed",
      code,
      trace,
    };
  }

  // Create shipment.
  const shipping = await runStep(
    trace,
    "shipping",
    () => act.createShipment(orderId, activityMetadata),
    activityMetadata,
  );

  if (!shipping.ok) {
    const undone = await compensate(
      trace,
      orderId,
      true,
      paymentAuthorized,
      activityMetadata,
    );

    const state: OutcomeState = undone
      ? "compensated"
      : "compensation_failed";

    const code = undone
      ? shipping.code
      : "compensation_failed";

    await finish(order, activityMetadata, state, code);

    return {
      orderId,
      status: undone ? "compensated" : "failed",
      code,
      trace,
    };
  }

  // Send notification.
  const notification = await runStep(
    trace,
    "notification",
    () =>
      act.sendNotification(
        orderId,
        order.customer.email,
        activityMetadata,
      ),
    activityMetadata,
  );

  if (!notification.ok) {
    const undone = await compensate(
      trace,
      orderId,
      true,
      paymentAuthorized,
      activityMetadata,
    );

    const state: OutcomeState = undone
      ? "compensated"
      : "compensation_failed";

    const code = undone
      ? notification.code
      : "compensation_failed";

    await finish(order, activityMetadata, state, code);

    return {
      orderId,
      status: undone ? "compensated" : "failed",
      code,
      trace,
    };
  }

  // Checkout completed.
  await finish(
    order,
    activityMetadata,
    "completed",
  );

  return {
    orderId,
    status: "completed",
    trace,
  };
}
