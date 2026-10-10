import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");

const IDS_FILE = "/data/processed-ids.json";
const LOG_FILE = "/data/notification.log";

class PermanentError extends Error {}

function loadProcessedIds(): string[] {
  if (!existsSync(IDS_FILE)) return [];
  return JSON.parse(readFileSync(IDS_FILE, "utf8")) as string[];
}

function saveProcessedIds(ids: string[]): void {
  writeFileSync(IDS_FILE, JSON.stringify(ids));
}

async function main(): Promise<void> {
  if (!existsSync(IDS_FILE)) saveProcessedIds([]);

  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);
    console.log(`[Notification] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      if (!correlationId) {
        throw new PermanentError("missing correlationId header");
      }

      const processed = loadProcessedIds();
      if (processed.includes(correlationId)) {
        channel.ack(msg);
        console.log(`[Notification] duplicate skipped: ${correlationId}`);
        return;
      }

      let order: CanonicalOrder;
      try {
        order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      } catch {
        throw new PermanentError("body is not valid JSON");
      }

      const line = {
        correlationId,
        orderId: order.orderId,
        customerEmail: order.customer.email,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };
      appendFileSync(LOG_FILE, JSON.stringify(line) + "\n");

      processed.push(correlationId);
      saveProcessedIds(processed);

      const result = {
        correlationId,
        source: "notification",
        status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Order received" },
      };
      channel.publish(RESULTS_EXCHANGE, "", Buffer.from(JSON.stringify(result)), {
        headers: { correlationId },
        contentType: "application/json",
        persistent: true,
      });
      channel.ack(msg);
      console.log(`[Notification] OK ${correlationId}`);
    } catch (err) {
      if (err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> INVALID: ${err.message}`);
        return;
      }

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);
