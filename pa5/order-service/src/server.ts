import { randomUUID } from "node:crypto";
import express from "express";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  const orders = new Map<string, CanonicalOrder>();

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    const order = req.body as CanonicalOrder;

    if (
      typeof order?.orderId !== "string" ||
      typeof order?.customer?.email !== "string" ||
      !Array.isArray(order?.items)
    ) {
      res.status(400).json({ error: "not a canonical order" });
      return;
    }

    const correlationId = randomUUID();

    channel.publish("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });

    orders.set(correlationId, order);
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const correlationId = req.params.correlationId;
    const order = orders.get(correlationId);
    if (!order) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.status(200).json({ correlationId, order });
  });

  app.post("/dlq/replay", async (_req, res) => {
    let replayed = 0;

    while (true) {
      const msg = await channel.get("orders.dlq", { noAck: false });
      if (!msg) break;

      const originQueue = msg.properties.headers?.["originQueue"] as string;

      channel.publish("orders.return.exchange", originQueue, msg.content, {
        headers: withoutRetryHistory(msg.properties.headers),
        persistent: true,
      });

      channel.ack(msg);
      replayed++;
    }

    res.status(200).json({ replayed });
  });

  app.listen(PORT, () => {
    console.log(`[order-service] listening on ${PORT}`);
  });
}

main().catch(console.error);
