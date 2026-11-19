
import express from "express";
import { appendFile, mkdir, readFile } from "node:fs/promises";
import path from "node:path";
import { connectWithRetry } from "../../shared/rabbit.js";

const app = express();

const PORT = Number(process.env.PORT ?? 8801);
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://eai:eai-capstone@localhost:5680";

const DATA_DIR = process.env.DATA_DIR ?? "/data";
const TRACE_FILE = path.join(DATA_DIR, "trace-store.jsonl");

const RABBITMQ_MANAGEMENT_URL =
  process.env.RABBITMQ_MANAGEMENT_URL ??
  "http://rabbitmq:15672";

const RABBITMQ_MANAGEMENT_USER =
  process.env.RABBITMQ_MANAGEMENT_USER ?? "eai";

const RABBITMQ_MANAGEMENT_PASSWORD =
  process.env.RABBITMQ_MANAGEMENT_PASSWORD ?? "eai-capstone";

const ORCHESTRATOR_URL =
  process.env.ORCHESTRATOR_URL ?? "http://orchestrator:8802";

const LAG_THRESHOLD = 3;

const QUEUE_NAMES = [
  "saga.queue",
  "notifications.queue",
  "orders.dlq",
  "orders.invalid",
  "trace.store"
] as const;

interface TraceHop {
  hopId: string;
  correlationId: string;
  causationId: string | null;
  service: string;
  operation: string;
  status: string;
  attempt: number;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  messageId?: string;
  detail?: Record<string, unknown>;
}

interface QueueStats {
  ready: number;
  unacked: number;
  consumers: number;
}

interface Alert {
  key: string;
  firing: boolean;
  value: number;
  threshold: number | null;
}

let brokerConnected = false;
let consumerReady = false;
let rabbitChannel: any = null;

const memoryTraceStore = new Map<string, TraceHop[]>();

async function ensureDataDirectory(): Promise<void> {
  await mkdir(DATA_DIR, { recursive: true });
}

function isTraceHop(value: unknown): value is TraceHop {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value)
  ) {
    return false;
  }

  const hop = value as Record<string, unknown>;

  return (
    typeof hop.hopId === "string" &&
    typeof hop.correlationId === "string" &&
    (typeof hop.causationId === "string" || hop.causationId === null) &&
    typeof hop.service === "string" &&
    typeof hop.operation === "string" &&
    typeof hop.status === "string" &&
    typeof hop.attempt === "number" &&
    typeof hop.startedAt === "string" &&
    typeof hop.finishedAt === "string" &&
    typeof hop.durationMs === "number"
  );
}

async function saveTraceHop(hop: TraceHop): Promise<void> {
  await ensureDataDirectory();

  await appendFile(
    TRACE_FILE,
    `${JSON.stringify(hop)}\n`,
    "utf8"
  );

  const existing = memoryTraceStore.get(hop.correlationId) ?? [];
  existing.push(hop);
  memoryTraceStore.set(hop.correlationId, existing);
}

async function loadTraceHops(correlationId: string): Promise<TraceHop[]> {
  try {
    const content = await readFile(TRACE_FILE, "utf8");

    const hops: TraceHop[] = [];

    for (const line of content.split(/\r?\n/)) {
      if (!line.trim()) {
        continue;
      }

      try {
        const parsed: unknown = JSON.parse(line);

        if (
          isTraceHop(parsed) &&
          parsed.correlationId === correlationId
        ) {
          hops.push(parsed);
        }
      } catch {
        // Ignore an incomplete or malformed JSONL line.
      }
    }

    return hops.sort(
      (a, b) =>
        Date.parse(a.startedAt) - Date.parse(b.startedAt)
    );
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ENOENT"
    ) {
      return [];
    }

    throw error;
  }
}

async function getQueueStats(queueName: string): Promise<QueueStats> {
  const encodedQueueName = encodeURIComponent(queueName);
  const url =
    `${RABBITMQ_MANAGEMENT_URL}/api/queues/%2F/${encodedQueueName}`;

  const authorization = Buffer.from(
    `${RABBITMQ_MANAGEMENT_USER}:${RABBITMQ_MANAGEMENT_PASSWORD}`
  ).toString("base64");

  try {
    const response = await fetch(url, {
      headers: {
        Authorization: `Basic ${authorization}`
      },
      signal: AbortSignal.timeout(3000)
    });

    if (!response.ok) {
      throw new Error(
        `RabbitMQ management returned HTTP ${response.status}`
      );
    }

    const data = await response.json() as Record<string, unknown>;

    return {
      ready: Number(data.messages_ready ?? 0),
      unacked: Number(data.messages_unacknowledged ?? 0),
      consumers: Number(data.consumers ?? 0)
    };
  } catch (error) {
    // Basic queue statistics remain available through AMQP if the
    // management API is temporarily unavailable.
    if (!rabbitChannel) {
      throw error;
    }

    const info = await rabbitChannel.checkQueue(queueName);

    return {
      ready: Number(info.messageCount ?? 0),
      unacked: 0,
      consumers: Number(info.consumerCount ?? 0)
    };
  }
}

async function getStuckSagaCount(): Promise<number> {
  try {
    const response = await fetch(
      `${ORCHESTRATOR_URL}/sagas/stuck`,
      {
        signal: AbortSignal.timeout(3000)
      }
    );

    if (!response.ok) {
      return 0;
    }

    const data = await response.json() as unknown;

    if (Array.isArray(data)) {
      return data.length;
    }

    if (
      typeof data === "object" &&
      data !== null &&
      "sagas" in data &&
      Array.isArray(data.sagas)
    ) {
      return data.sagas.length;
    }

    if (
      typeof data === "object" &&
      data !== null &&
      "count" in data &&
      typeof data.count === "number"
    ) {
      return data.count;
    }

    return 0;
  } catch {
    return 0;
  }
}

async function buildMetrics(): Promise<Record<string, unknown>> {
  const queueEntries = await Promise.all(
    QUEUE_NAMES.map(async (name) => [
      name,
      await getQueueStats(name)
    ] as const)
  );

  const queues = Object.fromEntries(queueEntries) as Record<
    string,
    QueueStats
  >;

  const dlqDepth =
    queues["orders.dlq"].ready +
    queues["orders.dlq"].unacked;

  const invalidDepth =
    queues["orders.invalid"].ready +
    queues["orders.invalid"].unacked;

  const stuckSagas = await getStuckSagaCount();

  const sagaQueue = queues["saga.queue"];
  const notificationQueue = queues["notifications.queue"];

  const alerts: Alert[] = [
    {
      key: "dlq_not_empty",
      firing: dlqDepth > 0,
      value: dlqDepth,
      threshold: null
    },
    {
      key: "invalid_not_empty",
      firing: invalidDepth > 0,
      value: invalidDepth,
      threshold: null
    },
    {
      key: "saga_consumer_missing",
      firing: sagaQueue.consumers === 0,
      value: sagaQueue.consumers,
      threshold: null
    },
    {
      key: "notifier_consumer_missing",
      firing: notificationQueue.consumers === 0,
      value: notificationQueue.consumers,
      threshold: null
    },
    {
      key: "saga_consumer_lag",
      firing: sagaQueue.ready > LAG_THRESHOLD,
      value: sagaQueue.ready,
      threshold: LAG_THRESHOLD
    },
    {
      key: "notifier_consumer_lag",
      firing: notificationQueue.ready > LAG_THRESHOLD,
      value: notificationQueue.ready,
      threshold: LAG_THRESHOLD
    },
    {
      key: "stuck_sagas",
      firing: stuckSagas > 0,
      value: stuckSagas,
      threshold: null
    }
  ];

  return {
    generatedAt: new Date().toISOString(),
    queues,
    dlqDepth,
    invalidDepth,
    stuckSagas,
    alerts
  };
}

app.get("/health", (_req, res) => {
  const healthy = brokerConnected && consumerReady;

  return res.status(healthy ? 200 : 503).json({
    status: healthy ? "ok" : "degraded",
    checks: {
      broker: brokerConnected ? "up" : "down",
      consumers: {
        "trace.store": consumerReady ? 1 : 0
      }
    }
  });
});

app.get("/trace/:cid", async (req, res) => {
  const correlationId = req.params.cid;

  try {
    const hops = await loadTraceHops(correlationId);

    if (hops.length === 0) {
      return res.status(404).json({
        code: "not_found",
        correlationId
      });
    }

    return res.status(200).json({
      correlationId,
      hops
    });
  } catch (error) {
    console.error("Could not read trace:", error);

    return res.status(500).json({
      code: "trace_read_failed"
    });
  }
});

app.get("/ops/metrics", async (_req, res) => {
  try {
    const metrics = await buildMetrics();
    return res.status(200).json(metrics);
  } catch (error) {
    console.error("Could not build operations metrics:", error);

    return res.status(503).json({
      code: "metrics_unavailable"
    });
  }
});

async function maintainRabbitConnection(): Promise<void> {
  while (true) {
    try {
      console.log("Connecting observer to RabbitMQ...");
      const { connection, channel } = await connectWithRetry(RABBITMQ_URL);

      rabbitChannel = channel;
      brokerConnected = true;
      consumerReady = false;

      await channel.consume("trace.store", (message: any) => {
        if (!message) {
          return;
        }

        void (async () => {
          try {
            const parsed: unknown = JSON.parse(
              message.content.toString("utf8")
            );

            if (!isTraceHop(parsed)) {
              throw new Error("Invalid trace event");
            }

            await saveTraceHop(parsed);
            channel.ack(message);
          } catch (error) {
            console.error("Error processing trace event:", error);
            try {
              channel.nack(message, false, false);
            } catch (nackError) {
              console.error("Could not reject trace message:", nackError);
            }
          }
        })();
      });

      consumerReady = true;
      console.log("Observer connected and consuming trace.store");

      await new Promise<void>((resolve) => {
        let settled = false;
        const disconnected = () => {
          if (settled) return;
          settled = true;
          resolve();
        };

        connection.once("close", disconnected);
        connection.on("error", (error: Error) => {
          console.error("RabbitMQ connection error:", error);
        });
        channel.once("close", disconnected);
      });

      brokerConnected = false;
      consumerReady = false;
      if (rabbitChannel === channel) {
        rabbitChannel = null;
      }
      console.warn("Observer lost RabbitMQ connection; reconnecting...");
    } catch (error) {
      brokerConnected = false;
      consumerReady = false;
      rabbitChannel = null;
      console.error("Observer RabbitMQ connection failed:", error);
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

async function startServer(): Promise<void> {
  try {
    await ensureDataDirectory();

    app.listen(PORT, "0.0.0.0", () => {
      console.log(`Observer listening on port ${PORT}`);
      console.log("Consuming and storing messages from trace.store");
      console.log(`Trace storage: ${TRACE_FILE}`);
      console.log("Trace and operations endpoints are ready");
    });

    void maintainRabbitConnection();
  } catch (error) {
    console.error("Could not start observer:", error);
    process.exit(1);
  }
}

void startServer();
