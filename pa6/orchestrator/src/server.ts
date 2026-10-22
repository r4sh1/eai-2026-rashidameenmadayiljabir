/**
 * PA6 orchestrator. POST /checkout runs payment, inventory, shipping and
 * notification strictly one after another. On a failure or a timeout it undoes
 * inventory and payment in reverse order. Idempotency and saga records are kept
 * in /data and survive a restart. The contract is ../openapi.yaml.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type NextFunction, type Request, type Response } from "express";
import axios from "axios";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import type { CanonicalOrder } from "./canonical-order";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json. Keep them in sync if you change
// either side — the tests validate persisted files against those schemas.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// The request body is a canonical order. docker-compose.yml mounts the
// course's canonical/ folder read-only at /canonical, so the orchestrator
// validates against the very file PA4's output was validated against.
const CANONICAL_SCHEMA_PATH = "/canonical/order.schema.json";

if (!fs.existsSync(CANONICAL_SCHEMA_PATH)) {
  throw new Error(
    `${CANONICAL_SCHEMA_PATH} not found. docker-compose.yml mounts ../canonical ` +
      "from your repository root: copy canonical/ from eai-2026 next to pa6/ " +
      '(root README, "Starting an assignment"), then run docker compose up again.',
  );
}

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCanonicalOrder = ajv.compile(JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8")));

// Must match maxItems on Order.items in openapi.yaml.
const MAX_ITEMS = 20;

function validateCheckoutPayload(payload: unknown): string | null {
  if (!validateCanonicalOrder(payload)) {
    return `Request body is not a canonical order: ${ajv.errorsText(validateCanonicalOrder.errors, { dataVar: "body" })}`;
  }
  // The canonical schema has no bound on items. The contract does, so enforce it here.
  const count = (payload as CanonicalOrder).items.length;
  if (count < 1 || count > MAX_ITEMS) {
    return `items must hold between 1 and ${MAX_ITEMS} entries, got ${count}`;
  }
  return null;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

// ------------------------------------------------------------------- saga --

type StepOutcome = { kind: "success" } | { kind: "failed"; code: string } | { kind: "timeout" };

interface Undo {
  step: "inventory_release" | "payment_refund";
  url: string;
  body: unknown;
}

interface Forward {
  step: "payment" | "inventory" | "shipping" | "notification";
  url: string;
  body: unknown;
  undo?: Undo;
}

interface SagaResult {
  httpStatus: number;
  state: SagaState;
  response: { orderId: string; status: SagaState; code?: string; trace: TraceItem[] };
}

// Sum unitPrice x quantity in whole cents (BigInt, never a float), format once.
function computeAmount(items: CanonicalOrder["items"]): string {
  let cents = 0n;
  for (const item of items) {
    const negative = item.unitPrice.startsWith("-");
    const [whole = "0", fraction = ""] = item.unitPrice.replace("-", "").split(".");
    const magnitude = BigInt(whole) * 100n + BigInt((fraction + "00").slice(0, 2));
    cents += (negative ? -magnitude : magnitude) * BigInt(item.quantity);
  }
  const sign = cents < 0n ? "-" : "";
  const abs = cents < 0n ? -cents : cents;
  return `${sign}${abs / 100n}.${(abs % 100n).toString().padStart(2, "0")}`;
}

function codeFromBody(data: unknown, fallback: string): string {
  if (data && typeof data === "object" && typeof (data as { code?: unknown }).code === "string") {
    return (data as { code: string }).code;
  }
  return fallback;
}

// One downstream call. Records exactly one trace entry. Never throws.
// A call that has not answered within REQUEST_TIMEOUT_MS is a timeout; any
// answer outside 2xx, or a refused connection, is a business failure.
async function callDownstream(step: string, url: string, body: unknown, trace: TraceItem[]): Promise<StepOutcome> {
  const startedMs = Date.now();
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, config.requestTimeoutMs);

  let outcome: StepOutcome;
  try {
    const res = await axios.post(url, body, {
      signal: controller.signal,
      timeout: config.requestTimeoutMs,
      validateStatus: () => true,
    });
    outcome =
      res.status >= 200 && res.status < 300
        ? { kind: "success" }
        : { kind: "failed", code: codeFromBody(res.data, `${step}_failed`) };
  } catch (err) {
    const axiosTimeout = axios.isAxiosError(err) && (err.code === "ECONNABORTED" || err.code === "ETIMEDOUT");
    outcome = timedOut || axiosTimeout ? { kind: "timeout" } : { kind: "failed", code: "downstream_unavailable" };
  } finally {
    clearTimeout(timer);
  }

  const finishedMs = Date.now();
  trace.push({
    step,
    status: outcome.kind,
    startedAt: new Date(startedMs).toISOString(),
    finishedAt: new Date(finishedMs).toISOString(),
    durationMs: finishedMs - startedMs,
  });
  return outcome;
}

// Runs the four steps one after another. On the first failure or timeout it
// undoes what completed, newest first. Compensation is best effort: if one
// undo fails, the next one is still tried, and the saga reports compensation_failed.
async function runSaga(order: CanonicalOrder): Promise<SagaResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];
  const steps: Forward[] = [
    {
      step: "payment",
      url: `${config.paymentUrl}/payment/authorize`,
      body: { orderId, amount: computeAmount(order.items) },
      undo: { step: "payment_refund", url: `${config.paymentUrl}/payment/refund`, body: { orderId } },
    },
    {
      step: "inventory",
      url: `${config.inventoryUrl}/inventory/reserve`,
      body: { orderId, items: order.items },
      undo: { step: "inventory_release", url: `${config.inventoryUrl}/inventory/release`, body: { orderId } },
    },
    { step: "shipping", url: `${config.shippingUrl}/shipping/create`, body: { orderId } },
    {
      step: "notification",
      url: `${config.notificationUrl}/notification/send`,
      body: { orderId, recipient: order.customer.email },
    },
  ];

  const undos: Undo[] = [];
  for (const step of steps) {
    const outcome = await callDownstream(step.step, step.url, step.body, trace);
    if (outcome.kind === "success") {
      if (step.undo) undos.push(step.undo);
      continue;
    }

    const timedOut = outcome.kind === "timeout";
    const code = outcome.kind === "failed" ? outcome.code : "timeout";

    let compensationFailed = false;
    for (const undo of [...undos].reverse()) {
      const undoOutcome = await callDownstream(undo.step, undo.url, undo.body, trace);
      if (undoOutcome.kind !== "success") compensationFailed = true;
    }

    if (compensationFailed) {
      return { httpStatus: 422, state: "failed", response: { orderId, status: "failed", code: "compensation_failed", trace } };
    }
    const state: SagaState = undos.length > 0 ? "compensated" : "failed";
    return { httpStatus: timedOut ? 504 : 422, state, response: { orderId, status: state, code, trace } };
  }

  return { httpStatus: 200, state: "completed", response: { orderId, status: "completed", trace } };
}

// Both stores are re-read right before writing. The saga awaits network calls,
// so another request may have written the files in the meantime; writing back
// an object read before the awaits would erase that request's record.
function persistResult(idempotencyKey: string, requestHash: string, result: SagaResult): void {
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  if (!sagaStore.sagas) sagaStore.sagas = {};
  sagaStore.sagas[result.response.orderId] = {
    idempotencyKey,
    state: result.state,
    steps: result.response.trace,
    updatedAt: nowIso(),
  };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) idempotencyStore.records = {};
  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: result.state,
    httpStatus: result.httpStatus,
    response: result.response,
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);
}

function releaseKey(idempotencyKey: string): void {
  const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (store.records) {
    delete store.records[idempotencyKey];
    writeJsonFile(IDEMPOTENCY_STORE_PATH, store);
  }
}

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey || idempotencyKey.length > 128) {
    res.status(400).json({
      code: "validation_error",
      message: "Idempotency-Key header is required and must be at most 128 characters",
    });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({ code: "validation_error", message: validationError });
    return;
  }

  const requestHash = payloadHash(req.body);

  // Everything from the read below to the in_progress write is synchronous, so
  // no other request can slip in between: one key is claimed by one request.
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) {
    idempotencyStore.records = {};
  }

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }
    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still being processed",
      });
      return;
    }
    // Finished earlier: replay the stored status and body, run nothing.
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  const order = req.body as CanonicalOrder;
  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId: order.orderId, status: "in_progress" },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  try {
    const result = await runSaga(order);
    persistResult(idempotencyKey, requestHash, result);
    res.status(result.httpStatus).json(result.response);
  } catch (err) {
    console.error("[orchestrator] unexpected error", err);
    try {
      releaseKey(idempotencyKey);
    } catch (releaseErr) {
      console.error("[orchestrator] could not release idempotency key", releaseErr);
    }
    res.status(500).json({ code: "internal_error", message: "Unexpected error while running the saga" });
  }
});

// express.json() rejects a broken or oversized body before the route runs, and
// its default answer is an HTML page. The contract promises the Problem shape.
app.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
  const type = (err as { type?: unknown } | null)?.type;
  if (typeof type === "string" && type.startsWith("entity.")) {
    res.status(400).json({
      code: "validation_error",
      message: type === "entity.too.large" ? "Request body is too large" : "Request body is not valid JSON",
    });
    return;
  }
  next(err);
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
