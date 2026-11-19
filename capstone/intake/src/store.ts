
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

export interface StoredResponse {
  status: number;
  body: unknown;
}

export interface IdempotencyRecord {
  bodySha256: string;
  state: "in_flight" | "done";
  response?: StoredResponse;
  correlationId?: string;
  createdAt: string;
}

export type OrderState =
  | "accepted"
  | "processing"
  | "parked"
  | "completed"
  | "failed"
  | "compensated"
  | "compensation_failed"
  | "needs_attention"
  | "duplicate";

export interface OrderRecord {
  correlationId: string;
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  state: OrderState;
  code?: string;
  duplicateOf?: string;
  updatedAt: string;
}

async function readJson<T>(filePath: string): Promise<T> {
  try {
    const content = await readFile(filePath, "utf8");
    return JSON.parse(content) as T;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return {} as T;
    }
    throw error;
  }
}

async function writeJsonAtomic(
  filePath: string,
  data: unknown
): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });

  const temporaryPath = join(
    dirname(filePath),
    `.tmp-${randomUUID()}.json`
  );

  await writeFile(
    temporaryPath,
    JSON.stringify(data, null, 2),
    "utf8"
  );

  await rename(temporaryPath, filePath);
}

export async function loadIdempotencyStore(
  dataDir: string
): Promise<Record<string, IdempotencyRecord>> {
  return readJson(join(dataDir, "idempotency-store.json"));
}

export async function saveIdempotencyStore(
  dataDir: string,
  store: Record<string, IdempotencyRecord>
): Promise<void> {
  await writeJsonAtomic(
    join(dataDir, "idempotency-store.json"),
    store
  );
}

export async function loadOrders(
  dataDir: string
): Promise<Record<string, OrderRecord>> {
  return readJson(join(dataDir, "orders.json"));
}

export async function saveOrders(
  dataDir: string,
  orders: Record<string, OrderRecord>
): Promise<void> {
  await writeJsonAtomic(join(dataDir, "orders.json"), orders);
}
