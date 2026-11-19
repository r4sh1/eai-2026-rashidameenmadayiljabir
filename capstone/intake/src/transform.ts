/**
 * PA4 — three sources to one canonical order model.
 *
 * Three systems you do not control each send you an order in their own
 * shape:
 *
 *   data/web-order.json      nested JSON, web store
 *   data/mobile-order.json   flat JSON, abbreviated field names, mobile app
 *   data/b2b-order.xml       EDI-style XML, CP1257 ("windows-1257"), B2B partner
 *
 * Your job is the message translator, content enricher and content filter
 * patterns from this week's session, applied together:
 *
 *   1. Translator  — map each source's shape onto ../../canonical/order.schema.json.
 *      That schema is fixed. You conform to it; you do not adjust it.
 *   2. Enricher    — the sources do not carry a trustworthy price. Normalize
 *      each line item's product id to the pricing API's PROD-XXX format, call
 *      the API, and use ITS price, currency and tax rate. A price you typed
 *      into this file yourself is a hardcoded price, and a public test
 *      changes a price in the mock catalog specifically to catch that.
 *   3. Filter      — all three sources carry payment details (a card number,
 *      or an IBAN). The canonical schema has no field for any of it.
 *      Whatever you do with it, it must not reach the object you return.
 *
 * Everything you need to call the pricing API is in the Node standard
 * library (global fetch). The one dependency here, fast-xml-parser, exists
 * because hand-rolling an XML tokenizer is not this week's lesson — reading
 * the object it gives you back correctly is.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { XMLParser } from "fast-xml-parser";


export interface Address {
  street: string;
  city: string;
  postalCode: string;
  country: string;
}

export interface CanonicalCustomer {
  name: string;
  email: string;
  address: Address;
}

export interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  /** Decimal STRING, e.g. "24.99" — see toDecimalAmount() below. Never a number. */
  unitPrice: string;
  currency: string;
  taxRate: number;
}

export type OrderStatus = "new" | "processing" | "shipped" | "delivered";

export interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  /** When this translator produced the record — new Date().toISOString(), or options.now(). */
  receivedAt: string;
  /** The order's own timestamp, normalized to ISO-8601 UTC. */
  orderDate: string;
  customer: CanonicalCustomer;
  items: CanonicalItem[];
  currency: string;
  status: OrderStatus;
}

export type TransformWarningCode = "UNKNOWN_PRODUCT" | "PRICING_API_ERROR";

export interface TransformWarning {
  code: TransformWarningCode;
  productId: string;
  message: string;
}

export interface TransformResult {
  /**
   * null when every line item failed enrichment (so there is nothing left
   * that would validate against the schema's `minItems: 1`). Otherwise a
   * schema-valid order — possibly with fewer items than the source, if some
   * of them could not be priced. See warnings for what happened to each one.
   */
  order: CanonicalOrder | null;
  warnings: TransformWarning[];
}

export interface TranslateOptions {
  /** e.g. "http://localhost:4100" (docker-compose's host port mapping). */
  pricingBaseUrl: string;
  /** Defaults to the key docker-compose.yml sets for the mock-pricing container. */
  apiKey?: string;
  /** Injectable clock, for deterministic tests. Defaults to () => new Date(). */
  now?: () => Date;
}

export const DEFAULT_PRICING_API_KEY = "pa4-pricing-key-2026";


const PA4_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_WEB_ORDER_PATH = path.join(PA4_ROOT, "data", "web-order.json");
export const DEFAULT_MOBILE_ORDER_PATH = path.join(PA4_ROOT, "data", "mobile-order.json");
export const DEFAULT_B2B_ORDER_PATH = path.join(PA4_ROOT, "data", "b2b-order.xml");
export const DEFAULT_PRICING_BASE_URL = "http://localhost:4100";

const OUT_DIR = path.join(PA4_ROOT, "out");


export interface WebOrderInput {
  orderId: string;
  orderType: string;
  customer: {
    name: string;
    email: string;
    address: Address;
    payment: {
      method: string;
      cardHolder: string;
      cardNumber: string;
      expiryMonth: number;
      expiryYear: number;
    };
  };
  items: Array<{ productId: string; productName: string; quantity: number }>;
  orderDate: string;
  status: string;
  currency: string;
}

export interface MobileOrderInput {
  oid: string;
  ot: string;
  cust_name: string;
  cust_email: string;
  /** "Street, City, PostalCode, Country" — one comma-separated string. */
  addr: string;
  items: Array<{ pid: string; pname: string; qty: number }>;
  /** Unix epoch, seconds. */
  ts: number;
  /** 1=new, 2=processing, 3=shipped, 4=delivered. */
  st: number;
  /** ISO 4217 numeric currency code, e.g. 978 = EUR. */
  cur: number;
  pm: string;
  pan: string;
  pexp: string;
}

/**
 * The shape fast-xml-parser (options below) gives back for b2b-order.xml.
 * Attributes come back as keys prefixed "@_" — an element
 * `<LineItem sku="X" quantity="2">` parses to
 * `{ "@_sku": "X", "@_quantity": "2", Description: "..." }`.
 *
 * This type is deliberately loose (not every element is listed) — the XML is
 * allowed to carry elements you do not map, and a hidden test sends you one
 * with an extra element it has never told you about. A crash there is a
 * failure; a translator that just does not recognize the element and moves on
 * is the tolerant reader pattern this week is about.
 */
export interface ParsedB2BOrder {
  PurchaseOrder: {
    "@_orderId": string;
    "@_orderType": string;
    "@_orderDate": string;
    BuyerParty: {
      Name: string;
      ContactEmail: string;
      ShipToAddress: {
        "@_country": string;
        Street: string;
        City: string;
        PostalCode: string;
      };
      [key: string]: unknown;
    };
    LineItems: {
      "@_currency": string;
      /**
       * fast-xml-parser gives you a single object (not a one-element array)
       * when there is exactly one <LineItem>, and an array when there is
       * more than one. Both of this assignment's fixtures have two, but do
       * not assume that stays true forever — normalize to an array before
       * you iterate.
       */
      LineItem: unknown;
    };
    Status: string;
    [key: string]: unknown;
  };
}

const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });


/**
 * Normalize a source-specific product id to the pricing API's own format,
 * "PROD-XXX", before you call it.
 *
 * TODO:
 *   web:    already "PROD-XXX"      -> unchanged
 *   mobile: numeric only, e.g "001" -> prepend "PROD-"    -> "PROD-001"
 *   b2b:    "SKU-PROD-XXX"          -> strip the "SKU-" prefix -> "PROD-XXX"
 */
export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
    const id = String(rawId ?? "").trim();
  if (source === "b2b") return id.replace(/^SKU-/i, "");
  if (source === "mobile" && /^[0-9]+$/.test(id)) return `PROD-${id}`;
  return id;
}

/**
 * A pricing API response's price, as a decimal STRING with exactly two
 * fraction digits: 24.99 -> "24.99", 4.5 -> "4.50", 22 -> "22.00".
 *
 * TODO: This is the other half of judgment call #1. `String(4.5)` gives you
 * "4.5", not "4.50" — a later assignment plants exactly that bug ("flip a
 * decimal string to a float") and expects you to recognize it. Do not round
 * through a float representation more than once; the pricing API already
 * gave you a number, so this function's only job is formatting it, not
 * doing arithmetic on it.
 */
export function toDecimalAmount(price: number): string {
    // go through cents as an integer so 4.5 doesn't turn into "4.5" instead of "4.50"
  const cents = Math.round(Math.abs(price) * 100);
  const sign = price < 0 && cents !== 0 ? "-" : "";
  const whole = Math.floor(cents / 100);
  const frac = String(cents % 100).padStart(2, "0");
  return `${sign}${whole}.${frac}`;
}

export type EnrichResult =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number; productName: string }
  | { status: "unknown_product" }
  | { status: "error"; httpStatus: number };

/**
 * Call GET {pricingBaseUrl}/pricing/{productId} with the X-API-Key header
 * and turn its response into an EnrichResult.
 *
 * TODO:
 *   - 200: read unitPrice/currency/taxRate/productName from the body. Format
 *     unitPrice with toDecimalAmount() — do not pass the API's number straight
 *     through, and do not round-trip it through parseFloat/toString yourself.
 *   - 404: return { status: "unknown_product" }. Do not throw, and do not
 *     invent a price.
 *   - any other non-2xx (401, 500, a network failure): return
 *     { status: "error", httpStatus }. Never fall back to a price of 0 — a
 *     hidden test sends this a 500 specifically to check you did not.
 */
export async function enrichFromPricing(
  productId: string,
  options: TranslateOptions,
): Promise<EnrichResult> {
    // if this isn't a real PROD-xxx id, don't even bother hitting the API
  if (!/^PROD-[0-9]+$/.test(productId)) return { status: "unknown_product" };
  const base = options.pricingBaseUrl.replace(/\/+$/, "");
  let res: Response;
  try {
    res = await fetch(`${base}/pricing/${encodeURIComponent(productId)}`, {
      headers: { "X-API-Key": options.apiKey ?? DEFAULT_PRICING_API_KEY },
      signal: AbortSignal.timeout(5000),
    });
  } catch {
    // fetch itself blew up (no server, timeout, etc) - treat like any other failure
    return { status: "error", httpStatus: 0 }; // network failure or timeout
  }
  if (res.status === 404) return { status: "unknown_product" };
  if (!res.ok) return { status: "error", httpStatus: res.status };
  try {
    const body = (await res.json()) as Record<string, unknown>;
    const price = Number(body.unitPrice);
    const taxRate = Number(body.taxRate);
    if (!Number.isFinite(price) || !Number.isFinite(taxRate) || typeof body.currency !== "string") {
      return { status: "error", httpStatus: res.status };
    }
    return {
      status: "ok",
      unitPrice: toDecimalAmount(price),
      currency: body.currency,
      taxRate,
      productName: String(body.productName ?? ""),
    };
  } catch {
    return { status: "error", httpStatus: res.status };
  }
}

/**
 * Enrich a list of (already normalized) product ids/quantities/names against
 * the pricing API, splitting the results into priced items and warnings.
 *
 * TODO: For each input line, call enrichFromPricing(). On "ok", produce a
 * CanonicalItem (quantity and productName come from the SOURCE line, not
 * from the pricing API's productName, unless you decide otherwise — say so
 * in your ADR if you do). On "unknown_product" or "error", push a
 * TransformWarning instead of an item, and do not let one bad line item stop
 * the others from being priced (same lesson as PA1: one bad record does not
 * take down the whole batch).
 */
export async function enrichItems(
  lines: Array<{ productId: string; productName: string; quantity: number }>,
  options: TranslateOptions,
): Promise<{ items: CanonicalItem[]; warnings: TransformWarning[] }> {
    const results = await Promise.all(lines.map((l) => enrichFromPricing(l.productId, options)));
  const items: CanonicalItem[] = [];
  const warnings: TransformWarning[] = [];
  lines.forEach((line, i) => {
    const r = results[i];
    if (!r) return; // shouldn't happen, results and lines are the same length // unreachable: results has exactly one entry per line, in order
    if (r.status === "ok") {
      items.push({
        productId: line.productId,
        productName: line.productName || r.productName,
        quantity: line.quantity,
        unitPrice: r.unitPrice,
        currency: r.currency,
        taxRate: r.taxRate,
      });
    } else if (r.status === "unknown_product") {
      warnings.push({
        code: "UNKNOWN_PRODUCT",
        productId: line.productId,
        message: `Pricing API has no product ${line.productId} (404); item dropped from the order.`,
      });
    } else {
      warnings.push({
        code: "PRICING_API_ERROR",
        productId: line.productId,
        message: `Pricing lookup for ${line.productId} failed (HTTP ${r.httpStatus || "no response"}); item dropped, no price substituted.`,
      });
    }
  });
  return { items, warnings };
}

/**
 * Unix epoch (seconds, UTC) -> ISO-8601: 1788258600 -> "2026-09-01T10:30:00.000Z".
 *
 * TODO: `new Date(seconds * 1000).toISOString()` is the whole function. The
 * TODO is remembering the *1000 — this is seconds, not milliseconds.
 */
export function epochSecondsToIso(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}

/**
 * ISO 4217 numeric currency code -> alpha code. This course's fixtures only
 * need: 978 -> EUR, 840 -> USD, 826 -> GBP.
 *
 * TODO: handle at least those three. Decide what to do with a code you do
 * not recognize (throw, or pass the number through as a string) and say why
 * in your ADR — this is a real tolerant-reader judgment call, not a trick.
 */
export function mapCurrencyCode(numericCode: number): string {
      // only the 3 codes this course actually uses - if we get something else just pass it through as a string
  const known: Record<number, string> = { 978: "EUR", 840: "USD", 826: "GBP" };
  return known[numericCode] ?? String(numericCode);
}

/**
 * Mobile's integer status enum -> canonical status string.
 * 1=new, 2=processing, 3=shipped, 4=delivered.
 *
 * TODO: implement the mapping above.
 */
export function mapMobileStatus(code: number): OrderStatus {
    const map: Record<number, OrderStatus> = { 1: "new", 2: "processing", 3: "shipped", 4: "delivered" };
  const status = map[code];
  if (!status) throw new Error(`Unknown mobile status code: ${code}`);
  return status;
}

/**
 * B2B's uppercase status string -> canonical status string. "NEW" -> "new".
 *
 * TODO: lowercase it. Decide what happens to a value you do not recognize.
 */
export function mapB2BStatus(raw: string): OrderStatus {
    const status = String(raw ?? "").trim().toLowerCase();
  if (status === "new" || status === "processing" || status === "shipped" || status === "delivered") return status;
  throw new Error(`Unknown B2B status: ${JSON.stringify(raw)}`);
}

/**
 * "Brīvības iela 100, Rīga, LV-1001, LV" -> { street, city, postalCode, country }
 *
 * TODO: split on ", " (comma-space) into exactly four parts, in that order.
 * This is string work, not a library.
 */
export function parseMobileAddress(addr: string): Address {
      // mobile sends address as one comma-separated string, split it back into pieces
  const parts = addr.split(",").map((part) => part.trim());
  if (parts.length < 4) throw new Error(`Cannot parse mobile address: ${JSON.stringify(addr)}`);
  const tail = parts.slice(-3);
  const city = tail[0];
  const postalCode = tail[1];
  const country = tail[2];
  if (!city || !postalCode || !country) {
    throw new Error(`Cannot parse mobile address: ${JSON.stringify(addr)}`);
  }
  return { street: parts.slice(0, -3).join(", "), city, postalCode, country };
}

/**
 * Decode the B2B XML file's raw bytes into text.
 *
 * TODO: The file declares `encoding="windows-1257"` in its XML prolog, and
 * it means it — reading these bytes as UTF-8 will silently corrupt every
 * Latvian diacritic in it (you will get "?" characters, not an error).
 * `TextDecoder` knows the label "windows-1257". This is the same lesson as
 * PA1's order file, applied to XML instead of a fixed-width record.
 */
export function decodeB2BXmlBytes(bytes: Buffer): string {
    // read the encoding from the xml prolog before decoding the rest of the file
  const prolog = bytes.subarray(0, 200).toString("latin1");
  const label = /<\?xml[^>]*encoding\s*=\s*["']([^"']+)["']/i.exec(prolog)?.[1] ?? "utf-8";
  try {
    return new TextDecoder(label).decode(bytes);
  } catch {
    return new TextDecoder("utf-8").decode(bytes);
  }
}


/**
 * TODO:
 *   - Read and JSON.parse webOrderPath as a WebOrderInput.
 *   - Map customer/address across unchanged (this source is already closest
 *     to canonical) — EXCEPT customer.payment, which must not appear
 *     anywhere in the object you return.
 *   - Normalize each item's productId (identity for web) and enrich via
 *     enrichItems().
 *   - orderType is already "standard"/"express"/"b2b" for this source; source
 *     is "web"; status and currency pass through unchanged (already
 *     canonical shape for this source).
 *   - orderDate: re-parse and re-serialize it (`new Date(x).toISOString()`),
 *     do not pass the source's string straight through. It already LOOKS
 *     like ISO-8601, but "looks like" is not "is": your B2B/mobile sources
 *     also produce ISO-8601 for the same instant, from a different
 *     representation, and toISOString() is what guarantees all three come
 *     out byte-for-byte identical (milliseconds included) rather than
 *     merely numerically equal.
 *   - Set receivedAt from options.now?.() ?? new Date().
 */
export async function translateWeb(
  webOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
      // web is closest to canonical already, mostly just mapping fields across
  const src = JSON.parse(readFileSync(webOrderPath, "utf8")) as WebOrderInput;
  const { items, warnings } = await enrichItems(
    (src.items ?? []).map((i) => ({
      productId: normalizeProductId("web", i.productId),
      productName: i.productName,
      quantity: i.quantity,
    })),
    options,
  );
  const order: CanonicalOrder | null =
    items.length === 0
      ? null
      : {
          orderId: src.orderId,
          orderType: src.orderType as CanonicalOrder["orderType"],
          source: "web",
          receivedAt: (options.now?.() ?? new Date()).toISOString(),
          orderDate: new Date(src.orderDate).toISOString(),
          customer: {
            name: src.customer.name,
            email: src.customer.email,
            address: {
              street: src.customer.address.street,
              city: src.customer.address.city,
              postalCode: src.customer.address.postalCode,
              country: src.customer.address.country,
            },
          },
          items,
          currency: src.currency,
          status: src.status as OrderStatus,
        };
  return { order, warnings };
}

/**
 * TODO:
 *   - Read and JSON.parse mobileOrderPath as a MobileOrderInput.
 *   - cust_name/cust_email -> customer.name/email. addr -> parseMobileAddress().
 *   - ts -> orderDate via epochSecondsToIso(). st -> status via mapMobileStatus().
 *   - cur -> currency via mapCurrencyCode().
 *   - Each item: pid -> normalizeProductId("mobile", pid), pname -> productName,
 *     qty -> quantity. Enrich via enrichItems().
 *   - ot -> orderType ("express" in the given fixture). source is "mobile".
 *   - pm/pan/pexp must not appear anywhere in the object you return.
 */
export async function translateMobile(
  mobileOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
      // mobile uses short field names and needs a few conversions (epoch, currency code, status code)
  const src = JSON.parse(readFileSync(mobileOrderPath, "utf8")) as MobileOrderInput;
  const { items, warnings } = await enrichItems(
    (src.items ?? []).map((i) => ({
      productId: normalizeProductId("mobile", i.pid),
      productName: i.pname,
      quantity: i.qty,
    })),
    options,
  );
  const order: CanonicalOrder | null =
    items.length === 0
      ? null
      : {
          orderId: src.oid,
          orderType: src.ot as CanonicalOrder["orderType"],
          source: "mobile",
          receivedAt: (options.now?.() ?? new Date()).toISOString(),
          orderDate: epochSecondsToIso(src.ts),
          customer: {
            name: src.cust_name,
            email: src.cust_email,
            address: parseMobileAddress(src.addr),
          },
          items,
          currency: mapCurrencyCode(src.cur),
          status: mapMobileStatus(src.st),
        };
  return { order, warnings };
}

/**
 * TODO:
 *   - Read b2bOrderPath as BYTES (not as a string — decodeB2BXmlBytes needs
 *     the raw bytes to decode correctly).
 *   - Decode with decodeB2BXmlBytes(), then xmlParser.parse() the result.
 *   - Map BuyerParty.Name/ContactEmail/ShipToAddress -> customer.
 *     PaymentDetails (wherever it lives) must not appear anywhere in the
 *     object you return.
 *   - Normalize LineItems.LineItem to an array (see ParsedB2BOrder's note),
 *     then for each: "@_sku" -> normalizeProductId("b2b", sku),
 *     Description -> productName, "@_quantity" -> quantity (this arrives as
 *     a string attribute — parse it to a number). Ignore any UnitListPrice
 *     element if you find one: it is a legacy list price, not a live price,
 *     and a public test changes the REAL price in the mock catalog
 *     specifically to check you did not use it.
 *   - Status -> status via mapB2BStatus(). "@_orderType" -> orderType ("b2b").
 *     source is "b2b". "@_orderId" -> orderId. "@_orderDate" -> orderDate,
 *     but re-parse and re-serialize it (`new Date(x).toISOString()`) — same
 *     reason as translateWeb: it must come out identical to the other two
 *     sources' orderDate, not merely the same instant.
 *   - Ignore any element you do not recognize (ContractDate, Notes, or
 *     anything a hidden test adds that is not documented here) rather than
 *     throwing on it.
 */
export async function translateB2B(
  b2bOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
      // b2b is xml with attributes, has to be decoded and parsed before we can map anything
  const parsed = xmlParser.parse(decodeB2BXmlBytes(readFileSync(b2bOrderPath))) as ParsedB2BOrder;
  const po = parsed.PurchaseOrder;
  const buyer = po.BuyerParty;
  const ship = buyer.ShipToAddress;
    // fast-xml-parser gives back one object for a single LineItem, an array for more than one
  const rawLines = po.LineItems?.LineItem;
  const lineList: Array<Record<string, unknown>> = Array.isArray(rawLines)
    ? rawLines
    : rawLines == null
      ? []
      : [rawLines as Record<string, unknown>];
  const { items, warnings } = await enrichItems(
    lineList.map((li) => ({
      productId: normalizeProductId("b2b", String(li["@_sku"] ?? "")),
      productName: String(li["Description"] ?? ""),
      quantity: parseInt(String(li["@_quantity"] ?? ""), 10),
    })),
    options,
  );
  const order: CanonicalOrder | null =
    items.length === 0
      ? null
      : {
          orderId: String(po["@_orderId"]),
          orderType: String(po["@_orderType"]) as CanonicalOrder["orderType"],
          source: "b2b",
          receivedAt: (options.now?.() ?? new Date()).toISOString(),
          orderDate: new Date(String(po["@_orderDate"])).toISOString(),
          customer: {
            name: String(buyer.Name),
            email: String(buyer.ContactEmail),
            address: {
              street: String(ship.Street),
              city: String(ship.City),
              postalCode: String(ship.PostalCode),
              country: String(ship["@_country"]),
            },
          },
          items,
          currency: String(po.LineItems["@_currency"]),
          status: mapB2BStatus(String(po.Status)),
        };
  return { order, warnings };
}


/** Runs all three translators against this assignment's own fixtures and writes pa4/out/*.json. Run with: npm start */
export async function main(): Promise<void> {
  const options: TranslateOptions = { pricingBaseUrl: DEFAULT_PRICING_BASE_URL };

  const results = {
    web: await translateWeb(DEFAULT_WEB_ORDER_PATH, options),
    mobile: await translateMobile(DEFAULT_MOBILE_ORDER_PATH, options),
    b2b: await translateB2B(DEFAULT_B2B_ORDER_PATH, options),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  for (const [name, result] of Object.entries(results)) {
    const outPath = path.join(OUT_DIR, `${name}.json`);
    writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n", "utf8");
    console.log(`wrote ${outPath}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
