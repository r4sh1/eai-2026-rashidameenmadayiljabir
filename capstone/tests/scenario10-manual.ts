import { randomUUID } from "node:crypto";
import {
  publishRaw,
  spy,
  peek,
  exact,
  closeBroker
} from "./lib/broker.ts";

const sleep = (ms: number) =>
  new Promise(resolve => setTimeout(resolve, ms));

const invalidBaseline = 0;
const dlqBaseline = 1;
const bytes = Buffer.from("{ not json");
const retry = await spy("orders.retry.exchange", "saga.queue");
const dead = await spy("orders.dlq.exchange");

try {
  await publishRaw("orders.exchange", "", bytes, {
    messageId: randomUUID()
  });

  let arrived = false;

  for (let i = 0; i < 50; i++) {
    const count = await exact("orders.invalid");
    if (count.ready === invalidBaseline + 1) {
      arrived = true;
      break;
    }
    await sleep(200);
  }

  await sleep(2500);

  const invalidMessages = await peek("orders.invalid");
  const invalidCount = await exact("orders.invalid");
  const dlqCount = await exact("orders.dlq");

  const originalBytesPreserved =
    invalidMessages.some(m => m.content.equals(bytes));

  console.log(JSON.stringify({
    published: true,
    invalidArrived: arrived,
    invalidCount: invalidCount.ready,
    originalBytesPreserved,
    retryMessages: retry.messages.length,
    dlqExchangeMessages: dead.messages.length,
    dlqCount: dlqCount.ready,
    expectedInvalidCount: 1,
    expectedDlqCount: dlqBaseline
  }, null, 2));
} finally {
  await retry.stop();
  await dead.stop();
  await closeBroker();
}
