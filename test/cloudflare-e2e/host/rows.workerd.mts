import { env } from "cloudflare:test";
import { expect, test } from "vitest";
import { createHttpSdkFactory } from "@durable/sdk";
import { written, type Router } from "./worker";

type Stub = { fetch(request: Request): Promise<Response> };
type Namespace = { newUniqueId(): unknown; get(id: unknown): Stub };

// Runs one execution in a new object, and returns what Cloudflare bills as
// rows written for it. The object's tables exist before the count starts.
async function rowsWritten(event: Parameters<ReturnType<typeof createSdk>["trigger"]>[0]) {
  const sdk = createSdk();
  await sdk.poll("none");
  written.rows = 0;
  written.alarmWrites = 0;
  const execution = await sdk.trigger(event);
  await execution.waitForResult();
  // The acknowledgement of the last delivery follows the result.
  await new Promise((resolve) => setTimeout(resolve, 100));
  return { ...written };
}

function createSdk() {
  const namespace = (env as unknown as { COUNTED: Namespace }).COUNTED;
  const stub = namespace.get(namespace.newUniqueId());
  return createHttpSdkFactory<Router>()({
    url: "http://object/durable",
    fetch: (input, init) => stub.fetch(new Request(input, init)),
  });
}

// These counts are what an execution costs on Cloudflare, at one millionth of
// a dollar for each row. A change that raises one of them raises that cost.

test("an execution with one step writes 11 rows and the alarm once", async () => {
  // 5 for the execution and its four index entries, 2 for the claim, 1 for
  // the step record, 1 for the workflow's result and 2 for the outcome.
  expect(await rowsWritten({ workflowId: "steps", params: { steps: 1 } })).toEqual({
    rows: 11,
    alarmWrites: 1,
  });
});

test("each further step writes one row", async () => {
  expect(await rowsWritten({ workflowId: "steps", params: { steps: 21 } })).toEqual({
    rows: 31,
    alarmWrites: 1,
  });
});

test("each delay writes 7 rows and the alarm twice", async () => {
  const one = await rowsWritten({ workflowId: "delays", params: { delays: 1 } });
  const four = await rowsWritten({ workflowId: "delays", params: { delays: 4 } });
  // A further delay writes 2 rows when the delivery suspends, 2 for the claim
  // when the alarm fires, 2 for the delay's two records and 1 for the step.
  // It sets the alarm to the end of the delay, and to the end of the lease
  // once the alarm has fired. The first delay writes one row more, for the
  // status index.
  expect((four.rows - one.rows) / 3).toBe(7);
  expect((four.alarmWrites - one.alarmWrites) / 3).toBe(2);
  expect(one).toEqual({ rows: 18, alarmWrites: 3 });
});
