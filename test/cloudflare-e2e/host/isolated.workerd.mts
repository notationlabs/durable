import { env } from "cloudflare:test";
import { expect, test } from "vitest";
import { createHttpSdkFactory } from "@durable/sdk";
import { HOLD, held, isolated, type Router } from "./worker";

type Stub = { fetch(request: Request): Promise<Response> };
type Namespace = { newUniqueId(): unknown; get(id: unknown): Stub };

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// The object finds the namespace of its own class, and calls the instance
// that runs a delivery over workerd's RPC. Local workerd puts every instance
// in one isolate, so these tests cover the calls and not the CPU limit.
function createObject() {
  const namespace = (env as unknown as { ISOLATED: Namespace }).ISOLATED;
  const id = namespace.newUniqueId();
  return createHttpSdkFactory<Router>()({
    url: "http://object/durable",
    fetch: (input, init) => namespace.get(id).fetch(new Request(input, init)),
  });
}

test("each delivery runs in another instance of the class", async () => {
  const sdk = createObject();
  const before = isolated.constructed;

  const execution = await sdk.trigger({ workflowId: "parent" });

  expect(await execution.waitForResult()).toBe(21);
  // The object that claims, and at least one instance that runs deliveries.
  expect(isolated.constructed - before).toBeGreaterThanOrEqual(2);
});

test("a delay resumes when the alarm fires", async () => {
  const sdk = createObject();

  const execution = await sdk.trigger({ workflowId: "delayed", params: { ms: 300 } });

  expect(await execution.waitForResult()).toBeGreaterThanOrEqual(295);
});

test("a forced cancellation stops a step that is waiting", async () => {
  const sdk = createObject();
  const key = crypto.randomUUID();

  const { executionId } = await sdk.trigger({ workflowId: "hold", params: { key } });
  await expect.poll(() => held.started.has(key)).toBe(true);
  await sdk.cancel(executionId, { force: true });

  expect(await sdk.wait(executionId, { timeout: 5_000 })).toMatchObject({ status: "cancelled" });
  await sleep(HOLD + 300);
  expect(held.finished.has(key)).toBe(false);
});

test("a step that is not cancelled runs to its end", async () => {
  const sdk = createObject();
  const key = crypto.randomUUID();

  const execution = await sdk.trigger({ workflowId: "hold", params: { key } });

  expect(await execution.waitForResult()).toBe("held");
  expect(held.finished.has(key)).toBe(true);
});
