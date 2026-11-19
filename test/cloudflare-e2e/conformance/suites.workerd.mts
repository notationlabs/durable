import { env, runInDurableObject } from "cloudflare:test";
import {
  registerCancellationClientConformance,
  registerEffectClientConformance,
  registerExecutionClientConformance,
  registerJoinClientConformance,
  registerStoreClientConformance,
} from "@durable/runtime-conformance";
import { createSqliteDb, type DurableObjectStorageLike } from "@durable/cloudflare";
import { SqliteRuntime, type SqliteRuntimeOptions } from "@durable/sqlite-runtime";

const name = "workerd-durable-object";

type Namespace = { newUniqueId(): unknown; get(id: unknown): any };

type Enter = <R>(fn: () => R) => Promise<Awaited<R>>;

// Durable Object storage can only be used from inside the object's own I/O
// context. One `runInDurableObject` call stays open for each harness and runs
// queued calls there. Calls queued in the same turn start in the same turn,
// as they would in one process; a separate `runInDurableObject` per call
// would start each one a hop later and hide the races the suites set up.
function openContext(stub: unknown) {
  const queue: (() => void)[] = [];
  let wake: (() => void) | undefined;
  let open = true;
  const done = runInDurableObject(stub as never, async () => {
    while (open) {
      while (queue.length) queue.shift()!();
      await new Promise<void>((resolve) => (wake = resolve));
    }
  });
  const enter: Enter = (fn) =>
    new Promise((resolve, reject) => {
      queue.push(() => {
        try {
          Promise.resolve(fn()).then(resolve as never, reject);
        } catch (err) {
          reject(err);
        }
      });
      wake?.();
    });
  const close = async () => {
    open = false;
    wake?.();
    await done;
  };
  return { enter, close };
}

// Forwards every call on the runtime and its clients through `enter`. Calls
// that were synchronous return promises; `clock` stays synchronous because it
// does no I/O.
function forward<T extends object>(enter: Enter, target: T): T {
  return new Proxy(target, {
    get(object, key, receiver) {
      const value = Reflect.get(object, key, receiver);
      if (key === "clock" || typeof key === "symbol") return value;
      if (typeof value === "function") {
        return (...args: unknown[]) => enter(() => value.apply(object, args));
      }
      if (value && typeof value === "object") return forward(enter, value);
      return value;
    },
  });
}

async function create(options?: SqliteRuntimeOptions) {
  const namespace = (env as unknown as { CONFORMANCE: Namespace }).CONFORMANCE;
  const stub = namespace.get(namespace.newUniqueId());
  let storage: DurableObjectStorageLike | undefined;
  await runInDurableObject(stub as never, (_instance: unknown, state: { storage: DurableObjectStorageLike }) => {
    storage = state.storage;
  });
  const context = openContext(stub);
  const open = async () =>
    forward(context.enter, await context.enter(() => new SqliteRuntime(createSqliteDb(storage!), options)));
  return { runtime: await open(), createPeer: open, restart: open, dispose: context.close };
}

registerExecutionClientConformance({ name, create });
registerStoreClientConformance({ name, create: () => create() });
registerEffectClientConformance({ name, create });
registerJoinClientConformance({ name, create });
registerCancellationClientConformance({ name, create });
