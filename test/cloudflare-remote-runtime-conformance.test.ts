import {
  registerCancellationClientConformance,
  registerEffectClientConformance,
  registerExecutionClientConformance,
  registerJoinClientConformance,
  registerStoreClientConformance,
} from "@durable/runtime-conformance";
import { createSqliteDb } from "@durable/cloudflare";
import type { Runtime } from "@durable/core";
import { SqliteRuntime, type SqliteRuntimeOptions } from "@durable/sqlite-runtime";
import { RemoteRuntime, serveRuntime } from "../packages/cloudflare/src/remote-runtime";
import { createFakeStorage } from "./durable-object-storage";

// The runtime a delivery uses when it runs in its own Durable Object: each
// call a delivery makes is encoded, sent to the SQLite runtime and decoded
// again. The calls that only a dispatcher or an SDK makes go to the SQLite
// runtime directly, as they do in the object that claims the delivery.
const name = "remote-durable-object";

function create(options?: SqliteRuntimeOptions) {
  const fake = createFakeStorage();
  const open = (): Runtime => {
    const local = new SqliteRuntime(createSqliteDb(fake.storage), options);
    const remote = new RemoteRuntime(serveRuntime(local), options);
    const executions = new Proxy(local.executions, {
      get: (target, method) => {
        if (method === "start") return remote.executions.start;
        const value = Reflect.get(target, method, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    return {
      clock: local.clock,
      executions,
      effects: remote.effects,
      heap: remote.heap,
      joins: remote.joins,
      stores: remote.stores,
    };
  };
  return {
    runtime: open(),
    createPeer: open,
    restart: open,
    dispose: () => fake.close(),
  };
}

registerExecutionClientConformance({ name, create });
registerStoreClientConformance({ name, create: () => create() });
registerEffectClientConformance({ name, create });
registerJoinClientConformance({ name, create });
registerCancellationClientConformance({ name, create });
