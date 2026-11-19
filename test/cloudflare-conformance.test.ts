import {
  registerCancellationClientConformance,
  registerEffectClientConformance,
  registerExecutionClientConformance,
  registerJoinClientConformance,
  registerStoreClientConformance,
} from "@durable/runtime-conformance";
import { createSqliteDb } from "@durable/cloudflare";
import { SqliteRuntime, type SqliteRuntimeOptions } from "@durable/sqlite-runtime";
import { createFakeStorage } from "./durable-object-storage";

const name = "sqlite-durable-object";

function create(options?: SqliteRuntimeOptions) {
  const fake = createFakeStorage();
  const open = () => new SqliteRuntime(createSqliteDb(fake.storage), options);
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
