import { env, runInDurableObject } from "cloudflare:test";
import { expect, test } from "vitest";
import { defineStore, type StandardSchemaV1 } from "@durable/core";
import { createSqliteDb, type DurableObjectStorageLike } from "@durable/cloudflare";
import { SqliteRuntime } from "@durable/sqlite-runtime";

type Namespace = { newUniqueId(): unknown; get(id: unknown): any };

// The store suite's CAS race case, run wholly inside the object's I/O context.
// suites.workerd.mts skips it: there the test's promise is awaited from inside
// the object, and with only that promise moved to the test's context this
// case ends with ["b", "a"].
test("retries an updater after an interleaved CAS conflict, inside the object", async () => {
  const namespace = (env as unknown as { CONFORMANCE: Namespace }).CONFORMANCE;
  const stub = namespace.get(namespace.newUniqueId());

  const observed = await runInDurableObject(
    stub as never,
    async (_instance: unknown, state: { storage: DurableObjectStorageLike }) => {
      let validations = 0;
      let racing = false;
      let release!: () => void;
      const bothPrepared = new Promise<void>((resolve) => {
        release = resolve;
      });
      const schema: StandardSchemaV1<unknown, { messages: string[] }> = {
        "~standard": {
          version: 1 as const,
          vendor: "durable-conformance",
          async validate(value: unknown) {
            if (!racing) return { value: value as { messages: string[] } };
            validations++;
            if (validations === 2) release();
            if (validations <= 2) await bothPrepared;
            return { value: value as { messages: string[] } };
          },
        },
      };
      const RaceStore = defineStore("in-object-cas-race", schema, {
        initial: () => ({ messages: [] }),
      });
      const runtime = new SqliteRuntime(createSqliteDb(state.storage));
      let updaterRuns = 0;

      await runtime.stores.getStore({ definition: RaceStore, id: "race" });
      racing = true;
      const results = await Promise.all(
        ["a", "b"].map((id) =>
          runtime.stores.updateStore({
            definition: RaceStore,
            id: "race",
            updater(draft) {
              updaterRuns++;
              draft.messages.push(id);
            },
          })
        )
      );
      const final = await runtime.stores.getStore({ definition: RaceStore, id: "race" });
      return {
        updaterRuns,
        versions: results.map((result) => result.version).sort(),
        messages: final.state.messages,
        version: final.version,
      };
    }
  );

  expect(observed).toEqual({
    updaterRuns: 3,
    versions: [1, 2],
    messages: ["a", "b"],
    version: 2,
  });
});
