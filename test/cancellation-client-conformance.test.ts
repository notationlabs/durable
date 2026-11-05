import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { registerCancellationClientConformance } from "@yieldstar/runtime-conformance";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

type SqliteDb = ReturnType<typeof createSqliteDb>;

// The trigger aborts any update of the execution's row, so a cancel that
// propagates to it, or wakes it as a joiner, fails before it commits.
function failExecutionWrites(db: SqliteDb, executionId: string) {
  db.run(`
    CREATE TEMP TRIGGER fail_execution_write BEFORE UPDATE ON executions
    WHEN OLD.execution_id = '${executionId.replaceAll("'", "''")}'
    BEGIN SELECT RAISE(ABORT, 'injected execution write failure'); END
  `);
  return () => db.run("DROP TRIGGER temp.fail_execution_write");
}

registerCancellationClientConformance({
  name: "memory",
  create(options) {
    return { runtime: new MemoryRuntime(options) };
  },
});

registerCancellationClientConformance({
  name: isBun ? "sqlite-bun" : "sqlite-node",
  create(options) {
    const db = createSqliteDb({ path: ":memory:", wal: false });
    return {
      runtime: new SqliteRuntime(db, options),
      createPeer() {
        return new SqliteRuntime(db, options);
      },
      restart() {
        return new SqliteRuntime(db, options);
      },
      failExecutionWrites: (executionId) => failExecutionWrites(db, executionId),
      dispose() {
        db.close();
      },
    };
  },
});

// Separate connections to one database file, as separate processes would use.
registerCancellationClientConformance({
  name: isBun ? "sqlite-bun-file" : "sqlite-node-file",
  create(options) {
    const dir = mkdtempSync(join(tmpdir(), "yieldstar-cancellation-"));
    const path = join(dir, "cancellation.db");
    const dbs = [createSqliteDb({ path, wal: true })];
    const open = () => {
      const db = createSqliteDb({ path, wal: true });
      dbs.push(db);
      return new SqliteRuntime(db, options);
    };
    return {
      runtime: new SqliteRuntime(dbs[0], options),
      createPeer: open,
      restart: open,
      failExecutionWrites: (executionId) => failExecutionWrites(dbs[0], executionId),
      dispose() {
        for (const db of dbs) db.close();
        rmSync(dir, { recursive: true, force: true });
      },
    };
  },
});

test("a database created before cancellation reads its rows as attached, with no request", async () => {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  try {
    db.run(`
      CREATE TABLE executions (
        execution_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        params TEXT NOT NULL,
        context TEXT NOT NULL,
        status TEXT NOT NULL,
        outcome TEXT,
        admitted_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        due_at INTEGER,
        claimed_due_at INTEGER,
        lease_token INTEGER NOT NULL DEFAULT 0,
        lease_expires_at INTEGER,
        unacknowledged_delivery_count INTEGER NOT NULL DEFAULT 0,
        redeliver_at INTEGER,
        next_claim_at INTEGER,
        parent_execution_id TEXT,
        parent_step_key TEXT,
        admission_sequence INTEGER
      );
      CREATE TABLE start_requests (
        caller_execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        params TEXT,
        execution_id TEXT NOT NULL,
        PRIMARY KEY (caller_execution_id, step_key)
      );
      INSERT INTO executions (execution_id, workflow_id, params, context, status, admitted_at, updated_at, admission_sequence)
      VALUES ('order-42', 'place-order', '{}', '[]', 'suspended', 10, 10, 1);
      INSERT INTO executions (execution_id, workflow_id, params, context, status, admitted_at, updated_at, parent_execution_id, parent_step_key, admission_sequence)
      VALUES ('fulfil-42', 'fulfil-order', '{}', '[]', 'suspended', 10, 10, 'order-42', 'start:fulfil', 2);
      INSERT INTO start_requests (caller_execution_id, step_key, workflow_id, params, execution_id)
      VALUES ('order-42', 'start:fulfil', 'fulfil-order', NULL, 'fulfil-42');
    `);

    const runtime = new SqliteRuntime(db);
    // A second runtime on the same database finds the columns already added.
    const peer = new SqliteRuntime(db);
    expect(await peer.executions.get("order-42")).not.toHaveProperty("cancellation");
    expect((await peer.executions.get("fulfil-42"))!.parent).toEqual({
      executionId: "order-42",
      stepKey: "start:fulfil",
      detached: false,
    });
    expect(
      await runtime.executions.start({
        requestId: { executionId: "order-42", stepKey: "start:fulfil" },
        workflowId: "fulfil-order",
      })
    ).toEqual({ status: "accepted", executionId: "fulfil-42" });

    await runtime.executions.cancel({ executionId: "order-42", reason: "abandoned" });
    expect((await peer.executions.get("fulfil-42"))!.cancellation).toMatchObject({
      reason: "abandoned",
      source: "order-42",
    });
  } finally {
    db.close();
  }
});
