import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { registerExecutionClientConformance } from "@yieldstar/runtime-conformance";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime, type SqliteParameters } from "@yieldstar/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

type SqliteDb = ReturnType<typeof createSqliteDb>;

// The trigger aborts the start-request insert, after the target's insert in the
// same transaction, so the case checks that the target rolls back with it.
function failStartCommits(db: SqliteDb) {
  db.run(`
    CREATE TEMP TRIGGER fail_start_commit BEFORE INSERT ON start_requests
    BEGIN SELECT RAISE(ABORT, 'injected start failure'); END
  `);
  return () => db.run("DROP TRIGGER temp.fail_start_commit");
}

registerExecutionClientConformance({
  name: "memory",
  create(options) {
    return { runtime: new MemoryRuntime(options) };
  },
});

registerExecutionClientConformance({
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
      failStartCommits: () => failStartCommits(db),
      dispose() {
        db.close();
      },
    };
  },
});

// Separate connections to one database file, as separate processes would use.
registerExecutionClientConformance({
  name: isBun ? "sqlite-bun-file" : "sqlite-node-file",
  create(options) {
    const dir = mkdtempSync(join(tmpdir(), "yieldstar-executions-"));
    const path = join(dir, "executions.db");
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
      failStartCommits: () => failStartCommits(dbs[0]),
      dispose() {
        for (const db of dbs) db.close();
        rmSync(dir, { recursive: true, force: true });
      },
    };
  },
});

test("a database created before list lists its executions in insertion order", async () => {
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
        parent_step_key TEXT
      );
      INSERT INTO executions (execution_id, workflow_id, params, context, status, admitted_at, updated_at)
      VALUES ('old-1', 'charge-order', '{}', '[]', 'admitted', 20, 20),
             ('old-2', 'charge-order', '{}', '[]', 'admitted', 10, 10);
    `);

    const runtime = new SqliteRuntime(db);
    const peer = new SqliteRuntime(db);
    expect(
      await runtime.executions.admit({
        executionId: "new",
        workflowId: "charge-order",
        params: {},
        context: new Map(),
      })
    ).toEqual({ status: "accepted", executionId: "new" });

    const page = await peer.executions.list({ limit: 2 });
    expect(page.executions.map((record) => record.executionId)).toEqual(["new", "old-2"]);
    const rest = await peer.executions.list({ limit: 2, cursor: page.cursor });
    expect(rest).toMatchObject({ executions: [{ executionId: "old-1" }] });
    expect(rest.cursor).toBeUndefined();
  } finally {
    db.close();
  }
});

test("filtered list queries read the workflow_id and status indexes in rowid order", () => {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  try {
    new SqliteRuntime(db);
    const plan = (where: string, bindings: SqliteParameters) =>
      db
        .query<{ detail: string }>(
          `EXPLAIN QUERY PLAN SELECT *, rowid AS sequence FROM executions
           WHERE ${where} ORDER BY rowid DESC LIMIT 51`
        )
        .all(bindings)
        .map((row) => row.detail);

    expect(
      plan("workflow_id = $workflowId AND rowid < $before", {
        $workflowId: "charge-order",
        $before: 10,
      })
    ).toEqual([
      expect.stringMatching(/USING INDEX idx_executions_workflow_id \(workflow_id=\? AND rowid<\?\)/),
    ]);
    expect(
      plan("status = $status AND (lease_expires_at IS NULL OR lease_expires_at <= $now)", {
        $status: "admitted",
        $now: 0,
      })
    ).toEqual([expect.stringMatching(/USING INDEX idx_executions_status \(status=\?\)/)]);
  } finally {
    db.close();
  }
});
