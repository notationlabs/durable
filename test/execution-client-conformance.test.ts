import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { registerExecutionClientConformance } from "@yieldstar/runtime-conformance";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";

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

test("a database created before list gives its executions admission sequences in insertion order", async () => {
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
    // A second runtime on the same database finds the column already added.
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

function admissionSequences(db: SqliteDb) {
  return Object.fromEntries(
    db
      .query<{ execution_id: string; admission_sequence: number }>(
        `SELECT execution_id, admission_sequence FROM executions`
      )
      .all()
      .map((row) => [row.execution_id, Number(row.admission_sequence)])
  );
}

async function admitCharge(runtime: SqliteRuntime, executionId: string) {
  expect(
    await runtime.executions.admit({
      executionId,
      workflowId: "charge-order",
      params: {},
      context: new Map(),
    })
  ).toEqual({ status: "accepted", executionId });
}

test("an insert that omits admission_sequence, as a process from before list makes, takes the next sequence", async () => {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  try {
    const runtime = new SqliteRuntime(db);
    await admitCharge(runtime, "new-1");
    db.run(`
      INSERT INTO executions (execution_id, workflow_id, params, context, status, admitted_at, updated_at)
      VALUES ('old-code', 'charge-order', '{}', '[]', 'admitted', 0, 0);
    `);
    await admitCharge(runtime, "new-2");

    expect(admissionSequences(db)).toEqual({ "new-1": 1, "old-code": 2, "new-2": 3 });
    const page = await runtime.executions.list();
    expect(page.executions.map((record) => record.executionId)).toEqual([
      "new-2",
      "old-code",
      "new-1",
    ]);
  } finally {
    db.close();
  }
});

test("a deleted execution's admission sequence is not reissued", async () => {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  try {
    const runtime = new SqliteRuntime(db);
    await admitCharge(runtime, "first");
    await admitCharge(runtime, "newest");
    db.run(`DELETE FROM executions WHERE execution_id = 'newest'`);
    // A runtime opened after the delete continues from the counter, not the remaining rows.
    const reopened = new SqliteRuntime(db);
    await admitCharge(reopened, "after-delete");

    expect(admissionSequences(db)).toEqual({ first: 1, "after-delete": 3 });
  } finally {
    db.close();
  }
});
