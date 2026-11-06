import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { registerReplayStabilityClientConformance } from "@yieldstar/runtime-conformance";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

registerReplayStabilityClientConformance({
  name: "memory",
  create(options) {
    return { runtime: new MemoryRuntime(options) };
  },
});

registerReplayStabilityClientConformance({
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
      dispose() {
        db.close();
      },
    };
  },
});

// Separate connections to one database file, as separate processes would use.
registerReplayStabilityClientConformance({
  name: isBun ? "sqlite-bun-file" : "sqlite-node-file",
  create(options) {
    const dir = mkdtempSync(join(tmpdir(), "yieldstar-replay-stability-"));
    const path = join(dir, "replay-stability.db");
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
      dispose() {
        for (const db of dbs) db.close();
        rmSync(dir, { recursive: true, force: true });
      },
    };
  },
});

test("a database created before replay stability reads its rows with no version, no divergence and no step kind", async () => {
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
        parent_detached INTEGER NOT NULL DEFAULT 0,
        cancellation_reason TEXT,
        cancellation_source TEXT,
        cancellation_forced INTEGER,
        cancellation_requested_at INTEGER,
        cancellation_delivered_at INTEGER
      );
      CREATE TABLE step_responses (
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        step_attempt INTEGER NOT NULL,
        step_done BOOL NOT NULL,
        step_response TEXT NOT NULL,
        PRIMARY KEY (execution_id, step_key, step_attempt)
      );
      INSERT INTO executions (execution_id, workflow_id, params, context, status, admitted_at, updated_at, due_at, next_claim_at)
      VALUES ('order-42', 'place-order', '{}', '[]', 'suspended', 10, 10, 10, 10);
      INSERT INTO step_responses (execution_id, step_key, step_attempt, step_done, step_response)
      VALUES ('order-42', 'reserve-stock', 1, 1, '{}');
    `);

    const runtime = new SqliteRuntime(db);
    // A second runtime on the same database finds the columns already added.
    const peer = new SqliteRuntime(db);
    const record = await peer.executions.get("order-42");
    expect(record).not.toHaveProperty("workflowVersion");
    expect(record).not.toHaveProperty("divergence");
    expect((await peer.heap.readHistory({ executionId: "order-42" })).stepKeys).toEqual(["reserve-stock"]);
    expect(
      (await peer.heap.readStep({ executionId: "order-42", stepKey: "reserve-stock" }))!.meta
    ).toEqual({ attempt: 1, done: true });
    expect(await peer.executions.list({ diverged: false })).toMatchObject({
      executions: [{ executionId: "order-42" }],
    });

    // The old row takes a version, a kind and a divergence once delivered.
    const [lease] = await runtime.executions.claimDue({ limit: 1, duration: 60_000 });
    await runtime.heap.writeStep({
      lease,
      stepKey: "charge",
      stepAttempt: 1,
      done: true,
      stepResponseJson: "{}",
      stepKind: "run",
      workflowVersion: 2,
    });
    expect(
      await runtime.executions.acknowledge({
        lease,
        outcome: {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "ship",
          message: 'Step "ship" would run, but replay did not reach recorded step "charge"',
        },
      })
    ).toEqual({ status: "diverged" });
    expect(await peer.executions.get("order-42")).toMatchObject({
      workflowVersion: 2,
      divergence: {
        code: "NONDETERMINISTIC_REPLAY",
        stepKey: "ship",
        message: 'Step "ship" would run, but replay did not reach recorded step "charge"',
      },
    });
    expect(((await peer.heap.readHistory({ executionId: "order-42" })).stepKeys).sort()).toEqual([
      "charge",
      "reserve-stock",
    ]);
    expect(
      (await peer.heap.readStep({ executionId: "order-42", stepKey: "charge" }))!.meta.kind
    ).toBe("run");
  } finally {
    db.close();
  }
});
