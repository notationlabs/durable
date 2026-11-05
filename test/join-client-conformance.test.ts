import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registerJoinClientConformance } from "@yieldstar/runtime-conformance";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

type SqliteDb = ReturnType<typeof createSqliteDb>;

// The trigger aborts any update of the execution's row, so a terminal
// acknowledgement that wakes it as a joiner fails before it commits.
function failExecutionWrites(db: SqliteDb, executionId: string) {
  db.run(`
    CREATE TEMP TRIGGER fail_execution_write BEFORE UPDATE ON executions
    WHEN OLD.execution_id = '${executionId.replaceAll("'", "''")}'
    BEGIN SELECT RAISE(ABORT, 'injected execution write failure'); END
  `);
  return () => db.run("DROP TRIGGER temp.fail_execution_write");
}

registerJoinClientConformance({
  name: "memory",
  create(options) {
    return { runtime: new MemoryRuntime(options) };
  },
});

registerJoinClientConformance({
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
registerJoinClientConformance({
  name: isBun ? "sqlite-bun-file" : "sqlite-node-file",
  create(options) {
    const dir = mkdtempSync(join(tmpdir(), "yieldstar-joins-"));
    const path = join(dir, "joins.db");
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
