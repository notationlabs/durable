import { SqliteRuntime } from "@durable/sqlite-runtime";

const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@durable/sqlite-runtime/bun")
    : await import("@durable/sqlite-runtime/node");

const db = createSqliteDb({ path: process.argv[2] });
new SqliteRuntime(db);
db.close();
