import type { SqliteDriver } from "@durable/sqlite-runtime";

/** Opens the database at `LOAD_DB` with the SQLite driver of the runtime this process runs on. */
export async function openDb(): Promise<SqliteDriver> {
  const path = process.env.LOAD_DB;
  if (!path) throw new Error("LOAD_DB is not set");
  const { createSqliteDb } = process.versions.bun
    ? await import("@durable/sqlite-runtime/bun")
    : await import("@durable/sqlite-runtime/node");
  return createSqliteDb({ path });
}
