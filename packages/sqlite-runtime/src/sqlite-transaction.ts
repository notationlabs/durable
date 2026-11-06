import type { SqliteDriver } from "./sqlite-driver";

/**
 * Runs `fn` in one `BEGIN IMMEDIATE … COMMIT` transaction, rolling back if it
 * throws. `fn` is synchronous so no other operation on this connection can
 * interleave, and IMMEDIATE takes the write lock up front so concurrent
 * processes sharing the database file serialize on it.
 */
export function runTransaction<R>(db: SqliteDriver, fn: () => R): R {
  db.run("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.run("COMMIT");
    return result;
  } catch (err) {
    db.run("ROLLBACK");
    throw err;
  }
}

/**
 * Adds the columns of `table` that are missing, under the write lock so
 * processes opening the file together add each one once. Each column is
 * given as its SQL definition, such as `step_kind TEXT`.
 */
export function addMissingColumns(db: SqliteDriver, table: string, columns: readonly string[]) {
  const missing = () => {
    const existing = new Set(
      db
        .query<{ name: string }>(`PRAGMA table_info(${table})`)
        .all()
        .map((column) => column.name)
    );
    return columns.filter((column) => !existing.has(column.split(" ")[0]!));
  };
  if (missing().length === 0) return;
  runTransaction(db, () => {
    for (const column of missing()) {
      db.run(`ALTER TABLE ${table} ADD COLUMN ${column}`);
    }
  });
}
