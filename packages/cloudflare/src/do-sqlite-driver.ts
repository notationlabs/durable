import type {
  SqliteDriver,
  SqliteParameters,
  SqliteStatement,
} from "@durable/sqlite-runtime";
import type { DurableObjectStorageLike } from "./storage";

type ParameterName = `$${string}`;

/**
 * Replaces each `$name` parameter with `?`, because Durable Object SQL only
 * binds by position. `names` lists the parameters in the order they appear,
 * with a name repeated each time the SQL repeats it. Quoted literals and
 * identifiers are copied unchanged.
 */
export function toPositionalSql(sql: string): {
  sql: string;
  names: ParameterName[];
} {
  const names: ParameterName[] = [];
  const rewritten = sql.replace(
    /'(?:[^']|'')*'|"(?:[^"]|"")*"|\$[A-Za-z_][A-Za-z0-9_]*/g,
    (token) => {
      if (!token.startsWith("$")) return token;
      names.push(token as ParameterName);
      return "?";
    }
  );
  return { sql: rewritten, names };
}

function bind(names: readonly ParameterName[], parameters?: SqliteParameters) {
  return names.map((name) => {
    if (!parameters || !(name in parameters)) {
      throw new TypeError(`Missing SQL parameter ${name}`);
    }
    const value = parameters[name];
    return typeof value === "boolean" ? Number(value) : value;
  });
}

/** Runs the SQLite runtime on a Durable Object's SQLite storage. */
export class DurableObjectSqliteDriver implements SqliteDriver {
  /**
   * Called after each transaction commits, and after each write made outside
   * a transaction. It runs before the caller's next `await`.
   */
  onCommit: (() => void) | undefined;
  private depth = 0;

  constructor(private readonly storage: DurableObjectStorageLike) {}

  run(sql: string) {
    this.storage.sql.exec(sql).toArray();
    this.committed();
  }

  query<Row>(sql: string): SqliteStatement<Row> {
    const { sql: positional, names } = toPositionalSql(sql);
    const { storage } = this;
    // A cursor reads rows as it is iterated, so each call reads it to the end
    // before returning.
    const all = (parameters?: SqliteParameters) =>
      storage.sql.exec(positional, ...bind(names, parameters)).toArray() as Row[];
    return {
      get: (parameters) => all(parameters)[0],
      all,
      run: (parameters) => {
        all(parameters);
        // The cursor's `rowsWritten` also counts index rows, so the number of
        // changed rows comes from SQLite.
        const [row] = storage.sql.exec("SELECT changes() AS changes").toArray();
        this.committed();
        return { changes: Number(row.changes) };
      },
    };
  }

  // Durable Object SQL rejects BEGIN and COMMIT as statements.
  transaction<R>(fn: () => R): R {
    this.depth++;
    let result: R;
    try {
      result = this.storage.transactionSync(fn);
    } finally {
      this.depth--;
    }
    this.committed();
    return result;
  }

  // The object owns its storage, so there is nothing to close.
  close() {}

  private committed() {
    if (this.depth === 0) this.onCommit?.();
  }
}

export function createSqliteDb(storage: DurableObjectStorageLike) {
  return new DurableObjectSqliteDriver(storage);
}
