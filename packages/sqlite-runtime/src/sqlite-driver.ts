export type SqliteParameterValue =
  | string
  | number
  | bigint
  | boolean
  | Uint8Array
  | null;

export type SqliteParameters = Record<`$${string}`, SqliteParameterValue>;

export type SqliteRunResult = {
  changes: number | bigint;
};

/**
 * Callers must supply exactly the named parameters used by the SQL, including
 * each `$` prefix. Connectors do not normalize additional names because native
 * drivers differ: some ignore them while others throw.
 */
export interface SqliteStatement<Row> {
  get(parameters?: SqliteParameters): Row | undefined;
  all(parameters?: SqliteParameters): Row[];
  run(parameters?: SqliteParameters): SqliteRunResult;
}

export interface SqliteDriver {
  run(sql: string): void;
  query<Row>(sql: string): SqliteStatement<Row>;
  /**
   * Runs `fn` in one write transaction: it commits when `fn` returns and
   * rolls back when `fn` throws. Implement it when the database does not
   * accept `BEGIN` and `COMMIT` as statements, as Durable Object storage does
   * not. When absent, the runtime runs `BEGIN IMMEDIATE`, `COMMIT` and
   * `ROLLBACK` through `run`.
   */
  transaction?<R>(fn: () => R): R;
  close(): void;
}
