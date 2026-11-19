import type {
  DurableObjectStateLike,
  DurableObjectStorageLike,
} from "@durable/cloudflare";
import type { SqliteParameters } from "@durable/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@durable/sqlite-runtime/bun")
  : await import("@durable/sqlite-runtime/node");

/**
 * A stand-in for a Durable Object's `ctx.storage`, over an in-memory SQLite
 * database. Like the real storage, `sql.exec` binds by position and rejects
 * transaction statements. `alarmAt` is the time the alarm is set to.
 */
export function createFakeStorage() {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  let alarmAt: number | null = null;
  let onAlarmChange = () => {};

  const storage: DurableObjectStorageLike = {
    sql: {
      exec(query: string, ...bindings: unknown[]) {
        if (/^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b/i.test(query)) {
          throw new Error("Durable Object SQL rejects transaction statements");
        }
        if (/\$[A-Za-z_]/.test(query.replace(/'(?:[^']|'')*'/g, ""))) {
          throw new Error("Durable Object SQL does not bind named parameters");
        }
        let rows: unknown[] = [];
        if (bindings.length === 0 && !/^\s*(SELECT|PRAGMA)\b/i.test(query)) {
          db.run(query);
        } else {
          let index = 0;
          const parameters: SqliteParameters = {};
          const named = query.replace(/'(?:[^']|'')*'|\?/g, (token) => {
            if (token !== "?") return token;
            parameters[`$p${index}`] = bindings[index] as never;
            return `$p${index++}`;
          });
          if (index !== bindings.length) {
            throw new Error(`Expected ${index} bindings, got ${bindings.length}`);
          }
          rows = db.query(named).all(index > 0 ? parameters : undefined);
        }
        return { toArray: () => rows };
      },
    },
    transactionSync(closure) {
      db.run("BEGIN IMMEDIATE");
      try {
        const result = closure();
        db.run("COMMIT");
        return result;
      } catch (err) {
        db.run("ROLLBACK");
        throw err;
      }
    },
    async setAlarm(scheduledTime) {
      alarmAt = Number(scheduledTime);
      onAlarmChange();
    },
    async deleteAlarm() {
      alarmAt = null;
      onAlarmChange();
    },
  };

  return {
    storage,
    /** The raw database, for statements the storage API rejects. */
    db,
    get alarmAt() {
      return alarmAt;
    },
    /** Clears the alarm, as Cloudflare does before it calls the handler. */
    takeAlarm() {
      const at = alarmAt;
      alarmAt = null;
      return at;
    },
    set onAlarmChange(fn: () => void) {
      onAlarmChange = fn;
    },
    close() {
      db.close();
    },
  };
}

export type FakeStorage = ReturnType<typeof createFakeStorage>;

/**
 * Stands in for the Cloudflare runtime around one object: it calls `alarm`
 * when the alarm time arrives, one call at a time. `restart` builds the
 * object again over the same storage, as after an eviction.
 */
export function createFakeObject<Host extends { alarm(): Promise<void> }>(
  create: (ctx: DurableObjectStateLike) => Host,
  fake: FakeStorage = createFakeStorage()
) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let running = false;
  let stopped = false;
  const alarms: number[] = [];
  const ctx: DurableObjectStateLike = {
    storage: fake.storage,
    waitUntil() {},
  };
  let host = create(ctx);

  const schedule = () => {
    clearTimeout(timer);
    if (stopped || running || fake.alarmAt === null) return;
    timer = setTimeout(fire, Math.max(0, fake.alarmAt - Date.now()));
  };
  const fire = async () => {
    running = true;
    alarms.push(fake.takeAlarm()!);
    try {
      await host.alarm();
    } finally {
      running = false;
      schedule();
    }
  };
  fake.onAlarmChange = schedule;
  schedule();

  return {
    fake,
    /** The time of each alarm that has fired. */
    alarms,
    get host() {
      return host;
    },
    restart() {
      host = create(ctx);
      schedule();
      return host;
    },
    stop() {
      stopped = true;
      clearTimeout(timer);
    },
  };
}
