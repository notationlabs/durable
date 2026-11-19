import type {
  DurableObjectStateLike,
  DurableObjectStorageLike,
} from "@durable/cloudflare";
import type { SqliteParameters } from "@durable/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@durable/sqlite-runtime/bun")
  : await import("@durable/sqlite-runtime/node");

/** The retries Cloudflare gives an alarm handler that throws. */
const ALARM_RETRIES = 6;

/**
 * A stand-in for a Durable Object's `ctx.storage`, over an in-memory SQLite
 * database. Like the real storage, `sql.exec` binds by position and rejects
 * transaction statements. `alarmAt` is the time the alarm is set to.
 *
 * The alarm follows what a deployed Worker did when measured:
 *
 * - `setAlarm` with a time behind the clock sets the alarm to the clock.
 * - During the alarm handler the alarm reads as not set, until the handler
 *   sets one.
 * - An alarm that the handler sets to the handler's own scheduled time is
 *   `slow`: it fired 3 to 60 seconds late, not at once. Any other time,
 *   one millisecond later included, fired on time.
 * - The clock in a Worker does not move until the handler waits for I/O, and
 *   in most runs the handler started with the clock at the scheduled time. So
 *   until the handler has waited for a timer, a time at or behind the clock
 *   sets the alarm to the scheduled time.
 */
export function createFakeStorage() {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  let alarmAt: number | null = null;
  let slow = false;
  let handler: { scheduled: number; clockMoved: boolean } | undefined;
  let onAlarmChange = () => {};
  let fault: Error | undefined;

  const storage: DurableObjectStorageLike = {
    sql: {
      exec(query: string, ...bindings: unknown[]) {
        if (fault) throw fault;
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
      if (fault) throw fault;
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
      const at = Number(scheduledTime);
      if (at > Date.now()) alarmAt = at;
      else alarmAt = handler && !handler.clockMoved ? handler.scheduled : Date.now();
      slow = alarmAt === handler?.scheduled;
      onAlarmChange();
    },
    async deleteAlarm() {
      alarmAt = null;
      slow = false;
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
    /** Whether the alarm set is one that Cloudflare fires late. */
    get slow() {
      return slow;
    },
    /** Clears the alarm, as Cloudflare does before it calls the handler. */
    takeAlarm() {
      const at = alarmAt;
      alarmAt = null;
      slow = false;
      return at;
    },
    /**
     * Starts the handler's turn, for the alarm that is set or for a retry of
     * the alarm scheduled at `retryOf`. `endAlarm` ends the turn.
     */
    beginAlarm(retryOf?: number) {
      const scheduled = retryOf ?? this.takeAlarm()!;
      const turn = { scheduled, clockMoved: false };
      handler = turn;
      setTimeout(() => {
        turn.clockMoved = true;
      }, 0);
      return scheduled;
    },
    endAlarm() {
      handler = undefined;
    },
    /** While set, every SQL statement and transaction throws it. */
    set fault(err: Error | undefined) {
      fault = err;
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
 *
 * When `alarm` rejects and has not set an alarm, it runs again after
 * `retryDelay`, and each later retry waits twice as long. After six retries
 * the alarm is dropped. When `alarm` rejects after setting an alarm, that
 * alarm fires as set, and the retries start from none.
 */
export function createFakeObject<Host extends { alarm(): Promise<void> }>(
  create: (ctx: DurableObjectStateLike) => Host,
  fake: FakeStorage = createFakeStorage(),
  options: {
    /** The wait before the first retry, in milliseconds. @default 2000, as on Cloudflare */
    retryDelay?: number;
    /** How late a slow alarm fires, in milliseconds. @default 60_000, the longest measured */
    slowAlarmDelay?: number;
  } = {}
) {
  const { retryDelay = 2_000, slowAlarmDelay = 60_000 } = options;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let running = false;
  let stopped = false;
  let retry: { scheduled: number; count: number } | undefined;
  const alarms: number[] = [];
  const failures: unknown[] = [];
  let dropped = 0;
  const ctx: DurableObjectStateLike = {
    storage: fake.storage,
    waitUntil() {},
  };
  let host = create(ctx);

  const schedule = () => {
    clearTimeout(timer);
    if (stopped || running) return;
    if (fake.alarmAt !== null) {
      retry = undefined;
      const late = fake.slow ? slowAlarmDelay : 0;
      timer = setTimeout(fire, Math.max(0, fake.alarmAt - Date.now()) + late);
    } else if (retry) {
      timer = setTimeout(fire, retryDelay * 2 ** (retry.count - 1));
    }
  };
  const fire = async () => {
    running = true;
    const scheduled = fake.beginAlarm(fake.alarmAt === null ? retry?.scheduled : undefined);
    alarms.push(scheduled);
    try {
      await host.alarm();
      retry = undefined;
    } catch (err) {
      failures.push(err);
      const count = (retry?.count ?? 0) + 1;
      retry = count > ALARM_RETRIES ? undefined : { scheduled, count };
      if (!retry && fake.alarmAt === null) dropped++;
    } finally {
      fake.endAlarm();
      running = false;
      schedule();
    }
  };
  fake.onAlarmChange = schedule;
  schedule();

  return {
    fake,
    /** The scheduled time of each call to `alarm`, retries included. */
    alarms,
    /** What each failed call to `alarm` rejected with. */
    failures,
    /** The number of alarms dropped after their last retry failed. */
    get dropped() {
      return dropped;
    },
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
