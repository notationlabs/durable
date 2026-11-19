import { createHash } from "crypto";
import { EventEmitter } from "node:events";
import { createWorkflowObject } from "../../packages/durable/dist/cloudflare.js";
import { padding } from "./padding";
import { live, router, useSql } from "./workflows";

type Storage = {
  sql: { exec(query: string, ...bindings: unknown[]): { toArray(): any[]; rowsWritten?: number } };
  transactionSync<R>(closure: () => R): R;
  setAlarm(at: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
  getAlarm(): Promise<number | null>;
  sync(): Promise<void>;
};
type AlarmInfo = { retryCount: number; isRetry: boolean };
type AlarmProbe = { case: string; t: number; peek: boolean; hold: number; n: number };
type State = {
  storage: Storage;
  waitUntil(promise: Promise<unknown>): void;
  abort(reason?: string): void;
};
type Stub = { fetch(request: Request | string, init?: RequestInit): Promise<Response> };
type Namespace = { idFromName(name: string): unknown; get(id: unknown): Stub };
type Env = { WORKFLOWS: Namespace; PROBE: Namespace; E2E_SECRET?: string };

// Random values are not allowed in global scope, so the ID is made on first use.
let isolate: string | undefined;
const isolateId = () => (isolate ??= crypto.randomUUID().slice(0, 8));
const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });

function log(storage: Storage, instance: string, event: string, detail?: unknown) {
  storage.sql.exec(
    "INSERT INTO e2e_log (at, isolate, instance, event, detail) VALUES (?, ?, ?, ?, ?)",
    Date.now(),
    isolateId(),
    instance,
    event,
    detail === undefined ? null : JSON.stringify(detail)
  );
}

function captureStack(): string[] {
  const holder: { stack?: string } = {};
  Error.captureStackTrace(holder, captureStack);
  return (holder.stack ?? "").split("\n");
}

export class Workflows {
  private durable;
  private instance = crypto.randomUUID().slice(0, 8);
  private alarms = 0;

  constructor(private ctx: State) {
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS e2e_log (id INTEGER PRIMARY KEY, at INTEGER, isolate TEXT, instance TEXT, event TEXT, detail TEXT)"
    );
    useSql(ctx.storage.sql);
    log(ctx.storage, this.instance, "constructed");
    this.durable = createWorkflowObject(ctx, { router, basePath: "/durable" });
  }

  async fetch(request: Request) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/durable/")) return this.durable.fetch(request);
    const { storage } = this.ctx;
    switch (url.pathname) {
      case "/debug/state":
        return json({
          isolateId: isolateId(),
          instance: this.instance,
          padding,
          now: Date.now(),
          alarm: await storage.getAlarm(),
          nextClaimAt: this.durable.runtime.executions.nextClaimAt(),
          live,
        });
      case "/debug/sql": {
        const { query, bindings = [] } = (await request.json()) as {
          query: string;
          bindings?: unknown[];
        };
        try {
          return json(storage.sql.exec(query, ...bindings).toArray());
        } catch (err) {
          return json({ error: String(err) }, 400);
        }
      }
      case "/debug/log":
        return json(
          storage.sql
            .exec("SELECT * FROM e2e_log WHERE id > ? ORDER BY id", Number(url.searchParams.get("after") ?? 0))
            .toArray()
        );
      case "/debug/reset-live":
        live.active = 0;
        live.maxActive = 0;
        return json(live);
      case "/debug/abort":
        log(storage, this.instance, "abort");
        this.ctx.abort("e2e abort");
        return json({ aborted: true });
    }
    return new Response("Not found", { status: 404 });
  }

  async alarm() {
    const n = ++this.alarms;
    log(this.ctx.storage, this.instance, "alarm-start", { n });
    try {
      await this.durable.alarm();
    } finally {
      log(this.ctx.storage, this.instance, "alarm-end", {
        n,
        alarm: await this.ctx.storage.getAlarm(),
      });
    }
  }
}

function attempt(fn: () => unknown) {
  try {
    return { ok: true, value: fn() };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

export class Probe {
  private instance = crypto.randomUUID().slice(0, 8);

  constructor(private ctx: State) {
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS e2e_log (id INTEGER PRIMARY KEY, at INTEGER, isolate TEXT, instance TEXT, event TEXT, detail TEXT)"
    );
  }

  async fetch(request: Request) {
    const url = new URL(request.url);
    const { storage } = this.ctx;
    const sql = storage.sql;
    switch (url.pathname) {
      case "/probe/sql": {
        sql.exec("DROP TABLE IF EXISTS p; DROP TABLE IF EXISTS q");
        const r: Record<string, unknown> = {};
        r.multiStatement = attempt(() =>
          sql.exec(
            "CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT, n INTEGER DEFAULT 0); CREATE INDEX p_v ON p (v); CREATE TABLE q (k TEXT PRIMARY KEY, n INTEGER)"
          ).toArray()
        );
        r.namedDollar = attempt(() => sql.exec("SELECT $a AS a", 1).toArray());
        r.namedDollarObject = attempt(() => sql.exec("SELECT $a AS a", { $a: 1 }).toArray());
        r.namedColon = attempt(() => sql.exec("SELECT :a AS a", 1).toArray());
        r.numbered = attempt(() => sql.exec("SELECT ?1 AS a, ?1 AS b", 7).toArray());
        r.positional = attempt(() => sql.exec("SELECT ? AS a, ? AS b", 1, "x").toArray());
        r.booleanBinding = attempt(() => sql.exec("SELECT ? AS a", true).toArray());
        r.undefinedBinding = attempt(() => sql.exec("SELECT ? AS a", undefined).toArray());
        r.nullBinding = attempt(() => sql.exec("SELECT ? AS a", null).toArray());
        r.insert3 = attempt(() => {
          const c = sql.exec("INSERT INTO p (v) VALUES ('a'), ('b'), ('c')");
          c.toArray();
          return { rowsWritten: c.rowsWritten, changes: sql.exec("SELECT changes() AS changes").toArray() };
        });
        r.updateNone = attempt(() => {
          sql.exec("UPDATE p SET n = 1 WHERE v = 'zzz'").toArray();
          return sql.exec("SELECT changes() AS changes").toArray();
        });
        r.updateTwo = attempt(() => {
          const c = sql.exec("UPDATE p SET v = v || '!' WHERE v IN ('a', 'b')");
          c.toArray();
          return { rowsWritten: c.rowsWritten, changes: sql.exec("SELECT changes() AS changes").toArray() };
        });
        r.changesInTransaction = attempt(() =>
          storage.transactionSync(() => {
            sql.exec("UPDATE p SET n = n + 1").toArray();
            const a = sql.exec("SELECT changes() AS changes").toArray();
            sql.exec("DELETE FROM p WHERE v = 'c'").toArray();
            const b = sql.exec("SELECT changes() AS changes").toArray();
            return { afterUpdateAll: a, afterDeleteOne: b };
          })
        );
        r.changesAfterSelect = attempt(() => {
          sql.exec("UPDATE p SET n = 5").toArray();
          sql.exec("SELECT * FROM p").toArray();
          return sql.exec("SELECT changes() AS changes").toArray();
        });
        r.onConflict = attempt(() => {
          sql.exec("INSERT INTO q (k, n) VALUES ('x', 1) ON CONFLICT (k) DO UPDATE SET n = n + 1").toArray();
          sql.exec("INSERT INTO q (k, n) VALUES ('x', 1) ON CONFLICT (k) DO UPDATE SET n = n + 1").toArray();
          const d = sql.exec("INSERT INTO q (k, n) VALUES ('x', 1) ON CONFLICT DO NOTHING");
          d.toArray();
          return {
            row: sql.exec("SELECT * FROM q").toArray(),
            changesAfterDoNothing: sql.exec("SELECT changes() AS changes").toArray(),
          };
        });
        r.returning = attempt(() => sql.exec("INSERT INTO q (k, n) VALUES ('y', 9) RETURNING k, n").toArray());
        r.tableInfo = attempt(() => sql.exec("PRAGMA table_info(p)").toArray());
        r.tableInfoFn = attempt(() => sql.exec("SELECT name FROM pragma_table_info('p')").toArray());
        r.alterAddColumn = attempt(() => sql.exec("ALTER TABLE p ADD COLUMN extra TEXT").toArray());
        r.jsonEach = attempt(() =>
          sql.exec("SELECT value FROM json_each(?)", JSON.stringify(["a", "b", 3])).toArray()
        );
        r.jsonExtract = attempt(() => sql.exec("SELECT json_extract(?, '$.a.b') AS v", '{"a":{"b":5}}').toArray());
        r.begin = attempt(() => sql.exec("BEGIN").toArray());
        r.beginImmediate = attempt(() => sql.exec("BEGIN IMMEDIATE").toArray());
        r.savepoint = attempt(() => sql.exec("SAVEPOINT s1").toArray());
        r.commit = attempt(() => sql.exec("COMMIT").toArray());
        r.rollbackOnThrow = attempt(() => {
          const before = sql.exec("SELECT count(*) AS c FROM q").toArray()[0].c;
          let thrown: string | undefined;
          try {
            storage.transactionSync(() => {
              sql.exec("INSERT INTO q (k, n) VALUES ('rollback-me', 1)").toArray();
              throw new Error("closure threw");
            });
          } catch (err) {
            thrown = String(err);
          }
          const after = sql.exec("SELECT count(*) AS c FROM q").toArray()[0].c;
          return { before, after, thrown };
        });
        r.nestedTransactionSync = attempt(() =>
          storage.transactionSync(() => {
            let inner: string | undefined;
            try {
              storage.transactionSync(() => {
                sql.exec("INSERT INTO q (k, n) VALUES ('nested', 1)").toArray();
                throw new Error("inner threw");
              });
            } catch (err) {
              inner = String(err);
            }
            return { inner, nestedRows: sql.exec("SELECT count(*) AS c FROM q WHERE k = 'nested'").toArray() };
          })
        );
        r.sqliteVersion = attempt(() => sql.exec("SELECT sqlite_version() AS v").toArray());
        r.bigint = attempt(() => sql.exec("SELECT 9007199254740993 AS big, typeof(9007199254740993) AS t").toArray());
        r.blob = attempt(() => {
          const out = sql.exec("SELECT ? AS b", new Uint8Array([1, 2, 3])).toArray();
          return { type: Object.prototype.toString.call(out[0].b) };
        });
        r.lastInsertRowid = attempt(() => sql.exec("SELECT last_insert_rowid() AS id").toArray());
        return json(r);
      }
      case "/probe/node": {
        const emitter = new EventEmitter();
        let got: unknown;
        emitter.on("x", (v) => (got = v));
        emitter.emit("x", 42);
        function stepLike() {
          return captureStack();
        }
        const lines = [stepLike(), stepLike()];
        return json({
          sha1: createHash("sha1").update("abc").digest("hex"),
          eventEmitter: got,
          stacks: lines,
        });
      }
      case "/probe/alarm-write": {
        // Write, then set the alarm without awaiting it, in the same turn.
        sql.exec("CREATE TABLE IF NOT EXISTS w (at INTEGER)");
        sql.exec("INSERT INTO w (at) VALUES (?)", Date.now());
        const at = Number(url.searchParams.get("at") ?? Date.now() + 60_000);
        void storage.setAlarm(at);
        if (url.searchParams.has("abort")) this.ctx.abort("after write and setAlarm");
        return json({ setTo: at });
      }
      case "/probe/alarm-past": {
        log(storage, this.instance, "set-past");
        await storage.setAlarm(Date.now() - 5_000);
        return json({ alarm: await storage.getAlarm() });
      }
      case "/probe/alarm-overlap": {
        // The alarm handler holds for `hold` ms and re-arms itself at once.
        sql.exec("CREATE TABLE IF NOT EXISTS cfg (k TEXT PRIMARY KEY, v TEXT)");
        sql.exec(
          "INSERT INTO cfg (k, v) VALUES ('overlap', ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
          url.searchParams.get("rounds") ?? "3"
        );
        await storage.setAlarm(Date.now());
        return json({ ok: true });
      }
      case "/probe/alarm-case": {
        // Sets the alarm `delay` ms ahead. The alarm handler then does what
        // `case` names on its first run, and nothing on later runs.
        sql.exec("CREATE TABLE IF NOT EXISTS cfg (k TEXT PRIMARY KEY, v TEXT)");
        const probe: AlarmProbe = {
          case: url.searchParams.get("case") ?? "untouched",
          t: Date.now() + Number(url.searchParams.get("delay") ?? 2000),
          peek: url.searchParams.has("peek"),
          hold: Number(url.searchParams.get("hold") ?? 0),
          n: 0,
        };
        sql.exec(
          "INSERT INTO cfg (k, v) VALUES ('probe', ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
          JSON.stringify(probe)
        );
        log(storage, this.instance, "probe-armed", probe);
        await storage.setAlarm(probe.t);
        return json(probe);
      }
      case "/probe/state":
        return json({
          now: Date.now(),
          isolateId: isolateId(),
          instance: this.instance,
          alarm: await storage.getAlarm(),
          log: sql.exec("SELECT * FROM e2e_log ORDER BY id").toArray(),
          w: attempt(() => sql.exec("SELECT * FROM w").toArray()),
        });
      case "/probe/clear":
        sql.exec("DELETE FROM e2e_log");
        await storage.deleteAlarm();
        return json({ ok: true });
    }
    return new Response("Not found", { status: 404 });
  }

  // Returns false when no alarm probe is configured.
  private async runAlarmProbe(info: AlarmInfo | undefined): Promise<boolean> {
    const { storage } = this.ctx;
    const sql = storage.sql;
    const now = Date.now();
    let probe: AlarmProbe | undefined;
    try {
      const row = sql.exec("SELECT v FROM cfg WHERE k = 'probe'").toArray()[0];
      probe = row && JSON.parse(row.v);
    } catch {}
    if (!probe) return false;
    const n = ++probe.n;
    sql.exec("UPDATE cfg SET v = ? WHERE k = 'probe'", JSON.stringify(probe));
    const { t } = probe;
    const detail: Record<string, unknown> = {
      case: probe.case,
      n,
      t,
      now,
      retryCount: info?.retryCount,
      isRetry: info?.isRetry,
    };
    if (probe.peek) detail.before = await storage.getAlarm();
    const fail = async () => {
      log(storage, this.instance, "probe-run", detail);
      await storage.sync();
      throw new Error(`alarm probe ${probe.case} run ${n}`);
    };
    switch (probe.case) {
      case "fail":
        return fail();
      case "fail-once":
        if (n === 1) return fail();
        break;
      case "fail-set":
        if (n <= 8) {
          detail.setTo = now + 5000;
          await storage.setAlarm(now + 5000);
          return fail();
        }
        break;
    }
    if (n === 1) {
      const set = async (at: number) => {
        detail.setTo = at;
        await storage.setAlarm(at);
      };
      switch (probe.case) {
        case "same":
          await set(t);
          break;
        case "same-unawaited":
          // As the workflow object does it: not awaited, with writes after it.
          detail.setTo = t;
          void storage.setAlarm(t);
          storage.transactionSync(() => log(storage, this.instance, "probe-write"));
          break;
        case "same-plus-1":
          await set(t + 1);
          break;
        case "max-t-now":
          await set(Math.max(t, Date.now()));
          break;
        case "now":
          await set(Date.now());
          break;
        case "now-plus-1000":
          await set(Date.now() + 1000);
          break;
        case "past":
          await set(Date.now() - 5000);
          break;
        case "writes-only":
          storage.transactionSync(() => {
            log(storage, this.instance, "probe-write");
            log(storage, this.instance, "probe-write");
          });
          await new Promise((resolve) => setTimeout(resolve, 50));
          log(storage, this.instance, "probe-write");
          break;
        case "same-then-later":
          await set(t);
          await set(Date.now() + 2000);
          break;
      }
      if (probe.peek) detail.after = await storage.getAlarm();
      if (probe.hold) await new Promise((resolve) => setTimeout(resolve, probe.hold));
      detail.end = Date.now();
    }
    log(storage, this.instance, "probe-run", detail);
    return true;
  }

  async alarm(info?: AlarmInfo) {
    if (await this.runAlarmProbe(info)) return;
    const { storage } = this.ctx;
    const sql = storage.sql;
    log(storage, this.instance, "alarm-start");
    const cfg = (() => {
      try {
        return sql.exec("SELECT v FROM cfg WHERE k = 'overlap'").toArray()[0]?.v;
      } catch {
        return undefined;
      }
    })();
    const rounds = Number(cfg ?? 0);
    if (rounds > 0) {
      sql.exec("UPDATE cfg SET v = ? WHERE k = 'overlap'", String(rounds - 1));
      // Set a new alarm in the past while this handler is still running.
      await storage.setAlarm(Date.now() - 1);
      log(storage, this.instance, "alarm-rearmed", { alarm: await storage.getAlarm() });
      await new Promise((resolve) => setTimeout(resolve, 3000));
      await storage.deleteAlarm();
      log(storage, this.instance, "alarm-deleted", { alarm: await storage.getAlarm() });
      await storage.setAlarm(Date.now() + 100);
    }
    log(storage, this.instance, "alarm-end", { alarm: await storage.getAlarm() });
  }
}

export default {
  async fetch(request: Request, env: Env) {
    // Fails closed: without the secret configured, every request is refused.
    if (!env.E2E_SECRET || request.headers.get("x-e2e-secret") !== env.E2E_SECRET) {
      return new Response("Forbidden", { status: 403 });
    }
    const url = new URL(request.url);
    const name = url.searchParams.get("object") ?? "default";
    if (url.pathname.startsWith("/probe/")) {
      return env.PROBE.get(env.PROBE.idFromName(name)).fetch(request);
    }
    return env.WORKFLOWS.get(env.WORKFLOWS.idFromName(name)).fetch(request);
  },
};
