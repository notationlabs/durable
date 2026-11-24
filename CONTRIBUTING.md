## Contributing to Durable

### Before You Start

- Please open an issue to discuss significant changes before submitting a pull request. This helps align on scope and approach, and avoids wasted effort.

### Pull Requests

- Keep PRs focused and reasonably small; include a clear description and motivation.
- Add tests if you change behavior or fix a bug.
- Update documentation where relevant.
- Ensure `pok typecheck` and `pok test bun` pass locally.

This repository is a TypeScript monorepo using pnpm workspaces and Bun. It contains the core SDK, runtimes, server integrations, examples, and tests.


### Contributor License Agreement (CLA)

By contributing, you affirm that you have the right to submit your contributions and you agree to license them under the project’s license (MIT).


### Repository Layout

Each folder in `packages/` is a private workspace package named `@durable/<folder>`. Packages import each other only through these names, which keeps the boundaries between them. The private packages are not built. Their `exports` point at their TypeScript source.

- `packages/core` – The runtime contracts, the state helpers, `WorkflowRunner` and `ExecutionDispatcher`
- `packages/sdk` – The workflow API, the step runner, and the local and HTTP SDKs
- `packages/http-server` – HTTP route handlers and middleware
- `packages/sqlite-runtime` – `SqliteRuntime`, with the Bun and Node drivers
- `packages/cloudflare` – The SQLite driver for Durable Object storage, and the workflow object that claims on the object's alarm
- `packages/worker-invoker` – The invoker that runs each delivery in a worker process, and the worker
- `packages/runtime-conformance` – Qualification suites for runtimes
- `packages/runtime-load` – The load suite: workflows every host mounts, and a driver that triggers them through an SDK
- `packages/test-runtime` – `MemoryRuntime`, which keeps its records in memory
- `packages/test-invoker` – An invoker that runs deliveries in the current process
- `packages/test-utils` – `createTestSdk`, and the repository's test helpers
- `packages/durable` – The published `@notation/durable` package, bundled from the packages above
- `packages/durable-conformance` – The published `@notation/durable-conformance` package, bundled from `runtime-conformance`
- `examples/` – Workflow and runtime examples (HTTP server, local execution)
- `test/` – Integration tests with Vitest

Workspace configuration is in `pnpm-workspace.yaml`.

### Prerequisites

- pnpm installed
- Bun installed
- Node.js available for tooling where needed

### Install Dependencies

```
pnpm install
```

### Type Checking

```
pok typecheck
```

This runs two TypeScript programs. The first checks the private packages with Node types only, so Bun-only APIs fail outside `sqlite-runtime/src/bun.ts`. The second checks `test/` with Node and Bun types.

### Build

```
pok build
```

This runs the `build` script of each published package, then type-checks `examples/`, which imports the published packages.

Each published package has a `tsup.config.ts` that bundles the source of the private packages it ships:

- `@notation/durable` bundles `sdk`, `core`, `http-server`, `worker-invoker`, `sqlite-runtime`, `cloudflare`, and for `/testing`, `test-utils`, `test-runtime` and `test-invoker`. Each export path is one tsup entry. Code shared between entries goes into shared chunks, so every export path uses the same classes.
- `@notation/durable-conformance` bundles `runtime-conformance`. A `paths` entry in its `tsconfig.json` resolves `@durable/core` to `src/runtime.ts`, which re-exports `@notation/durable/runtime`. The suites then use the classes in the user's copy of `@notation/durable`. This package builds after `@notation/durable`, because its declarations import the built `@notation/durable`.

Both builds fail if bundled code imports a third-party package that the published `package.json` does not declare (`scripts/declared-imports.ts`).

Tests import the private packages. The examples import the published packages. Code that mixes the two loads two copies of the core classes.

### Run Tests

```
pok test bun
pok test conformance
```

`pok test all` runs every suite in sequence.

Local development runs on Bun. Some tests start child processes that import the private packages, and Node cannot resolve the extensionless imports in their source. CI also runs the suite under Node, with `NODE_OPTIONS=--import tsx` so that the suite and its child processes load the source through tsx.

`pok test conformance` builds nothing itself. Run `pok build` first. It packs both published packages, installs them in an empty project and runs every conformance suite under Node against the published SQLite runtime. It covers `@notation/durable/runtime`, `@notation/durable/sqlite/node` and `@notation/durable-conformance` as installed from the tarballs.

The Cloudflare scenarios in `test/cloudflare-e2e` are not part of `pok test`. They run a test Worker on local workerd, or on a deployed Worker, against the built package:

```sh
pok build
cd test/cloudflare-e2e
pnpm exec wrangler dev --port 8799 --var E2E_SECRET:local
E2E_URL=http://localhost:8799 E2E_SECRET=local node --import tsx run.ts
```

The same scenarios run against a deployed Worker. Deploy the test Worker with a secret, pass its URL and the secret to `run.ts`, and delete the Worker afterwards. Every route refuses a request without the secret:

```sh
export CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... E2E_SECRET=$(openssl rand -hex 16)
pnpm exec wrangler deploy --var E2E_SECRET:$E2E_SECRET
export E2E_URL=https://durable-e2e-test.<subdomain>.workers.dev

node --import tsx run.ts                      # the quick set, about 1 minute
node --import tsx run.ts evict-many           # ten idle evictions in parallel, about 62 minutes
node --import tsx run.ts soak                 # one hour; E2E_SOAK_MS=86400000 E2E_SOAK_RATE=0.1 for 24 hours
node --import tsx run.ts spin                 # a CPU-bound step
node --import tsx run.ts alarm-set alarm-fail # how Cloudflare fires and retries alarms, about 12 minutes

pnpm exec wrangler delete
```

`evict-many` runs `evict` on ten objects with delays from 5 to 60 minutes (`E2E_EVICT_DELAYS_MS`). A run fails unless a different instance ran the step after the delay, so it does not pass on local workerd, which keeps the object in memory. `deploy` needs a `wrangler deploy` with a changed `padding.ts` about 20 seconds after it starts. Each `run.ts` process uses a new object unless `E2E_OBJECT` names one, so scenarios can run at the same time in separate processes.

Two more suites run inside workerd with `@cloudflare/vitest-plugin`, on real Durable Object storage and alarms. They need no Cloudflare account and no build, and CI runs them in the `workerd` job. `test/cloudflare-e2e/conformance` runs the runtime conformance suites. `test/cloudflare-e2e/host` runs the workflow object's scenarios; its alarms are the ones workerd fires. `test/cloudflare-e2e` is outside the pnpm workspace, so install it with npm first:

```sh
(cd test/cloudflare-e2e && npm install)
node ./node_modules/vitest/vitest.mjs run --root test/cloudflare-e2e/conformance
node ./node_modules/vitest/vitest.mjs run --root test/cloudflare-e2e/host
```

`test/cloudflare-arm-invariant.test.ts` is part of `pok test`. It runs random sequences of triggers, cancellations, alarms and evictions against the workflow object over the fake storage, and checks that the alarm covers the earliest claim time. A failure prints its seed. `ARM_SEED=<seed>` runs that sequence again, and `ARM_RUNS` sets how many sequences run from that seed.

### Load Suite

The load suite measures how long executions take to end as the trigger rate rises. It reports numbers and is not part of `pok test` or CI. `packages/runtime-load` holds the workflows (`loadRouter`) and the driver (`runLoad`). Each runtime has a caller:

```sh
pok load memory   # the in-memory runtime, in one process
pok load bun      # SQLite on Bun: an HTTP server with worker processes
pok load node     # SQLite on Node: the same server
```

The SQLite callers start the server in a child process, with a new database in a temporary directory, and stop it at the end. The dispatcher uses its default concurrency of 10, and each delivery runs in a forked worker process. `LOAD_CONCURRENCY` changes the concurrency. The caller first bundles the server and the worker with `Bun.build`, for Bun or for Node. Every process then starts from JavaScript, and its start-up time does not include loading TypeScript source.

The Cloudflare caller is the `load` scenario of the e2e runner. It uses its own Durable Object, named by `E2E_LOAD_OBJECT` or `load-<time>`. Against local workerd:

```sh
pok build
cd test/cloudflare-e2e
pnpm exec wrangler dev --port 8799 --var E2E_SECRET:local
E2E_URL=http://localhost:8799 E2E_SECRET=local node --import tsx run.ts load
```

The `E2E_LOAD_*` variables set its options; see below.

The driver runs one stage per rate. In each stage it triggers executions for `stageMs` with exponential gaps between triggers, so the triggers arrive unevenly at the stage's mean rate. Each trigger picks one of these kinds: one step, ten steps, a delay of one second between two steps, a parent that starts two children and joins them, and a step that throws. After the last trigger of a stage, the driver lists executions until every execution of the stage has ended, or until the settle timeout passes. Between listings it waits on the newest open execution. The listing goes newest first and stops at the stage's oldest execution, so it does not read pre-filled executions. The next stage starts after that.

The driver stops the ramp early, and gives the reason in the report, after a stage in which:

- more than a tenth of triggers rejected,
- executions were still open at the settle timeout, or could not be listed until then, or
- lag p50 was above `maxLagMs`, when that option is set. With `waitEvery`, the rule uses waited lag instead.

A trigger that rejects, such as a `503` from the routing Worker, is counted and sampled in the stage's report and is not tried again. A failed `list`, `wait` or probe call is counted and tried again until the settle timeout.

The local callers read these environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOAD_STAGES` | `1,5,20,50` | Trigger rates per second, one stage each |
| `LOAD_STAGE_MS` | `10000` | How long each stage sends triggers |
| `LOAD_SETTLE_MS` | `60000` | How long a stage waits for its executions to end |
| `LOAD_MAX_LAG_MS` | none | Stop after a stage whose lag p50 is above this |
| `LOAD_WAIT_EVERY` | none | Time every nth execution from the client with `wait` |
| `LOAD_PREFILL` | `0` | Single-step executions to run before the first stage |
| `LOAD_PREFILL_CONCURRENCY` | `20` | Pre-fill triggers in flight at once |
| `LOAD_SEED` | the clock | Seeds the gaps and the choice of kind |
| `LOAD_OUT` | `test/load/reports/<runtime>-<run ID>.json` | Where the JSON report goes |

The Cloudflare scenario reads the same options with an `E2E_` prefix: `E2E_LOAD_STAGES`, `E2E_LOAD_STAGE_MS`, `E2E_LOAD_SETTLE_MS`, `E2E_LOAD_MAX_LAG_MS`, `E2E_LOAD_WAIT_EVERY` (default `20`), `E2E_LOAD_PREFILL`, `E2E_LOAD_PREFILL_CONCURRENCY` (default `50`) and `E2E_LOAD_OUT`. `E2E_LOAD_LIST_LIMIT` sets the page size of `list` (default `1000`). Each request sends the `x-e2e-secret` header and has a time limit of 30 seconds.

#### Running the Cloudflare scenario against a deployed Worker

Deploy the test Worker with a secret, as for the other deployed scenarios. Then run the ramp, and in a second run the ramp against an object that already holds 100,000 finished executions:

```sh
cd test/cloudflare-e2e
E2E_URL=https://<worker host> E2E_SECRET=<secret> \
  E2E_LOAD_STAGES=5,20,50,100,200,400 E2E_LOAD_STAGE_MS=60000 E2E_LOAD_MAX_LAG_MS=30000 \
  node --import tsx run.ts load
E2E_URL=https://<worker host> E2E_SECRET=<secret> \
  E2E_LOAD_PREFILL=100000 E2E_LOAD_STAGES=20,100 E2E_LOAD_STAGE_MS=60000 E2E_LOAD_MAX_LAG_MS=30000 \
  node --import tsx run.ts load
```

Each run uses a new object unless `E2E_LOAD_OBJECT` names one. On local workerd, a pre-fill of 100,000 took about six minutes at 50 triggers in flight, and wrote about 1.8 million rows. The object, not the client, limited the rate: 100 triggers in flight were about 15% faster. The first run's stages trigger about 46,000 executions at the planned rates.

#### Reading the report

The callers print three tables and write the full report as JSON.

- The first table has one row per stage. `sent/s` is the rate the driver reached. `trigger p50 / p99` is how long `trigger` took to resolve, measured by the driver. `ended` counts the stage's executions by status, children included. `lag` is `updatedAt - admittedAt` of each execution, less the time its delays take by design, read from the views that `list` returns. `settle ms` is the time from the stage's last trigger until every execution ended.
- With `waitEvery`, the first table also has `waited lag`. The driver times every nth execution from when it sends the trigger until `wait` returns the execution terminal, less the time its delays take by design. It includes the network time of the trigger request and the last `wait` response. `(n)` is the number of executions timed.
- The second table splits lag by kind. `child` is the children that `parent` starts; their lag counts from their own admission.
- The third table shows the caller's probe values before and after each stage. In-memory reports the heap and RSS of the process. SQLite reports the row count of `executions`, the database size and the WAL size. Cloudflare reports the object's database size and the `rowsWritten` of the runtime's SQL cursors, counted by the test Worker. That counter is held in memory, so it starts again from zero when the object restarts. The probe also returns `constructedAt`, the time the object instance was constructed. When it changes during a stage, the report marks the stage `(restarted)`, and the `rowsWritten` figures of that stage do not give the rows the stage wrote.

On Cloudflare, `Date.now()` advances only after I/O, and Durable Object SQL is not I/O. An execution that runs to the end within one request or one alarm therefore has the same `admittedAt` and `updatedAt`, and its lag reads 0. Lag there shows the time an execution waited across requests and alarms, such as the alarm after a delay, and not the time spent running it. For this reason the Cloudflare scenario times every 20th execution from the client, and its `maxLagMs` rule uses that waited lag. The timed executions add `wait` requests to the object's load.

The rate at which lag starts to climb from one stage to the next is the number to watch. With `LOAD_PREFILL`, compare lag against a run without pre-fill to see whether claims and lists slow down as the table grows. Finished executions are never deleted, so the table only grows.

The only check is that every execution ended in the status its kind should end in: `failed` for the throwing kind, `completed` for the others. An execution that is still open after the settle timeout fails the check. The caller then exits with code 1. There are no timing thresholds.

Compare numbers only between runs on the same runtime and machine. The runtimes differ in hardware, worker processes, concurrency and network, so numbers from two runtimes do not rank them. CPU time per execution on Cloudflare cannot be measured inside a Worker; read it from Cloudflare's analytics for the Worker.

### Dev Workflow

Run type checking and tests in watch mode:

```
pok dev
```

Run `pok` with no arguments for the full command menu.

### Documentation

`docs/` holds the Markdown, published at notation.dev/durable. `docs/site.ts`
declares the sidebar.

### Examples

Examples are in `examples/`.

- Interactive import/inspect helper:
  - `pok start` (select a workflow module to import; it does not execute workflows automatically)
- Direct example runs:
  - `bun examples/local-execution/app.ts`
  - `bun examples/http-server/server.ts` and in a second terminal `bun examples/http-server/app.ts`

### Releases

Releases use the pok-plugins `release()` flow configured in `pok.config.ts`.

- Bump, commit, tag and push: `pok version` (`pok version prerelease` for an alpha)
- Package and inspect every publish target without contacting npm: `pok publish --dry-run`
- Release: `pok publish` (installs and rebuilds first)

Every manifest is versioned in lockstep. Only `@notation/durable` and `@notation/durable-conformance` are published.

### Coding Guidelines

- Keep changes minimal and focused on the task.
- Match existing coding style and TypeScript patterns.
- Avoid unrelated refactors in the same change.
