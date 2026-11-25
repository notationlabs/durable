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
- `packages/cloudflare` – The SQLite driver for Durable Object storage, the workflow object that claims on the object's alarm, and the remote runtime that a delivery uses when it runs in another instance
- `packages/worker-invoker` – The invoker that runs each delivery in a worker process, and the worker
- `packages/runtime-conformance` – Qualification suites for runtimes
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
node --import tsx run.ts spin-neighbours      # a CPU-bound step next to steps that wait, about 4 minutes
node --import tsx run.ts spin-steps           # steps that together pass the CPU limit
node --import tsx run.ts force-cancel         # a forced cancellation while a step waits
node --import tsx run.ts step-cost where      # the time between two steps, and where a step runs
node --import tsx run.ts alarm-set alarm-fail # how Cloudflare fires and retries alarms, about 12 minutes

pnpm exec wrangler delete
```

`E2E_MODE` picks the class that a run uses, so one deployment compares them:

| `E2E_MODE` | Deliveries run | `maxExpiredDeliveries` |
| :--- | :--- | :--- |
| `isolated` (default) | In other instances of the class | none |
| `inline` | In the object that claims them (`isolateDeliveries: false`) | none |
| `isolated-limit` | In other instances of the class | 3 |
| `inline-limit` | In the object that claims them | 3 |

A step function may run in another instance than the object that admitted its execution. So the test workflows send their counters and log rows to that object, which the trigger middleware records in the execution's context, and the debug routes read them there.

`spin-neighbours` is the comparison of the two ways to run deliveries: run it with `E2E_MODE=isolated-limit` and with `E2E_MODE=inline-limit`. `wrangler.jsonc` sets no CPU limit, so a delivery is reset after the default 30 seconds. To let a longer `spin` finish, add `"limits": { "cpu_ms": 300000 }` before deploying. `E2E_JURISDICTION=eu` creates the object in that jurisdiction, and `where` then checks that its step ran in the same one.

`evict-many` runs `evict` on ten objects with delays from 5 to 60 minutes (`E2E_EVICT_DELAYS_MS`). A run fails unless a different instance ran the step after the delay, so it does not pass on local workerd, which keeps the object in memory. `deploy` needs a `wrangler deploy` with a changed `padding.ts` about 20 seconds after it starts. Each `run.ts` process uses a new object unless `E2E_OBJECT` names one, so scenarios can run at the same time in separate processes.

`test/cloudflare-e2e/bench` compares Notation Durable with Cloudflare Workflows. Its Worker holds the same workflows written for both (`durable.ts` and `native.ts`), and `run.ts` prints the medians and 90th percentiles as Markdown tables. Deploy it with a secret, run it, and delete the Worker and its workflow afterwards. A full run takes about 15 minutes:

```sh
cd test/cloudflare-e2e
export BENCH_SECRET=$(openssl rand -hex 16)
pnpm exec wrangler deploy -c bench/wrangler.jsonc --var BENCH_SECRET:$BENCH_SECRET
export BENCH_URL=https://durable-bench.<subdomain>.workers.dev

node --import tsx bench/run.ts > results.md   # every scenario
node --import tsx bench/run.ts chain burst    # some scenarios; BENCH_REPEAT=2 doubles the measurements, BENCH_OBJECTS sets how many sets of objects share them

pnpm exec wrangler delete -c bench/wrangler.jsonc
pnpm exec wrangler workflows delete durable-bench-native
```

The scenarios and what each measures:

| Scenario | Workflow | Measured |
| :--- | :--- | :--- |
| `start` | One step | The trigger call, the time until the step function starts, and the time until a caller in a Worker holds the result |
| `chain` | 100 steps in sequence | The time per step |
| `payload` | 20 steps that each return 100 KB | The time per step |
| `sleep` | 5 sleeps of 1 s | The time beyond 1 s until the next step starts |
| `fanout` | 20 concurrent steps that each wait 100 ms | The time beyond 100 ms until the next step starts |
| `signal` | A wait for a signal from outside | The time from the signal to the next step starting |
| `children` | A parent that runs 5 children | The time from starting the children to joining their results |
| `burst` | 100 three-step executions triggered at once | The trigger calls, and the time until the median and the last execution finish |

The `Bench` Durable Object drives each measurement and is its clock. A step function reports that it started by calling `mark` on that object, which records its own `Date.now()`, and the driver reads its times in the same object. So no figure compares the clocks of two machines, and none depends on the Worker's clock advancing during CPU work. A call to the clock takes 10 to 20 ms, which is as long as some of the measured times. So `mark` calls twice in a row, the time between the two arrivals gives the round trip, and the driver takes half of it off each figure that starts or ends at a step. The three systems take turns within each scenario, and the first execution of each workflow is not measured.

Two more suites run inside workerd with `@cloudflare/vitest-plugin`, on real Durable Object storage and alarms. They need no Cloudflare account and no build, and CI runs them in the `workerd` job. `test/cloudflare-e2e/conformance` runs the runtime conformance suites. `test/cloudflare-e2e/host` runs the workflow object's scenarios; its alarms are the ones workerd fires. Local workerd puts every instance of a class in one isolate and has no CPU limit, so the suite covers the calls between an object and the instances that run its deliveries, and the deployed scenarios cover the isolation. `test/cloudflare-e2e` is outside the pnpm workspace, so install it with npm first:

```sh
(cd test/cloudflare-e2e && npm install)
node ./node_modules/vitest/vitest.mjs run --root test/cloudflare-e2e/conformance
node ./node_modules/vitest/vitest.mjs run --root test/cloudflare-e2e/host
```

`pok test isolated` runs the whole suite again with `DURABLE_TEST_DELIVERIES=isolated`. `vitest.config.ts` then replaces `@durable/test-invoker` with `test/isolated-test-invoker.ts`, which runs each delivery as a Cloudflare object runs it in another instance: the request and the reply are encoded, and the workflow reaches the runtime through the remote runtime. A test that depends on a delivery running in the dispatcher's process is skipped in this mode with `test.skipIf(ISOLATED_DELIVERIES)`, and a comment says why.

`test/cloudflare-arm-invariant.test.ts` is part of `pok test`. It runs random sequences of triggers, cancellations, alarms and evictions against the workflow object over the fake storage, and checks that the alarm covers the earliest claim time. A failure prints its seed. `ARM_SEED=<seed>` runs that sequence again, and `ARM_RUNS` sets how many sequences run from that seed.

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
