## Contributing to Durable

### Before You Start

- Please open an issue to discuss significant changes before submitting a pull request. This helps align on scope and approach, and avoids wasted effort.

### Pull Requests

- Keep PRs focused and reasonably small; include a clear description and motivation.
- Add tests if you change behavior or fix a bug.
- Update documentation where relevant.
- Ensure `pok test bun` and `pok test node` pass locally.

This repository is a TypeScript monorepo using pnpm workspaces and Bun. It contains the core SDK, runtimes, server integrations, examples, and tests.


### Contributor License Agreement (CLA)

By contributing, you affirm that you have the right to submit your contributions and you agree to license them under the project’s license (MIT).


### Repository Layout

Each folder in `packages/` is a private workspace package named `@durable/<folder>`. Packages import each other only through these names, which keeps the boundaries between them.

- `packages/core` – Base types and runtime abstractions (scheduler, heap, steps)
- `packages/sdk` – Main SDK: workflow API, local/HTTP client
- `packages/http-server` – HTTP route handlers and middleware
- `packages/sqlite-runtime` – SQLite-backed heap, timers, task queue, event loop
- `packages/worker-invoker` – Subprocess-based step invoker and worker
- `packages/runtime-conformance` – Qualification suites for runtimes
- `packages/test-runtime` – In‑memory scheduler, heap, event loop (for tests)
- `packages/test-invoker` – Minimal invoker to run workflows in tests
- `packages/test-utils` – Internal test utilities (see note below)
- `packages/durable` – The published `@notation/durable` package, assembled from the packages above
- `packages/durable-conformance` – The published `@notation/durable-conformance` package, assembled from `runtime-conformance`
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

### Type Checking / Declarations

Generate and check types across the workspace:

```
pok dts
```

### Build All Packages

```
pok build
```

This runs `scripts/build.ts`, which takes these steps:

1. Removes every package's `dist` folder.
2. Runs each workspace package's `build` script (`bun build`).
3. Emits declarations with `pnpm exec tsc -b`.
4. Runs `scripts/assemble.ts`. It copies the output of the private packages into `packages/durable/dist` and `packages/durable-conformance/dist`, and rewrites their imports of `@durable/*` packages. In `@notation/durable` they become relative paths. In `@notation/durable-conformance`, `@durable/core` becomes `@notation/durable/runtime`. The script fails if the output imports a package that the published manifest does not declare.
5. Type-checks `examples/`, which imports the published packages.

Tests import the private packages. The examples import the published packages. Code that mixes the two loads two copies of the core classes.

`pok test conformance` packs both published packages, installs them in an empty project and runs every conformance suite against the published SQLite runtime.

### Run Tests

```
pok test bun
pok test node
```

`pok test all` runs every suite in sequence.

Tests live in `test/` and import from Vitest. The full suite runs under both Bun and Node.

### Dev Workflow

Run type‑checking, builds, and tests concurrently in watch mode:

```
pok dev
```

Run `pok` with no arguments for the full command menu.

### Documentation

`docs/` holds the Markdown, published at notation.dev/engine. `docs/site.ts`
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

### Internal Packages

`@durable/test-utils` is intended for this repo’s tests and development. It wires an in‑memory runtime for `triggerAndWait` in tests. Application users should prefer the SQLite runtime for local running.

### Coding Guidelines

- Keep changes minimal and focused on the task.
- Match existing coding style and TypeScript patterns.
- Avoid unrelated refactors in the same change.
