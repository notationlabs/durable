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

- `packages/core` – Base types and runtime abstractions (scheduler, heap, steps)
- `packages/sdk` – Main SDK: workflow API, local/HTTP client
- `packages/http-server` – HTTP route handlers and middleware
- `packages/sqlite-runtime` – SQLite-backed heap, timers, task queue, event loop
- `packages/worker-invoker` – Subprocess-based step invoker and worker
- `packages/runtime-conformance` – Qualification suites for runtimes
- `packages/test-runtime` – In‑memory scheduler, heap, event loop (for tests)
- `packages/test-invoker` – Minimal invoker to run workflows in tests
- `packages/test-utils` – Internal test utilities (see note below)
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

This checks every private package and `test/` as one TypeScript program.

### Build

```
pok build
```

This runs the `build` script of each published package, then type-checks `examples/`, which imports the published packages.

Each published package has a `tsup.config.ts` that bundles the source of the private packages it ships:

- `@notation/durable` bundles `sdk`, `core`, `http-server`, `worker-invoker` and `sqlite-runtime`. Each export path is one tsup entry. Code shared between entries goes into shared chunks, so every export path uses the same classes.
- `@notation/durable-conformance` bundles `runtime-conformance`. Its `@durable/core` imports resolve to `src/runtime.ts`, which re-exports `@notation/durable/runtime`. The suites then use the classes in the user's copy of `@notation/durable`. This package builds after `@notation/durable`, because its declarations import the built `@notation/durable`.

Tests import the private packages. The examples import the published packages. Code that mixes the two loads two copies of the core classes.

### Run Tests

```
pok test bun
pok test conformance
```

`pok test all` runs every suite in sequence.

Local development runs on Bun. Some tests start child processes that import the private packages, and only Bun can run that TypeScript source directly.

`pok test conformance` builds nothing itself. Run `pok build` first. It packs both published packages, installs them in an empty project and runs every conformance suite under Node against the published SQLite runtime. This is the test of the packages under Node.

### Dev Workflow

Run type checking and tests in watch mode:

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
