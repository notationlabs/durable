# Durable 🤘

JavaScript-native distributed workflows.

Write workflows as async generator functions that yield steps. First-class primitives for retries, delays, polling, and durable stores.

## Install

```sh
bun add @notation/durable
```

The runtime parts are entry points of the same package:

- Local execution: `@notation/durable/worker`, `@notation/durable/sqlite`
- HTTP server: `@notation/durable/http`, `@notation/durable/worker`

Runtime authors can check a runtime with `@notation/durable-conformance`.

Select `@notation/durable/sqlite/bun` for `bun:sqlite` or `@notation/durable/sqlite/node` for `node:sqlite`.

## Quick Look

Define a workflow that composes steps:

```ts
import { workflow } from "@notation/durable";

export const simple = workflow(async function* (step) {
  const n = yield* step.run(() => 1);
  yield* step.delay(1000);
  return yield* step.run(() => n * 2);
});
```

Retries are explicit and structured:

```ts
import { RetryableError } from "@notation/durable";

yield* step.run(async () => {
  // e.g. flaky fetch
  throw new RetryableError("temporary", { maxAttempts: 4, retryInterval: 1000 });
});
```

## Learn More

- Full usage, runtimes, APIs, and examples: [packages/durable/README.md](packages/durable/README.md)
- Runtime families and connectors: [docs/runtimes/index.md](docs/runtimes/index.md)
- Store connector specification and qualification: [docs/store-connector-spec.md](docs/store-connector-spec.md)

## Contributing

- Please open an issue to discuss significant changes before submitting a PR.
- See CONTRIBUTING.md for repo layout, dev workflow, testing, and release details.

## License

MIT

## Author

[Daniel Grant](https://danielgrant.co)
