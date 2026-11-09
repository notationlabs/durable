# Installation

```sh
@pm.add("@notation/durable")
```

The package contains the SDK, the resident-process runtime, the SQLite connectors and the HTTP routes. Each part has its own entry point, so an application loads only the parts it imports. The examples in this manual also use `pino` for logging:

```sh
@pm.add("pino")
```

Import `createSqliteDb` from `@notation/durable/sqlite/bun` when using Bun or `@notation/durable/sqlite/node` when using Node 22.6 or newer with TypeScript workers.

See the [runtime matrix](../runtimes/index.md) for supported runtime families and connector combinations.
