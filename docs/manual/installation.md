# Installation

Install Durable and `pino`:

```sh
@pm.add("@notation/durable pino")
```

Durable logs through a `pino` logger. Your application creates the logger and passes it to Durable, so it imports `pino` itself.

## Requirements

| Runtime | Version | Notes |
| --- | --- | --- |
| Bun | 1.3 or newer | Runs TypeScript files directly |
| Node | 22.18 or newer | Runs TypeScript files directly. Node 22.13 to 22.17 can run Durable from compiled JavaScript |

Durable uses SQLite through the runtime's built-in driver: `bun:sqlite` on Bun and `node:sqlite` on Node. You do not install a separate SQLite package.

## Entry points

`@notation/durable` is one package with several entry points. An application loads only the entry points it imports.

| Entry point | Contains |
| --- | --- |
| `@notation/durable` | `workflow`, the router, the SDKs and the error classes |
| `@notation/durable/sqlite/bun` | SQLite storage for Bun |
| `@notation/durable/sqlite/node` | SQLite storage for Node |
| `@notation/durable/worker` | The worker process that runs workflow code |
| `@notation/durable/http` | HTTP routes for triggering and observing workflows |
| `@notation/durable/testing` | An in-memory runtime for tests |
| `@notation/durable/runtime` | Types and helpers for writing your own runtime |

`@notation/durable-conformance` is a separate package. It contains test suites for people who write their own runtime.
