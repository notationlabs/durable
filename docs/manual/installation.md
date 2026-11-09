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
| Node | 22.18 or newer | Runs TypeScript files directly. Node 22.13 to 22.17 run them with `--experimental-strip-types`. See [Node](./node.md) |

Durable uses SQLite through the runtime's built-in driver: `bun:sqlite` on Bun and `node:sqlite` on Node. You do not install a separate SQLite package.
