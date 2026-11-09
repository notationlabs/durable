# @notation/durable-conformance

Vitest suites that check a [Durable](https://notation.dev/durable) runtime: its execution, heap, store, effect and join clients, and cancellation.

```sh
npm install --save-dev @notation/durable-conformance vitest
```

Each suite registers its tests in the file that calls it. `create` returns a runtime over an empty database for each test, built on the suite's clock:

```ts
import { registerExecutionClientConformance } from "@notation/durable-conformance";
import { MyRuntime, openDatabase } from "../src";

registerExecutionClientConformance({
  name: "my-runtime",
  create({ clock }) {
    const db = openDatabase();
    return {
      runtime: new MyRuntime(db, { clock }),
      createPeer: () => new MyRuntime(db.connect(), { clock }),
      dispose: () => db.close(),
    };
  },
});
```

[Conformance](https://notation.dev/durable/runtime-authors/conformance) lists the five suites and the optional harness fields, with a complete example for `SqliteRuntime`. The [Runtime authors](https://notation.dev/durable/runtime-authors/runtimes) section describes the contract that the suites check.

## License

MIT
