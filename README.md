# Durable

Durable is a TypeScript library for running workflows and keeping state that outlives any single process. It runs inside your application on Bun or Node, and keeps its records in a SQLite file.

```sh
npm install @notation/durable pino
```

A workflow is an async generator function made of steps. Durable records the result of each step as it finishes, so a workflow can wait for days and survive a restart without repeating the steps that already finished:

```ts
import { workflow } from "@notation/durable";
import { sendEmail } from "./email";

export const onboarding = workflow<{ email: string }>(async function* (step, event) {
  const { email } = event.params;

  yield* step.run("send-welcome", async () => {
    await sendEmail(email, "Welcome to Acme");
  });

  yield* step.delay("wait", 3 * 24 * 60 * 60 * 1000);

  yield* step.run("send-tips", async () => {
    await sendEmail(email, "Three tips for your first week");
  });
});
```

The [Quick start](https://notation.dev/durable/manual/quickstart) sets up the runtime and the worker process, and starts this workflow. The [documentation](https://notation.dev/durable) covers workflows, stores, deployment and the API.

## Packages

| Package | Is |
| --- | --- |
| [`@notation/durable`](packages/durable) | The library |
| [`@notation/durable-conformance`](packages/durable-conformance) | Test suites for authors of other runtimes |

## Contributing

Please open an issue to discuss a significant change before you submit a pull request. See [CONTRIBUTING.md](CONTRIBUTING.md) for the repository layout, the development commands and releases.

## License

MIT
