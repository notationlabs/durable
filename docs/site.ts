import type { DocsSite } from "@notation/docs";

export default {
  slug: "durable",
  label: "Durable",
  root: new URL(".", import.meta.url),
  defaultSlug: "core/introduction",
  version: { packageJson: "package.json" },
  categories: [
    {
      label: "Core",
      slug: "core",
      sections: [
        {
          heading: "Getting Started",
          icon: "rocket",
          links: [
            { label: "Introduction", slug: "core/introduction" },
            { label: "Quickstart", slug: "core/quickstart" },
          ],
        },
        {
          heading: "Workflows",
          icon: "cpu",
          links: [
            { label: "Workflows", slug: "core/workflows" },
            { label: "Steps", slug: "core/steps" },
            { label: "Step Values", slug: "core/step-values" },
            { label: "Concurrent Steps", slug: "core/concurrent-steps" },
            { label: "Child Workflows", slug: "core/child-workflows" },
            { label: "External Effects", slug: "core/external-effects" },
            { label: "Errors & Retries", slug: "core/errors-and-retries" },
            { label: "Cancellation", slug: "core/cancellation" },
          ],
        },
        {
          heading: "State",
          icon: "database",
          links: [
            { label: "Stores", slug: "core/stores" },
            { label: "Waiting on State", slug: "core/waiting-on-state" },
            { label: "External Store Access", slug: "core/external-store-access" },
          ],
        },
        {
          heading: "Executions & HTTP",
          icon: "terminal",
          links: [
            { label: "Executions", slug: "core/executions" },
            { label: "Local SDK", slug: "core/local-sdk" },
            { label: "HTTP SDK", slug: "core/http-sdk" },
            { label: "HTTP Server", slug: "core/http-server" },
          ],
        },
        {
          heading: "Testing & Deployment",
          icon: "cloud",
          links: [
            { label: "Testing", slug: "core/testing" },
            { label: "Running a Server", slug: "core/running-a-server" },
            { label: "Bun & Node", slug: "core/runtimes" },
            { label: "Cloudflare Workers", slug: "core/cloudflare" },
          ],
        },
      ],
    },
    {
      label: "Authoring",
      slug: "authoring",
      sections: [
        {
          heading: "Runtimes",
          icon: "layers",
          links: [
            { label: "Custom Runtimes", slug: "authoring/runtimes" },
            { label: "Leases & Delivery", slug: "authoring/delivery-guarantees" },
          ],
        },
        {
          heading: "Clients",
          icon: "database",
          links: [
            { label: "Execution Client", slug: "authoring/execution-client" },
            { label: "Heap Client", slug: "authoring/heap-client" },
            { label: "Store Client", slug: "authoring/store-client" },
            { label: "Effect Client", slug: "authoring/effect-client" },
            { label: "Join Client", slug: "authoring/join-client" },
          ],
        },
        {
          heading: "Testing",
          icon: "terminal",
          links: [{ label: "Conformance", slug: "authoring/conformance" }],
        },
      ],
    },
    {
      label: "Reference",
      slug: "reference",
      sections: [
        {
          heading: "SDK",
          icon: "layers",
          links: [
            { label: "@notation/durable", slug: "reference/durable" },
            { label: "@notation/durable/client", slug: "reference/client" },
            { label: "@notation/durable/testing", slug: "reference/testing" },
            { label: "Errors", slug: "reference/errors" },
          ],
        },
        {
          heading: "Adapters & Runtime",
          icon: "cpu",
          links: [
            { label: "@notation/durable/http", slug: "reference/http" },
            { label: "@notation/durable/worker", slug: "reference/worker" },
            { label: "@notation/durable/sqlite", slug: "reference/sqlite" },
            { label: "@notation/durable/cloudflare", slug: "reference/cloudflare" },
            { label: "@notation/durable/runtime", slug: "reference/runtime" },
            { label: "@notation/durable-conformance", slug: "reference/conformance" },
          ],
        },
      ],
    },
    {
      label: "Internals",
      slug: "internals",
      sections: [
        {
          heading: "Overview",
          icon: "layers",
          links: [{ label: "Architecture", slug: "internals/architecture" }],
        },
        {
          heading: "Execution Engine",
          icon: "cpu",
          links: [
            { label: "Replay & Step Heap", slug: "internals/replay-and-heap" },
            { label: "Concurrent Combinators", slug: "internals/combinators" },
            { label: "Cancellation", slug: "internals/cancellation" },
            { label: "Leases & Dispatcher", slug: "internals/leases-and-dispatcher" },
          ],
        },
        {
          heading: "State & Storage",
          icon: "database",
          links: [
            { label: "Store Wakes", slug: "internals/store-wakes" },
            { label: "Value Codec & Errors", slug: "internals/value-codec" },
            { label: "SQLite Storage", slug: "internals/sqlite-storage" },
          ],
        },
        {
          heading: "Transport",
          icon: "terminal",
          links: [
            { label: "Worker IPC", slug: "internals/worker-ipc" },
            { label: "HTTP Wire Protocol", slug: "internals/http-wire" },
          ],
        },
      ],
    },
  ],
} satisfies DocsSite;
