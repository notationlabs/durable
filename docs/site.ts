import type { DocsSite } from "@notation/docs";

export default {
  slug: "engine",
  label: "Durable Engine",
  root: new URL(".", import.meta.url),
  version: { packageJson: "package.json" },
  categories: [
    {
      label: "User Manual",
      slug: "manual",
      sections: [
        {
          heading: "Getting started",
          icon: "rocket",
          links: [
            { label: "Introduction", slug: "manual/introduction" },
            { label: "Installation", slug: "manual/installation" },
            { label: "Quick start", slug: "manual/quickstart" },
          ],
        },
        {
          heading: "Workflows",
          icon: "cpu",
          links: [
            { label: "Defining workflows", slug: "manual/workflows" },
            { label: "Steps", slug: "manual/steps" },
            { label: "Step keys", slug: "manual/step-keys" },
            { label: "Step values", slug: "manual/step-values" },
            { label: "Errors", slug: "manual/errors" },
            { label: "Retries", slug: "manual/retries" },
            { label: "Concurrent steps", slug: "manual/concurrent-steps" },
            { label: "Child workflows", slug: "manual/child-workflows" },
            { label: "Cancellation", slug: "manual/cancellation" },
            { label: "External effects", slug: "manual/effects" },
          ],
        },
        {
          heading: "State",
          icon: "database",
          links: [
            { label: "Stores", slug: "manual/stores" },
            { label: "Waiting on state", slug: "manual/store-waiting" },
            { label: "External store access", slug: "manual/store-external" },
          ],
        },
        {
          heading: "Interfaces",
          icon: "terminal",
          links: [
            { label: "Local SDK", slug: "manual/sdk-local" },
            { label: "HTTP SDK", slug: "manual/sdk-http" },
            { label: "HTTP server", slug: "manual/http-server" },
            { label: "Testing", slug: "manual/testing" },
          ],
        },
        {
          heading: "Deployment",
          icon: "cloud",
          links: [
            { label: "Running a server", slug: "manual/running-a-server" },
            { label: "Bun", slug: "manual/bun" },
            { label: "Node", slug: "manual/node" },
          ],
        },
        {
          heading: "Reference",
          icon: "bulb",
          links: [{ label: "Error reference", slug: "manual/error-reference" }],
        },
      ],
    },
    {
      label: "Runtime Authors",
      slug: "runtime-authors",
      sections: [
        {
          heading: "Runtimes",
          icon: "layers",
          links: [
            { label: "Runtimes", slug: "runtime-authors/runtimes" },
            { label: "Delivery guarantees", slug: "runtime-authors/delivery-guarantees" },
          ],
        },
        {
          heading: "Clients",
          icon: "database",
          links: [
            { label: "Execution client", slug: "runtime-authors/execution-client" },
            { label: "Heap client", slug: "runtime-authors/heap-client" },
            { label: "Store client", slug: "runtime-authors/store-client" },
            { label: "Effect client", slug: "runtime-authors/effect-client" },
            { label: "Join client", slug: "runtime-authors/join-client" },
          ],
        },
        {
          heading: "Testing",
          icon: "terminal",
          links: [{ label: "Conformance", slug: "runtime-authors/conformance" }],
        },
      ],
    },
    {
      label: "Packages",
      slug: "packages",
      sections: [
        {
          heading: "Reference",
          icon: "terminal",
          links: [
            { label: "@notation/durable", slug: "packages/durable" },
            { label: "/runtime", slug: "packages/runtime" },
            { label: "/http", slug: "packages/http" },
            { label: "/sqlite", slug: "packages/sqlite" },
            { label: "/worker", slug: "packages/worker" },
            {
              label: "@notation/durable-conformance",
              slug: "packages/conformance",
            },
          ],
        },
      ],
    },
  ],
} satisfies DocsSite;
