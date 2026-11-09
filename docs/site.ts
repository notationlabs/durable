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
          heading: "Getting Started",
          icon: "rocket",
          links: [
            { label: "Introduction", slug: "manual/introduction" },
            { label: "Installation", slug: "manual/installation" },
            { label: "Quick Start", slug: "manual/quickstart" },
          ],
        },
        {
          heading: "Workflows",
          icon: "cpu",
          links: [
            { label: "Defining Workflows", slug: "manual/workflows" },
            { label: "Steps", slug: "manual/steps" },
            { label: "Concurrent Steps", slug: "manual/concurrent-steps" },
            { label: "Retries", slug: "manual/retries" },
            { label: "Cancellation", slug: "manual/cancellation" },
          ],
        },
        {
          heading: "State",
          icon: "database",
          links: [
            { label: "Durable Stores", slug: "manual/stores" },
            { label: "Waiting on State", slug: "manual/store-waiting" },
            { label: "External Store Access", slug: "manual/store-external" },
          ],
        },
        {
          heading: "Interfaces",
          icon: "terminal",
          links: [
            { label: "Local SDK", slug: "manual/sdk-local" },
            { label: "HTTP SDK", slug: "manual/sdk-http" },
            { label: "HTTP Server", slug: "manual/http-server" },
            { label: "Testing", slug: "manual/testing" },
          ],
        },
      ],
    },
    {
      label: "Runtimes",
      slug: "runtimes",
      sections: [
        {
          heading: "Runtime families",
          icon: "cpu",
          links: [
            { label: "Runtime Matrix", slug: "runtimes/index" },
            { label: "Resident Process", slug: "runtimes/resident-process" },
          ],
        },
        {
          heading: "SQLite connectors",
          icon: "database",
          links: [
            { label: "Bun", slug: "runtimes/resident-process-bun" },
            { label: "Node", slug: "runtimes/resident-process-node" },
          ],
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
