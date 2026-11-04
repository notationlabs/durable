import type { DocsSite } from "@notation/docs";

export default {
  slug: "engine",
  label: "Engine",
  root: new URL(".", import.meta.url),
  categories: ["manual", "runtimes", "packages"],
  version: { packageJson: "package.json" },
} satisfies DocsSite;
