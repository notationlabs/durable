import { readdirSync, rmSync } from "node:fs";
import { join } from "node:path";

// Old chunks and declarations would otherwise be copied into the published
// packages, so every package is built from an empty dist folder.
for (const entry of readdirSync("packages", { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const folder = entry.name;
  rmSync(join("packages", folder, "dist"), { recursive: true, force: true });
  rmSync(join("packages", folder, "tsconfig.tsbuildinfo"), { force: true });
}

const steps = [
  ["bun", "--filter", "@durable/*", "build"],
  ["pnpm", "exec", "tsc", "-b"],
  ["bun", "scripts/assemble.ts"],
  // The examples import the published packages, so they are checked after assembly.
  ["pnpm", "exec", "tsc", "-p", "examples"],
];

for (const command of steps) {
  const result = Bun.spawnSync(command, { stdout: "inherit", stderr: "inherit" });
  if (result.exitCode !== 0) {
    throw new Error(`${command.join(" ")} exited with ${result.exitCode}`);
  }
}
