import type { DocCategory } from "@notation/docs/config";
import { manual } from "./manual/nav";
import { runtimes } from "./runtimes/nav";
import { packages } from "./packages/nav";

export const categories: DocCategory[] = [manual, runtimes, packages];
