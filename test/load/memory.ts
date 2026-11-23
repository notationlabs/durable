import { loadRouter, runLoad } from "@durable/runtime-load";
import { createTestSdk } from "@durable/test-utils";
import { finish, loadOptions } from "./options";

const sdk = createTestSdk(loadRouter);

const report = await runLoad(
  {
    sdk,
    probe: async () => {
      const { heapUsed, rss } = process.memoryUsage();
      return { heapUsedMb: Math.round(heapUsed / 2 ** 20), rssMb: Math.round(rss / 2 ** 20) };
    },
  },
  loadOptions()
);
finish("memory", report);
