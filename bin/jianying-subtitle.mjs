#!/usr/bin/env node
import { main } from "../dist/cli/cli.js";

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
