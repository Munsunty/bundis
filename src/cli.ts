#!/usr/bin/env bun
/**
 * CLI entry: load config from flags/env, start the server, signal readiness.
 *
 * Run: `bun run src/cli.ts [--host H] [--port P] [--db PATH] [--password PW]`
 * (or `bunx bun-resp-sqlite ...` when installed as a dependency).
 *
 * stdout carries exactly one machine-readable JSON ready line — this is the
 * signal `spawnServer()` waits for. Human-facing logs go to stderr.
 */

import { loadConfig } from "./config";
import { startServer } from "./server";
import { READY_EVENT } from "./launch";

const config = loadConfig();
const running = startServer(config);

console.log(
  JSON.stringify({
    event: READY_EVENT,
    host: running.hostname,
    port: running.port,
    db: config.dbPath,
  }),
);
console.error(
  `bun-resp-sqlite listening on ${running.hostname}:${running.port} ` +
    `(db: ${config.dbPath}${config.password ? ", auth: on" : ""})`,
);

function shutdown(): void {
  running.stop();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
