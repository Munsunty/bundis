/**
 * Entry point: load config, start the RESP server, log the bound address.
 *
 * Run: `bun run src/index.ts [--host H] [--port P] [--db PATH] [--password PW]`
 */

import { loadConfig } from "./config";
import { startServer } from "./server";

const config = loadConfig();
const running = startServer(config);

console.log(
  `bun-resp-sqlite listening on ${running.hostname}:${running.port} ` +
    `(db: ${config.dbPath}${config.password ? ", auth: on" : ""})`,
);

function shutdown(): void {
  running.stop();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
