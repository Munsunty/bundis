/**
 * Public API of bun-resp-sqlite.
 *
 * - embedServer(): run the server in the current process (main-process mode).
 * - spawnServer(): run the server as a separate Bun process (sidecar mode).
 * - startServer(): lower-level handle taking a full ServerConfig.
 *
 * For a standalone daemon, use the CLI: `bun run src/cli.ts` / `bunx bun-resp-sqlite`.
 */

export { startServer, type RunningServer } from "./server";
export { loadConfig, type ServerConfig } from "./config";
export {
  embedServer,
  spawnServer,
  READY_EVENT,
  type LaunchOptions,
  type EmbeddedServer,
  type SpawnedServer,
  type SpawnServerOptions,
} from "./launch";
