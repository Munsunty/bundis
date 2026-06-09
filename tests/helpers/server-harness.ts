/**
 * Contract-test harness.
 *
 * `withServer` boots a real server on an ephemeral port (an isolated in-memory
 * DB by default), connects a **stock** `Bun.RedisClient`, runs the test body,
 * then tears both down. This is the only honest way to verify wire compat (§8):
 * we assert on the JS values the real client returns.
 */

import { RedisClient } from "bun";
import { startServer, type RunningServer } from "../../src/server";
import { loadConfig, type ServerConfig } from "../../src/config";

export interface HarnessOptions {
  dbPath?: string;
  password?: string | null;
  reaperIntervalMs?: number;
}

export interface Harness {
  url: string;
  port: number;
  running: RunningServer;
  /** A connected stock client. */
  client: RedisClient;
  /** Open an additional stock client (e.g. a pub/sub subscriber). */
  makeClient(): Promise<RedisClient>;
}

export async function startHarness(opts: HarnessOptions = {}): Promise<Harness> {
  const config: ServerConfig = {
    ...loadConfig([]),
    host: "127.0.0.1",
    port: 0,
    dbPath: opts.dbPath ?? ":memory:",
    password: opts.password ?? null,
    reaperIntervalMs: opts.reaperIntervalMs ?? 50,
  };
  const running = startServer(config);
  const auth = config.password ? `:${config.password}@` : "";
  const url = `redis://${auth}127.0.0.1:${running.port}`;

  const clients: RedisClient[] = [];
  const makeClient = async (): Promise<RedisClient> => {
    const c = new RedisClient(url);
    await c.connect();
    clients.push(c);
    return c;
  };
  const client = await makeClient();

  return {
    url,
    port: running.port,
    running,
    client,
    makeClient,
    // Internal: closers are attached below via closeHarness.
    ...({ _clients: clients } as object),
  } as Harness & { _clients: RedisClient[] };
}

export function closeHarness(h: Harness): void {
  const clients = (h as Harness & { _clients?: RedisClient[] })._clients ?? [h.client];
  for (const c of clients) {
    try {
      c.close();
    } catch {
      // already closed
    }
  }
  h.running.stop();
}

/**
 * Convenience wrapper: start a harness, run `fn`, always tear down.
 */
export async function withServer(
  fn: (h: Harness) => Promise<void>,
  opts: HarnessOptions = {},
): Promise<void> {
  const h = await startHarness(opts);
  try {
    await fn(h);
  } finally {
    closeHarness(h);
  }
}
