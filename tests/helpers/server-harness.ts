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
import { lockPath } from "../../src/sidecar/lockfile";
import { cleanupDb, mkTempDbPath } from "./temp-db";

/**
 * BUNDIS_TEST_MAINTENANCE=worker re-runs the contract suite in worker
 * maintenance mode under churn: every harness that does not pick a mode
 * itself gets a file database and a maintenance cycle requested every
 * millisecond, so commands keep landing while the overlay buffers or applies
 * its buffer — with the runtime write guard watching the main connection.
 */
const CHURN = Bun.env.BUNDIS_TEST_MAINTENANCE === "worker";

export interface HarnessOptions {
  dbPath?: string;
  password?: string | null;
  reaperIntervalMs?: number;
  maintenance?: "off" | "worker";
  maintParams?: Record<string, string>;
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
  const churn = CHURN && opts.maintenance === undefined;
  const tempDb = churn && opts.dbPath === undefined ? mkTempDbPath() : null;
  const config: ServerConfig = {
    ...loadConfig([]),
    host: "127.0.0.1",
    port: 0,
    dbPath: tempDb ?? opts.dbPath ?? ":memory:",
    password: opts.password ?? null,
    reaperIntervalMs: opts.reaperIntervalMs ?? 50,
    maintenance: churn ? "worker" : (opts.maintenance ?? "off"),
    maintParams: churn
      ? { "maint-interval-sec": "0", "maint-expired-threshold": "0", "maint-flush-budget-ms": "1", ...opts.maintParams }
      : (opts.maintParams ?? {}),
  };
  const running = startServer(config);
  let churnTimer: ReturnType<typeof setInterval> | null = null;
  if (churn) {
    const maint = running.server.maintenance!;
    const tick = (): Promise<void> => new Promise((r) => setTimeout(r, 1));
    const until = async (ok: () => boolean): Promise<void> => {
      for (let i = 0; i < 5_000 && !ok(); i++) await tick();
    };
    // Warm-up: one full cycle boots the worker thread, so later cycles take
    // ~1-2 ms and commands interleave with IDLE, FLUSHING and NORMAL.
    maint.runNow();
    await until(() => Number(maint.status().maint_cycles) >= 1 && maint.state === "normal");
    // Contract tests often live for a millisecond or two: hand the server
    // out already IDLE, so the test's first command lands in the buffer.
    maint.runNow();
    await until(() => maint.state !== "normal");
    // runNow() refuses while a cycle runs, so this keeps cycles back to back.
    churnTimer = setInterval(() => maint.runNow(), 1);
    const stop = running.stop.bind(running);
    (running as { stop: () => void }).stop = () => {
      if (churnTimer) clearInterval(churnTimer);
      stop();
      if (tempDb) {
        cleanupDb(tempDb);
        cleanupDb(lockPath(tempDb));
      }
    };
  }
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
