/**
 * Public launchers for using bundis from another project.
 *
 * Two ways to run the server (both return a ready-to-use `redis://` URL for a
 * stock `Bun.RedisClient`):
 *
 * - {@link embedServer}  — in the **current** process. Zero IPC overhead, but
 *   shares the event loop with the host app (bun:sqlite is synchronous).
 * - {@link spawnServer}  — in a **separate** Bun process. Isolates the SQLite
 *   writer and any blocking work from the host app; resolves once the child
 *   signals readiness on stdout.
 */

import { startServer, type RunningServer } from "./server";
import type { ServerConfig } from "./config";

/** stdout JSON `event` value the CLI prints once it is accepting connections. */
export const READY_EVENT = "bundis:ready";

export interface LaunchOptions {
  /** Bind address (default "127.0.0.1"; use "0.0.0.0" to expose externally). */
  host?: string;
  /** TCP port (default 6379; 0 = pick an ephemeral port). */
  port?: number;
  /** SQLite file path (default "./data.db"; ":memory:" for non-persistent). */
  dbPath?: string;
  /** When set, clients must AUTH with this password. */
  password?: string;
  /** Active-expiry sweep interval in ms (default 100). */
  reaperIntervalMs?: number;
  /**
   * Overall memory budget in MB (default 256). Sizes the SQLite page cache
   * (50%) and the hot cache default (25%). A budget, not a hard OS limit.
   */
  maxMemoryMb?: number;
  /** Hot-cache ceiling in MB (default: maxMemoryMb/4; 0 disables the cache). */
  cacheMb?: number;
  /** Hot-cache base time-to-idle in seconds (default 300). */
  cacheIdleSec?: number;
}

export interface EmbeddedServer {
  readonly host: string;
  /** Actual bound port (resolves `port: 0`). */
  readonly port: number;
  /** Connection URL for a stock `new RedisClient(url)`. */
  readonly url: string;
  /** Underlying server handle (storage, pub/sub hub, config). */
  readonly server: RunningServer;
  /** Stop listening, stop the reaper, close storage. */
  stop(): void;
}

/** Start the server inside the current Bun process (main-process mode). */
export function embedServer(opts: LaunchOptions = {}): EmbeddedServer {
  const config = resolveConfig(opts);
  const running = startServer(config);
  return {
    host: running.hostname,
    port: running.port,
    url: clientUrl(running.hostname, running.port, config.password),
    server: running,
    stop: () => running.stop(),
  };
}

export interface SpawnedServer {
  readonly host: string;
  /** Actual bound port (resolves `port: 0`). */
  readonly port: number;
  /** Connection URL for a stock `new RedisClient(url)`. */
  readonly url: string;
  readonly pid: number;
  /** Kill the child process and wait for it to exit. */
  stop(): Promise<void>;
}

export interface SpawnServerOptions extends LaunchOptions {
  /** Bun executable to launch with (default: the currently running bun). */
  bunPath?: string;
  /** Max ms to wait for the child's ready signal (default 10_000). */
  readyTimeoutMs?: number;
}

/** Start the server as a separate Bun process (sidecar mode). */
export async function spawnServer(opts: SpawnServerOptions = {}): Promise<SpawnedServer> {
  const config = resolveConfig(opts);
  const cliPath = Bun.fileURLToPath(new URL("./cli.ts", import.meta.url));
  // The password travels via environment, never argv — argv is visible to
  // every local user in `ps` for the lifetime of the child.
  const env: Record<string, string | undefined> = { ...process.env };
  delete env.REDIS_PASSWORD;
  if (config.password !== null) env.REDIS_PASSWORD = config.password;
  const proc = Bun.spawn(
    [
      opts.bunPath ?? process.execPath,
      cliPath,
      "--host", config.host,
      "--port", String(config.port),
      "--db", config.dbPath,
      "--reaper", String(config.reaperIntervalMs),
      "--max-memory-mb", String(Math.floor(config.maxMemoryBytes / (1024 * 1024))),
      "--cache-mb", String(Math.floor(config.cacheMaxBytes / (1024 * 1024))),
      "--cache-idle", String(Math.floor(config.cacheIdleMs / 1000)),
    ],
    { stdout: "pipe", stderr: "inherit", env },
  );

  let ready: { host: string; port: number };
  try {
    ready = await waitForReady(proc.stdout, opts.readyTimeoutMs ?? 10_000);
  } catch (err) {
    proc.kill();
    await proc.exited;
    throw err;
  }

  return {
    host: ready.host,
    port: ready.port,
    url: clientUrl(ready.host, ready.port, config.password),
    pid: proc.pid,
    async stop() {
      proc.kill();
      await proc.exited;
    },
  };
}

// ── helpers ──────────────────────────────────────────────────────────────---

function resolveConfig(opts: LaunchOptions): ServerConfig {
  const MB = 1024 * 1024;
  const maxMemoryMb = opts.maxMemoryMb ?? 256;
  return {
    host: opts.host ?? "127.0.0.1",
    port: opts.port ?? 6379,
    dbPath: opts.dbPath ?? "./data.db",
    // Honor flag > env > none, so an env-configured password is not silently
    // dropped (which would launch an open server — fail-open regression).
    password: opts.password ?? Bun.env.REDIS_PASSWORD ?? null,
    reaperIntervalMs: opts.reaperIntervalMs ?? 100,
    maxClients: 10_000,
    maxMemoryBytes: maxMemoryMb * MB,
    cacheMaxBytes: (opts.cacheMb ?? Math.floor(maxMemoryMb / 4)) * MB,
    cacheIdleMs: (opts.cacheIdleSec ?? 300) * 1000,
  };
}

function clientUrl(host: string, port: number, password: string | null): string {
  // A wildcard bind address is not connectable; clients use loopback.
  const h = host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
  const auth = password === null ? "" : `:${encodeURIComponent(password)}@`;
  return `redis://${auth}${h}:${port}`;
}

/** Scan child stdout line-by-line until the READY_EVENT JSON line appears. */
async function waitForReady(
  stdout: ReadableStream<Uint8Array>,
  timeoutMs: number,
): Promise<{ host: string; port: number }> {
  const reader = stdout.getReader();
  const decoder = new TextDecoder();
  const deadline = Date.now() + timeoutMs;
  let buf = "";
  for (;;) {
    const { done, value } = await Promise.race([
      reader.read(),
      rejectAfter(deadline - Date.now()),
    ]);
    if (done) {
      reader.releaseLock();
      throw new Error("bundis child exited before signalling ready");
    }
    buf += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buf.indexOf("\n")) !== -1) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (!line.startsWith("{")) continue;
      try {
        const msg = JSON.parse(line) as { event?: string; host?: string; port?: number };
        if (msg.event === READY_EVENT && msg.host !== undefined && msg.port !== undefined) {
          void drain(reader); // keep the pipe from filling if the child logs later
          return { host: msg.host, port: msg.port };
        }
      } catch {
        // not our line
      }
    }
  }
}

async function drain(reader: { read(): Promise<{ done: boolean }> }): Promise<void> {
  try {
    while (!(await reader.read()).done) {
      // discard
    }
  } catch {
    // stream torn down with the process
  }
}

function rejectAfter(ms: number): Promise<never> {
  return new Promise((_, reject) => {
    const t = setTimeout(
      () => reject(new Error("timed out waiting for bundis ready signal")),
      Math.max(0, ms),
    );
    (t as unknown as { unref?: () => void }).unref?.();
  });
}
