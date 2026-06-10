/**
 * Server configuration, resolved from CLI flags and environment.
 *
 * Precedence: CLI flag > env var > default. Kept tiny and dependency-free.
 *   --host / REDIS_HOST          (default 127.0.0.1; use 0.0.0.0 to expose)
 *   --port / REDIS_PORT          (default 6379; 0 = ephemeral, used by tests)
 *   --db   / REDIS_DB_PATH       (default ./data.db; ":memory:" for in-memory)
 *   --password / REDIS_PASSWORD  (default none → AUTH always succeeds)
 *   --max-memory-mb / REDIS_MAX_MEMORY_MB (default 256 — overall budget:
 *       50% SQLite page cache, 25% hot cache unless --cache-mb given)
 *   --cache-mb / REDIS_CACHE_MB  (default: maxMemory/4; 0 disables hot cache)
 *   --cache-idle / REDIS_CACHE_IDLE_SEC (default 300 — hot-cache base TTI)
 */

export interface ServerConfig {
  host: string;
  port: number;
  dbPath: string;
  password: string | null;
  /** Active-expiry sweep interval in ms. */
  reaperIntervalMs: number;
  /** Max simultaneous client connections. */
  maxClients: number;
  /**
   * Overall memory budget for the server in bytes. A budget, not a hard OS
   * limit: it sizes the SQLite page cache (50%) and, when --cache-mb is not
   * given, the hot cache (25%); the rest is headroom for connections/JS heap.
   */
  maxMemoryBytes: number;
  /** Hot-cache byte ceiling; 0 disables the in-memory cache. */
  cacheMaxBytes: number;
  /** Hot-cache base time-to-idle in ms. */
  cacheIdleMs: number;
}

export function loadConfig(argv: string[] = Bun.argv.slice(2)): ServerConfig {
  const flags = parseFlags(argv);
  const env = Bun.env;
  const MB = 1024 * 1024;
  const maxMemoryMb = int(flags["max-memory-mb"] ?? env.REDIS_MAX_MEMORY_MB, 256);
  // Unless set explicitly, the hot cache takes 25% of the overall budget.
  const cacheMbRaw = flags["cache-mb"] ?? env.REDIS_CACHE_MB;
  const cacheMb = cacheMbRaw !== undefined ? int(cacheMbRaw, 64) : Math.floor(maxMemoryMb / 4);
  return {
    // Loopback by default: exposing a (possibly password-less) server to the
    // LAN must be an explicit decision (--host 0.0.0.0).
    host: flags.host ?? env.REDIS_HOST ?? "127.0.0.1",
    port: int(flags.port ?? env.REDIS_PORT, 6379),
    dbPath: flags.db ?? env.REDIS_DB_PATH ?? "./data.db",
    password: flags.password ?? env.REDIS_PASSWORD ?? null,
    reaperIntervalMs: int(flags.reaper ?? env.REDIS_REAPER_MS, 100),
    maxClients: int(flags["max-clients"] ?? env.REDIS_MAX_CLIENTS, 10_000),
    maxMemoryBytes: maxMemoryMb * MB,
    cacheMaxBytes: cacheMb * MB,
    cacheIdleMs: int(flags["cache-idle"] ?? env.REDIS_CACHE_IDLE_SEC, 300) * 1000,
  };
}

function parseFlags(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (!a.startsWith("--")) continue;
    const eq = a.indexOf("=");
    if (eq !== -1) {
      out[a.slice(2, eq)] = a.slice(eq + 1);
    } else {
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith("--")) {
        out[a.slice(2)] = next;
        i++;
      } else {
        out[a.slice(2)] = "true";
      }
    }
  }
  return out;
}

function int(v: string | undefined, fallback: number): number {
  if (v === undefined) return fallback;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? fallback : n;
}
