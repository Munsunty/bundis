/**
 * Server configuration, resolved from CLI flags and environment.
 *
 * Precedence: CLI flag > env var > default. Kept tiny and dependency-free.
 *   --host / REDIS_HOST          (default 0.0.0.0)
 *   --port / REDIS_PORT          (default 6379; 0 = ephemeral, used by tests)
 *   --db   / REDIS_DB_PATH       (default ./data.db; ":memory:" for in-memory)
 *   --password / REDIS_PASSWORD  (default none → AUTH always succeeds)
 */

export interface ServerConfig {
  host: string;
  port: number;
  dbPath: string;
  password: string | null;
  /** Active-expiry sweep interval in ms. */
  reaperIntervalMs: number;
}

export function loadConfig(argv: string[] = Bun.argv.slice(2)): ServerConfig {
  const flags = parseFlags(argv);
  const env = Bun.env;
  return {
    host: flags.host ?? env.REDIS_HOST ?? "0.0.0.0",
    port: int(flags.port ?? env.REDIS_PORT, 6379),
    dbPath: flags.db ?? env.REDIS_DB_PATH ?? "./data.db",
    password: flags.password ?? env.REDIS_PASSWORD ?? null,
    reaperIntervalMs: int(flags.reaper ?? env.REDIS_REAPER_MS, 100),
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
