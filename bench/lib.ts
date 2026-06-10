/**
 * Benchmark harness primitives.
 *
 * Measurement model (revised after methodology review):
 * - The server under test ALWAYS runs in a separate process (spawnServer) so the
 *   measuring client never shares an event loop with it. embedServer appears
 *   only in the explicit embed-vs-spawn comparison case.
 * - Headline cases run against a file-backed WAL DB (the real product config);
 *   :memory: appears only as the "storage ceiling" leg of the persistence case.
 * - Closed-loop sequential numbers are SERVICE TIME at concurrency 1, not
 *   "latency under load" (coordinated omission). Tail-latency claims under
 *   background load use the open-loop generator (intended-send-time latency).
 * - Throughput uses a sliding window: `depth` workers each issue the next op as
 *   soon as their previous one completes — no Promise.all batch-drain bubble.
 * - Timed loops never allocate: keys are precomputed, samples land in a
 *   preallocated Float64Array.
 * - Clients disable autoReconnect so a torn-down server can't leak retry work
 *   into the next case.
 */

import { RedisClient } from "bun";
import { embedServer, spawnServer, type LaunchOptions } from "../src";

export interface BenchServer {
  url: string;
  pid: number | null;
  stop(): Promise<void>;
}

export async function startBenchServer(
  mode: "spawn" | "embed",
  opts: LaunchOptions = {},
): Promise<BenchServer> {
  const base: LaunchOptions = { port: 0, ...opts };
  if (mode === "embed") {
    const s = embedServer(base);
    return { url: s.url, pid: null, stop: async () => s.stop() };
  }
  const s = await spawnServer(base);
  return { url: s.url, pid: s.pid, stop: () => s.stop() };
}

export async function connect(url: string): Promise<RedisClient> {
  const c = new RedisClient(url, { autoReconnect: false });
  await c.connect();
  return c;
}

// ── stats ────────────────────────────────────────────────────────────────---

export interface LatencyStats {
  n: number;
  p50us: number;
  p95us: number;
  p99us: number;
  maxUs: number;
  meanUs: number;
}

function percentileSorted(sorted: Float64Array, p: number): number {
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)]!;
}

export function latencyStats(samplesNs: Float64Array): LatencyStats {
  const sorted = Float64Array.from(samplesNs).sort();
  const toUs = (ns: number) => Math.round(ns / 100) / 10; // 0.1µs resolution
  let sum = 0;
  for (const v of sorted) sum += v;
  return {
    n: sorted.length,
    p50us: toUs(percentileSorted(sorted, 50)),
    p95us: toUs(percentileSorted(sorted, 95)),
    p99us: toUs(percentileSorted(sorted, 99)),
    maxUs: toUs(sorted[sorted.length - 1]!),
    meanUs: toUs(sum / sorted.length),
  };
}

export function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  const mid = s.length >> 1;
  return s.length % 2 ? s[mid]! : (s[mid - 1]! + s[mid]!) / 2;
}

// ── measurement ──────────────────────────────────────────────────────────---

/**
 * Closed-loop service time at concurrency 1: warmup with the SAME op, then n
 * timed sequential ops into a preallocated buffer.
 */
export async function measureServiceTime(
  n: number,
  warmup: number,
  op: (i: number) => Promise<unknown>,
): Promise<LatencyStats> {
  for (let i = 0; i < warmup; i++) await op(i);
  const samples = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    const t0 = Bun.nanoseconds();
    await op(warmup + i);
    samples[i] = Bun.nanoseconds() - t0;
  }
  return latencyStats(samples);
}

export interface ThroughputResult {
  opsPerSec: number;
  elapsedMs: number;
  total: number;
  depth: number;
}

/**
 * Sliding-window throughput: `depth` workers each run a serial loop over a
 * shared op counter, so a new op is issued the moment one completes. The clock
 * starts after a same-workload warmup and covers first-issue → last-completion.
 */
export async function measureThroughput(
  total: number,
  depth: number,
  op: (i: number) => Promise<unknown>,
  warmup = Math.max(10_000, depth * 20),
): Promise<ThroughputResult> {
  const window = async (count: number, base: number) => {
    let next = 0;
    const worker = async () => {
      for (;;) {
        const i = next++;
        if (i >= count) return;
        await op(base + i);
      }
    };
    await Promise.all(Array.from({ length: Math.min(depth, count) }, worker));
  };
  await window(warmup, -warmup); // negative indices: callers map them to scratch keys
  const t0 = Bun.nanoseconds();
  await window(total, 0);
  const elapsedMs = (Bun.nanoseconds() - t0) / 1e6;
  return { opsPerSec: Math.round(total / (elapsedMs / 1000)), elapsedMs, total, depth };
}

/** Repeat a throughput run; report median ops/sec plus per-repeat spread. */
export async function repeatThroughput(
  times: number,
  run: (rep: number) => Promise<ThroughputResult>,
): Promise<{ opsPerSec: number; repeats: number[]; depth: number; total: number }> {
  const runs: ThroughputResult[] = [];
  for (let r = 0; r < times; r++) {
    runs.push(await run(r));
    await settle();
  }
  return {
    opsPerSec: Math.round(median(runs.map((r) => r.opsPerSec))),
    repeats: runs.map((r) => r.opsPerSec),
    depth: runs[0]!.depth,
    total: runs[0]!.total,
  };
}

/**
 * Open-loop latency: ops are injected at a fixed rate and each sample measures
 * completion − INTENDED send time, so server stalls show up as the queue they
 * would create for a real arrival process (no coordinated omission). The
 * scheduler wakes on ~1ms timers and releases every op whose intended time has
 * passed, which preserves the arrival schedule even though sends burst.
 */
export async function measureOpenLoop(
  ratePerSec: number,
  total: number,
  op: (i: number) => Promise<unknown>,
): Promise<LatencyStats & { achievedRate: number }> {
  const intervalNs = 1e9 / ratePerSec;
  const samples = new Float64Array(total);
  let completed = 0;
  let firstErr: unknown = null;
  let resolveAll!: () => void;
  const all = new Promise<void>((r) => (resolveAll = r));
  const t0 = Bun.nanoseconds();
  for (let i = 0; i < total; i++) {
    const intended = t0 + i * intervalNs;
    // Release only at/after the intended instant — releasing early would record
    // completion − intended below true service time (even negative). Oversleep
    // just produces a burst of past-due sends, which intended-time accounting
    // charges correctly as queueing delay.
    let now = Bun.nanoseconds();
    while (now < intended) {
      await new Promise((r) =>
        setTimeout(r, Math.max(0, Math.floor((intended - now) / 1e6) - 1)),
      );
      now = Bun.nanoseconds();
    }
    void op(i)
      .then(
        () => {
          samples[i] = Bun.nanoseconds() - intended;
        },
        (err) => {
          firstErr ??= err;
          samples[i] = Bun.nanoseconds() - intended;
        },
      )
      .finally(() => {
        if (++completed === total) resolveAll();
      });
  }
  await all;
  if (firstErr !== null) throw firstErr;
  const elapsedNs = Bun.nanoseconds() - t0;
  return {
    ...latencyStats(samples),
    achievedRate: Math.round(total / (elapsedNs / 1e9)),
  };
}

/** GC + short pause between cases so one case's garbage doesn't bill the next. */
export async function settle(ms = 100): Promise<void> {
  Bun.gc(true);
  await new Promise((r) => setTimeout(r, ms));
}

// ── CPU sampling ─────────────────────────────────────────────────────────---

/**
 * Approximate %CPU of bench + server processes, sampled via `ps` during a run.
 * macOS ps reports a decaying average, so treat these as indicative only —
 * their job is to flag client-bound results, not to be precise.
 */
export function startCpuSampler(serverPid: number | null): { stop(): Promise<Record<string, number>> } {
  const pids = [process.pid, ...(serverPid !== null ? [serverPid] : [])];
  const samples: Record<number, number[]> = {};
  for (const p of pids) samples[p] = [];
  let active = true;
  const loop = (async () => {
    while (active) {
      try {
        const out = await Bun.$`ps -o pid=,pcpu= -p ${pids.join(",")}`.quiet().text();
        for (const line of out.trim().split("\n")) {
          const [pid, pcpu] = line.trim().split(/\s+/);
          samples[Number(pid)]?.push(Number(pcpu));
        }
      } catch {
        // process may have exited
      }
      await new Promise((r) => setTimeout(r, 200));
    }
  })();
  return {
    async stop() {
      active = false;
      await loop;
      const avg = (xs: number[]) =>
        xs.length ? Math.round(xs.reduce((a, b) => a + b, 0) / xs.length) : 0;
      return {
        benchCpuPct: avg(samples[process.pid]!),
        serverCpuPct: serverPid !== null ? avg(samples[serverPid]!) : -1,
      };
    },
  };
}

// ── payloads / keys ──────────────────────────────────────────────────────---

const ASCII = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/** Deterministic printable payload of exactly `size` bytes. */
export function asciiPayload(size: number, seed = 7): string {
  let s = "";
  let x = seed;
  while (s.length < size) {
    x = (x * 1103515245 + 12345) & 0x7fffffff;
    s += ASCII[x % ASCII.length];
  }
  return s;
}

/** Deterministic binary payload (full byte range) of exactly `size` bytes. */
export function binaryPayload(size: number, seed = 7): Uint8Array {
  const out = new Uint8Array(size);
  let x = seed;
  for (let i = 0; i < size; i++) {
    x = (x * 1103515245 + 12345) & 0x7fffffff;
    out[i] = x & 0xff;
  }
  return out;
}

export const key = (prefix: string, i: number): string =>
  `${prefix}:${String(i).padStart(7, "0")}`;

/** Precompute a key array so the timed loop does zero string building. */
export function keyArray(prefix: string, n: number): string[] {
  const out = new Array<string>(n);
  for (let i = 0; i < n; i++) out[i] = key(prefix, i);
  return out;
}

/** Deterministic LCG for reproducible workload mixes. */
export function lcg(seed: number): () => number {
  let x = seed;
  return () => (x = (x * 1103515245 + 12345) & 0x7fffffff);
}

/** Preload string keys via windowed SETs (not timed). */
export async function preload(
  client: RedisClient,
  keys: string[],
  value: string,
): Promise<void> {
  await measureThroughput(keys.length, 200, (i) => client.set(keys[i]!, value), 0);
}

// ── result collection ────────────────────────────────────────────────────---

export interface CaseResult {
  id: string;
  name: string;
  params: Record<string, unknown>;
  metrics: Record<string, unknown>;
}

export class Results {
  readonly cases: CaseResult[] = [];

  add(id: string, name: string, params: Record<string, unknown>, metrics: Record<string, unknown>): void {
    this.cases.push({ id, name, params, metrics });
    console.error(`  ✓ ${id} ${name}: ${JSON.stringify(metrics).slice(0, 200)}`);
  }
}
