/**
 * Maintenance controller (worker mode) — runtime-tunable data cleanup without
 * stalling the event loop.
 *
 * A cycle:
 *   NORMAL ──trigger──▶ (evict: tombstone victims, bounded) ──▶ IDLE
 *     IDLE:     writes buffer in OverlayStorage, reads go to SQLite (WAL),
 *               the worker thread reclaims expired/tombstoned rows,
 *               vacuums free pages and checkpoints the WAL
 *   ──worker done──▶ FLUSHING: buffer applied in `flushBudgetMs` slices,
 *               new writes keep buffering until the buffer drains
 *   ──buffer empty──▶ NORMAL
 *
 * Triggers: periodic (`maint-interval-sec`), expired backlog
 * (`maint-expired-threshold`), disk size (`maint-maxdisk-mb`), key count
 * (`maint-maxkeys`), or MAINT RUN. Every parameter is adjustable at runtime
 * with CONFIG SET and visible in CONFIG GET / INFO maintenance.
 *
 * Stability guards:
 *   - buffer over `maint-buffer-max-mb` → the worker is told to stop at its
 *     next chunk, so the buffer stops growing for long
 *   - `maint-max-run-ms` caps every worker job; leftover work resumes next cycle
 *   - eviction marks at most `maint-evict-batch` keys per cycle (O(batch))
 *   - a failed flush slice keeps its entries and retries with backoff
 *   - shutdown aborts the worker, waits for it to release the database, then
 *     drains the buffer synchronously (a clean stop never loses writes)
 */

import type { OverlayStorage } from "../storage/overlay";
import type { SqliteStorage } from "../storage/sqlite";
import type { MaintJob, MaintResult } from "./maint-worker";

export type EvictionPolicy = "noeviction" | "volatile-ttl";
export type MaintState = "normal" | "idle" | "flushing";

export interface MaintParams {
  enabled: boolean;
  intervalSec: number;
  expiredThreshold: number;
  maxRunMs: number;
  chunkRows: number;
  bufferMaxMb: number;
  flushBudgetMs: number;
  vacuumStepPages: number;
  maxKeys: number;
  maxDiskMb: number;
  evictionPolicy: EvictionPolicy;
  evictBatch: number;
}

export const DEFAULT_MAINT_PARAMS: Readonly<MaintParams> = {
  enabled: true,
  intervalSec: 60,
  expiredThreshold: 10_000,
  maxRunMs: 5_000,
  chunkRows: 2_000,
  bufferMaxMb: 64,
  flushBudgetMs: 5,
  vacuumStepPages: 256,
  maxKeys: 0,
  maxDiskMb: 0,
  evictionPolicy: "noeviction",
  evictBatch: 1_000,
};

interface ParamSpec {
  readonly key: keyof MaintParams;
  parse(raw: string): MaintParams[keyof MaintParams];
  format(v: MaintParams[keyof MaintParams]): string;
}

class ParamError extends Error {}

const intIn = (key: keyof MaintParams, min: number, max: number): ParamSpec => ({
  key,
  parse(raw) {
    const s = raw.trim();
    if (!/^\d+$/.test(s)) throw new ParamError(`argument must be an integer between ${min} and ${max}`);
    const n = Number(s);
    if (n < min || n > max) throw new ParamError(`argument must be between ${min} and ${max} inclusive`);
    return n;
  },
  format: (v) => String(v),
});

/** CONFIG parameter name → spec. The single list of runtime knobs. */
export const MAINT_PARAM_SPECS: Readonly<Record<string, ParamSpec>> = {
  "maint-enabled": {
    key: "enabled",
    parse(raw) {
      const s = raw.trim().toLowerCase();
      if (s === "yes" || s === "1" || s === "true") return true;
      if (s === "no" || s === "0" || s === "false") return false;
      throw new ParamError("argument must be 'yes' or 'no'");
    },
    format: (v) => (v ? "yes" : "no"),
  },
  "maint-interval-sec": intIn("intervalSec", 0, 86_400),
  "maint-expired-threshold": intIn("expiredThreshold", 0, 1_000_000_000),
  "maint-max-run-ms": intIn("maxRunMs", 10, 600_000),
  "maint-chunk-rows": intIn("chunkRows", 10, 1_000_000),
  "maint-buffer-max-mb": intIn("bufferMaxMb", 1, 65_536),
  "maint-flush-budget-ms": intIn("flushBudgetMs", 1, 1_000),
  "maint-vacuum-step-pages": intIn("vacuumStepPages", 0, 1_000_000),
  "maint-maxkeys": intIn("maxKeys", 0, Number.MAX_SAFE_INTEGER),
  "maint-maxdisk-mb": intIn("maxDiskMb", 0, 1_000_000_000),
  "maint-eviction-policy": {
    key: "evictionPolicy",
    parse(raw) {
      const s = raw.trim().toLowerCase();
      if (s === "noeviction" || s === "volatile-ttl") return s;
      throw new ParamError("argument must be 'noeviction' or 'volatile-ttl'");
    },
    format: (v) => String(v),
  },
  "maint-evict-batch": intIn("evictBatch", 1, 1_000_000),
};

/** Validate name/value pairs; returns parsed updates or an error message. */
export function parseMaintParams(
  pairs: ReadonlyArray<readonly [string, string]>,
): { ok: Partial<MaintParams> } | { error: string } {
  const out: Partial<MaintParams> = {};
  for (const [name, raw] of pairs) {
    const spec = MAINT_PARAM_SPECS[name.toLowerCase()];
    if (!spec) return { error: `Unknown option or number of arguments for CONFIG SET - '${name}'` };
    try {
      (out as Record<string, unknown>)[spec.key] = spec.parse(raw);
    } catch (err) {
      const msg = err instanceof ParamError ? err.message : String(err);
      return { error: `CONFIG SET failed (possibly related to argument '${name}') - ${msg}` };
    }
  }
  return { ok: out };
}

const TICK_MS = 1_000;
const MAXKEYS_CHECK_MS = 10_000;
const BACKLOG_STATUS_CAP = 100_000;
const ABORT = 0;
const BUSY = 1;

export class Maintenance {
  readonly params: MaintParams;
  #state: MaintState = "normal";
  #worker: Worker | null = null;
  readonly #control = new SharedArrayBuffer(8);
  readonly #ctl = new Int32Array(this.#control);
  #tickTimer: ReturnType<typeof setInterval> | null = null;
  #flushTimer: ReturnType<typeof setTimeout> | null = null;
  #lastRunEnd = Date.now();
  #lastMaxKeysCheck = 0;
  #cycleStart = 0;
  #closed = false;

  // stats
  #cycles = 0;
  #lastReason = "";
  #lastWorker: MaintResult | null = null;
  #lastCycleMs = 0;
  #keysReclaimed = 0;
  #rowsReclaimed = 0;
  #pagesVacuumed = 0;
  #evicted = 0;
  #workerMaxTxnMs = 0;
  #flushSteps = 0;
  #flushMaxStepMs = 0;
  #pressureAborts = 0;
  #lastError = "";

  constructor(
    private readonly sqlite: SqliteStorage,
    private readonly overlay: OverlayStorage,
    initial: Record<string, string> = {},
  ) {
    this.params = { ...DEFAULT_MAINT_PARAMS };
    const parsed = parseMaintParams(Object.entries(initial));
    if ("error" in parsed) throw new Error(`invalid maintenance option: ${parsed.error}`);
    this.apply(parsed.ok);
  }

  get state(): MaintState {
    return this.#state;
  }

  /** Begin the trigger loop and the worker thread. */
  start(): void {
    this.#worker = new Worker(new URL("./maint-worker.ts", import.meta.url).href);
    this.#worker.onmessage = (ev: MessageEvent<MaintResult>) => this.#onWorkerDone(ev.data);
    this.#worker.onerror = (ev) => {
      this.#lastError = `worker error: ${(ev as ErrorEvent).message ?? String(ev)}`;
      console.error("bundis: maintenance", this.#lastError);
      if (this.#state === "idle") this.#onWorkerDone(null);
    };
    this.#worker.unref();
    this.#tickTimer = setInterval(() => this.#tick(), TICK_MS);
    this.#tickTimer.unref?.();
  }

  /** Apply already-validated parameter updates. */
  apply(updates: Partial<MaintParams>): void {
    Object.assign(this.params, updates);
    this.overlay.maxBytes = this.params.bufferMaxMb * 1024 * 1024;
  }

  /** CONFIG GET view: parameter name → formatted value. */
  configValues(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [name, spec] of Object.entries(MAINT_PARAM_SPECS)) {
      out[name] = spec.format(this.params[spec.key]);
    }
    return out;
  }

  /** MAINT RUN: start a cycle now. Returns an error message when it cannot. */
  runNow(): string | null {
    if (this.#closed) return "maintenance is shut down";
    if (this.#state !== "normal") return `a maintenance cycle is already running (${this.#state})`;
    // Deferred: never begin inside a command dispatch (which may sit inside a
    // group-commit transaction on the main connection).
    setTimeout(() => {
      if (this.#state === "normal" && !this.#closed) this.#begin("manual");
    }, 0);
    return null;
  }

  /** Buffer grew past maint-buffer-max-mb: stop the worker at its next chunk. */
  onPressure(): void {
    if (this.#state === "idle" && Atomics.load(this.#ctl, ABORT) === 0) {
      Atomics.store(this.#ctl, ABORT, 1);
      this.#pressureAborts++;
    }
  }

  status(now = Date.now()): Record<string, string | number> {
    const disk = this.sqlite.diskStats();
    const ov = this.overlay.overlayStats();
    return {
      maint_mode: "worker",
      maint_enabled: this.params.enabled ? 1 : 0,
      maint_state: this.#state,
      maint_cycles: this.#cycles,
      maint_last_reason: this.#lastReason,
      maint_last_cycle_ms: Math.round(this.#lastCycleMs),
      maint_last_worker_ms: Math.round(this.#lastWorker?.ms ?? 0),
      maint_last_more: this.#lastWorker?.more ? 1 : 0,
      maint_keys_reclaimed: this.#keysReclaimed,
      maint_rows_reclaimed: this.#rowsReclaimed,
      maint_pages_vacuumed: this.#pagesVacuumed,
      maint_evicted_keys: this.#evicted,
      maint_worker_max_txn_ms: round2(this.#workerMaxTxnMs),
      maint_flush_steps: this.#flushSteps,
      maint_flush_max_step_ms: round2(this.#flushMaxStepMs),
      maint_flushed_entries: ov.flushedEntries,
      maint_buffer_entries: ov.entries,
      maint_buffer_bytes: ov.bytes,
      maint_buffer_peak_bytes: ov.peakBytes,
      maint_pressure_aborts: this.#pressureAborts,
      maint_expired_backlog: this.sqlite.expiredBacklog(now, BACKLOG_STATUS_CAP),
      disk_page_size: disk.pageSize,
      disk_pages: disk.pageCount,
      disk_free_pages: disk.freelistCount,
      disk_live_bytes: (disk.pageCount - disk.freelistCount) * disk.pageSize,
      disk_auto_vacuum: disk.autoVacuum === 2 ? "incremental" : disk.autoVacuum === 1 ? "full" : "none",
      maint_last_error: this.#lastError,
    };
  }

  /**
   * Stop triggers, abort a running worker, wait (bounded) until it released
   * the database, then drain the buffer synchronously.
   */
  shutdown(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#tickTimer) clearInterval(this.#tickTimer);
    if (this.#flushTimer) clearTimeout(this.#flushTimer);
    if (this.#state === "idle") {
      Atomics.store(this.#ctl, ABORT, 1);
      Atomics.wait(this.#ctl, BUSY, 1, 10_000);
    }
    for (let attempt = 0; this.overlay.active && attempt < 100; attempt++) {
      try {
        this.overlay.flushStep(Infinity, Date.now());
      } catch (err) {
        console.error("bundis: maintenance: final flush failed:", err);
      }
    }
    this.#worker?.terminate();
    this.#worker = null;
    this.#state = "normal";
  }

  // ── cycle ──────────────────────────────────────────────────────────────--

  #tick(): void {
    if (this.#closed || !this.params.enabled || this.#state !== "normal") return;
    const now = Date.now();
    const p = this.params;
    let reason: string | null = null;
    try {
      if (p.intervalSec > 0 && now - this.#lastRunEnd >= p.intervalSec * 1000) reason = "interval";
      else if (p.expiredThreshold > 0 && this.sqlite.expiredBacklog(now, p.expiredThreshold) >= p.expiredThreshold)
        reason = "expired";
      else if (p.maxDiskMb > 0 && this.#liveBytes() > p.maxDiskMb * 1024 * 1024) reason = "disk";
      else if (p.maxKeys > 0 && now - this.#lastMaxKeysCheck >= MAXKEYS_CHECK_MS) {
        this.#lastMaxKeysCheck = now;
        if (this.sqlite.liveKeysUpTo(now, p.maxKeys + 1) > p.maxKeys) reason = "maxkeys";
      }
    } catch (err) {
      this.#lastError = `trigger check failed: ${String(err)}`;
      return;
    }
    if (reason) this.#begin(reason);
  }

  #begin(reason: string): void {
    const now = Date.now();
    this.#cycleStart = performance.now();
    this.#cycles++;
    this.#lastReason = reason;
    try {
      this.#evict(now);
    } catch (err) {
      this.#lastError = `eviction failed: ${String(err)}`;
    }
    if (!this.#worker) {
      this.#lastError = "worker not started";
      return;
    }
    const job: MaintJob = {
      path: this.sqlite.path,
      cutoff: Date.now(),
      chunkRows: this.params.chunkRows,
      maxMs: this.params.maxRunMs,
      vacuumPages: this.params.vacuumStepPages,
      control: this.#control,
    };
    Atomics.store(this.#ctl, ABORT, 0);
    Atomics.store(this.#ctl, BUSY, 1);
    // From here on the main thread must not write to SQLite.
    this.overlay.activate();
    this.#state = "idle";
    this.#worker.postMessage(job);
  }

  /** Tombstone up to evict-batch keys when over maxkeys / maxdisk (volatile-ttl). */
  #evict(now: number): void {
    const p = this.params;
    if (p.evictionPolicy !== "volatile-ttl") return;
    let n = 0;
    if (p.maxKeys > 0) {
      const live = this.sqlite.liveKeysUpTo(now, p.maxKeys + p.evictBatch);
      if (live > p.maxKeys) n = Math.min(live - p.maxKeys, p.evictBatch);
    }
    if (n === 0 && p.maxDiskMb > 0 && this.#liveBytes() > p.maxDiskMb * 1024 * 1024) n = p.evictBatch;
    if (n > 0) this.#evicted += this.sqlite.evictVolatileTtl(n, now);
  }

  #onWorkerDone(res: MaintResult | null): void {
    if (this.#closed || this.#state !== "idle") return;
    if (res) {
      this.#lastWorker = res;
      this.#keysReclaimed += res.keysReclaimed;
      this.#rowsReclaimed += res.rowsReclaimed;
      this.#pagesVacuumed += res.pagesVacuumed;
      if (res.maxTxnMs > this.#workerMaxTxnMs) this.#workerMaxTxnMs = res.maxTxnMs;
      if (res.error) {
        this.#lastError = `worker: ${res.error}`;
        console.error("bundis: maintenance worker:", res.error);
      }
    }
    this.#state = "flushing";
    this.#flushLoop();
  }

  #flushLoop = (): void => {
    this.#flushTimer = null;
    if (this.#closed) return;
    const t = performance.now();
    try {
      this.overlay.flushStep(this.params.flushBudgetMs, Date.now());
    } catch (err) {
      this.#lastError = `flush failed: ${String(err)}`;
      console.error("bundis: maintenance flush failed (will retry):", err);
      this.#flushTimer = setTimeout(this.#flushLoop, 100);
      return;
    }
    const d = performance.now() - t;
    this.#flushSteps++;
    if (d > this.#flushMaxStepMs) this.#flushMaxStepMs = d;
    if (this.overlay.active) {
      // Yield to the event loop between slices so commands keep flowing.
      this.#flushTimer = setTimeout(this.#flushLoop, 0);
      return;
    }
    this.#state = "normal";
    this.#lastRunEnd = Date.now();
    this.#lastCycleMs = performance.now() - this.#cycleStart;
  };

  #liveBytes(): number {
    const d = this.sqlite.diskStats();
    return (d.pageCount - d.freelistCount) * d.pageSize;
  }
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}
