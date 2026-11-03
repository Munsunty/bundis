/**
 * Maintenance worker — runs on its own thread with its own SQLite connection.
 *
 * One job = one maintenance cycle. While it runs, the main thread buffers all
 * writes in memory (OverlayStorage) and only reads from SQLite (WAL allows
 * that concurrently), so this thread is the only writer and nothing on the
 * main event loop waits for its locks.
 *
 * Work per job, each step in its own short transaction:
 *   1. reclaim expired / tombstoned keys (expire_at_ms <= cutoff): child rows
 *      first in `chunkRows` slices, then the key row
 *   2. incremental_vacuum (only if the file uses auto_vacuum = INCREMENTAL)
 *   3. WAL checkpoint (PASSIVE — never waits on the main connection's readers)
 *
 * The job stops early when the main thread raises the abort flag (buffer
 * pressure / shutdown) or `maxMs` elapses; the next cycle resumes where it
 * left off (a half-reclaimed key is still expired, so it never reads back).
 *
 * Shared control block (Int32Array over a SharedArrayBuffer):
 *   [0] abort flag (main → worker)
 *   [1] busy flag  (1 while a job holds a connection; worker clears it and
 *       notifies after closing the database, so shutdown can Atomics.wait)
 */

import { Database } from "bun:sqlite";

declare const self: Worker;

export interface MaintJob {
  readonly path: string;
  readonly cutoff: number;
  readonly chunkRows: number;
  readonly maxMs: number;
  readonly vacuumPages: number;
  readonly control: SharedArrayBuffer;
}

export interface MaintResult {
  keysReclaimed: number;
  rowsReclaimed: number;
  pagesVacuumed: number;
  /** Longest single write transaction on the worker (ms). */
  maxTxnMs: number;
  ms: number;
  /** Expired keys remain (stopped by abort/time budget). */
  more: boolean;
  aborted: boolean;
  error: string | null;
}

const CHILD: Record<string, { table: string; col: string }> = {
  string: { table: "kv", col: "key" },
  hash: { table: "hash_fields", col: "field" },
  set: { table: "set_members", col: "member" },
  list: { table: "list_items", col: "seq" },
  zset: { table: "zset_members", col: "member" },
};

const ABORT = 0;
const BUSY = 1;

export function runJob(job: MaintJob): MaintResult {
  const ctl = new Int32Array(job.control);
  const start = performance.now();
  const res: MaintResult = {
    keysReclaimed: 0,
    rowsReclaimed: 0,
    pagesVacuumed: 0,
    maxTxnMs: 0,
    ms: 0,
    more: false,
    aborted: false,
    error: null,
  };
  const stop = (): boolean => {
    if (Atomics.load(ctl, ABORT) !== 0) {
      res.aborted = true;
      return true;
    }
    return performance.now() - start >= job.maxMs;
  };
  const timed = <T>(fn: () => T): T => {
    const t = performance.now();
    try {
      return fn();
    } finally {
      const d = performance.now() - t;
      if (d > res.maxTxnMs) res.maxTxnMs = d;
    }
  };

  let db: Database | null = null;
  try {
    db = new Database(job.path);
    db.exec("PRAGMA busy_timeout = 5000;");
    db.exec("PRAGMA foreign_keys = ON;");
    db.exec("PRAGMA journal_size_limit = 67108864;");

    // 1. Reclaim expired keys.
    const pick = db.query(
      "SELECT key, type FROM keys WHERE expire_at_ms IS NOT NULL AND expire_at_ms <= ? LIMIT 256",
    );
    const delKey = db.query(
      "DELETE FROM keys WHERE key = ? AND expire_at_ms IS NOT NULL AND expire_at_ms <= ?",
    );
    // One bounded slice of a key's child rows; returns rows removed.
    const delChild: Record<string, (key: Uint8Array) => number> = {};
    for (const [type, { table, col }] of Object.entries(CHILD)) {
      const stmt =
        type === "string"
          ? db.query("DELETE FROM kv WHERE key = ?")
          : db.query(
              `DELETE FROM ${table} WHERE key = ?1 AND ${col} IN ` +
                `(SELECT ${col} FROM ${table} WHERE key = ?1 LIMIT ?2)`,
            );
      delChild[type] =
        type === "string"
          ? (key) => (stmt.run(key) as { changes: number }).changes
          : (key) => (stmt.run(key, job.chunkRows) as { changes: number }).changes;
    }
    outer: for (;;) {
      if (stop()) {
        res.more = true;
        break;
      }
      const rows = pick.all(job.cutoff) as Array<{ key: Uint8Array; type: string }>;
      if (rows.length === 0) break;
      for (const { key, type } of rows) {
        const child = delChild[type];
        if (child) {
          for (;;) {
            const n = timed(() => child(key));
            res.rowsReclaimed += n;
            if (n < job.chunkRows || type === "string") break;
            if (stop()) {
              res.more = true;
              break outer;
            }
          }
        }
        res.keysReclaimed += timed(() => (delKey.run(key, job.cutoff) as { changes: number }).changes);
      }
    }

    // 2. Return free pages to the filesystem (incremental mode only).
    const autoVacuum = Number(Object.values(db.query("PRAGMA auto_vacuum").get() as object)[0]);
    if (job.vacuumPages > 0 && autoVacuum === 2) {
      const freelist = db.query("PRAGMA freelist_count");
      for (;;) {
        if (stop()) break;
        const free = Number(Object.values(freelist.get() as object)[0]);
        if (free === 0) break;
        const n = Math.min(free, job.vacuumPages);
        timed(() => db!.exec(`PRAGMA incremental_vacuum(${n})`));
        res.pagesVacuumed += n;
      }
    }

    // 3. Move committed WAL frames into the main file without blocking readers.
    timed(() => db!.query("PRAGMA wal_checkpoint(PASSIVE)").get());
  } catch (err) {
    res.error = err instanceof Error ? err.message : String(err);
  } finally {
    try {
      db?.close();
    } catch {
      // ignore
    }
    res.ms = performance.now() - start;
    Atomics.store(ctl, BUSY, 0);
    Atomics.notify(ctl, BUSY);
  }
  return res;
}

// Worker entry (no-op when imported on the main thread, e.g. by tests).
if (!Bun.isMainThread) {
  self.onmessage = (ev: MessageEvent<MaintJob>) => {
    self.postMessage(runJob(ev.data));
  };
}
