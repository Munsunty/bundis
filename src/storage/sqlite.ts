/**
 * L6 SqliteStorage — bun:sqlite implementation of {@link StorageEngine}.
 *
 * Design (CLAUDE.md §5): metadata is unified in `keys` (type + ttl); values are
 * split per type (`kv` / `hash_fields` / `set_members`). All payloads are BLOB
 * for binary safety. WAL mode + single-writer assumption keep atomicity simple.
 * bun:sqlite is synchronous, so read-modify-write under `withTransaction` is a
 * genuine atomic unit.
 */

import { Database } from "bun:sqlite";
import {
  NotFloatError,
  NotIntegerError,
  RespError,
  TypeMismatchError,
} from "../engine/errors";
import type {
  RedisType,
  ScoreBound,
  SetOptions,
  StorageEngine,
  ZAddOptions,
} from "./types";

const ENC = new TextEncoder();
const DEC = new TextDecoder();

/** Max expired keys reclaimed per sweep call (bounds reaper-tick stalls). */
const SWEEP_BATCH = 1000;

/**
 * Zset range/rank SQL, exported so the query-plan regression tests can assert
 * these statements stay on the (key, score, member) index — no full scans, no
 * temp B-tree sorts (the performance contract for ZRANGE/ZRANGEBYSCORE/ZRANK).
 * LIMIT -1 means "no limit" in SQLite, so one shape covers the LIMIT-less case.
 */
export const ZSET_RANGE_SQL = {
  byRankAsc:
    "SELECT member, score FROM zset_members WHERE key = ? " +
    "ORDER BY score, member LIMIT ? OFFSET ?",
  byRankDesc:
    "SELECT member, score FROM zset_members WHERE key = ? " +
    "ORDER BY score DESC, member DESC LIMIT ? OFFSET ?",
  byScore: (minOp: ">" | ">=", maxOp: "<" | "<="): string =>
    "SELECT member, score FROM zset_members WHERE key = ? " +
    `AND score ${minOp} ? AND score ${maxOp} ? ` +
    "ORDER BY score, member LIMIT ? OFFSET ?",
  rank:
    "SELECT COUNT(*) AS n FROM zset_members WHERE key = ? " +
    "AND (score < ? OR (score = ? AND member < ?))",
} as const;

export interface SqliteStorageOptions {
  /** Hook invoked with a key whenever it is mutated (drives WATCH versioning). */
  readonly onWrite?: (key: Uint8Array) => void;
  /** Hook invoked after FLUSHDB/FLUSHALL (per-key hooks can't enumerate). */
  readonly onFlushAll?: () => void;
  /** SQLite page-cache size in KB (PRAGMA cache_size). Default: SQLite's own. */
  readonly pageCacheKb?: number;
}

export class SqliteStorage implements StorageEngine {
  #db: Database;
  #onWrite: (key: Uint8Array) => void;
  #onFlushAll: () => void;

  constructor(path = ":memory:", opts: SqliteStorageOptions = {}) {
    try {
      this.#db = new Database(path, { create: true, strict: false });
      this.#onWrite = opts.onWrite ?? (() => {});
      this.#onFlushAll = opts.onFlushAll ?? (() => {});
      this.#init(opts.pageCacheKb);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/locked|busy/i.test(msg)) {
        throw new Error(
          `database "${path}" is already in use by another bundis process ` +
            "(the single-writer contract forbids sharing one .db file)",
        );
      }
      throw new Error(`cannot open database "${path}": ${msg}`);
    }
  }

  #init(pageCacheKb?: number): void {
    // Enforce the documented single-writer contract (§5.3): EXCLUSIVE locking
    // holds the file lock for the process lifetime, so a second bundis on the
    // same .db fails fast here instead of corrupting cache coherence later.
    // (:memory: is unaffected; WAL+exclusive runs with a heap WAL-index.)
    this.#db.exec("PRAGMA busy_timeout = 2000;");
    this.#db.exec("PRAGMA locking_mode = EXCLUSIVE;");
    this.#db.exec("PRAGMA journal_mode = WAL;");
    this.#db.exec("PRAGMA synchronous = NORMAL;");
    this.#db.exec("PRAGMA foreign_keys = ON;");
    if (pageCacheKb !== undefined && pageCacheKb > 0) {
      // negative value = size in KB (positive would mean pages)
      this.#db.exec(`PRAGMA cache_size = -${Math.floor(pageCacheKb)};`);
    }
    this.#db.exec(`
      CREATE TABLE IF NOT EXISTS keys (
        key          BLOB PRIMARY KEY,
        type         TEXT NOT NULL,
        expire_at_ms INTEGER
      ) WITHOUT ROWID;
      CREATE INDEX IF NOT EXISTS idx_keys_expire
        ON keys(expire_at_ms) WHERE expire_at_ms IS NOT NULL;
      CREATE TABLE IF NOT EXISTS kv (
        key   BLOB PRIMARY KEY REFERENCES keys(key) ON DELETE CASCADE,
        value BLOB NOT NULL
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS hash_fields (
        key   BLOB NOT NULL,
        field BLOB NOT NULL,
        value BLOB NOT NULL,
        PRIMARY KEY (key, field),
        FOREIGN KEY (key) REFERENCES keys(key) ON DELETE CASCADE
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS set_members (
        key    BLOB NOT NULL,
        member BLOB NOT NULL,
        PRIMARY KEY (key, member),
        FOREIGN KEY (key) REFERENCES keys(key) ON DELETE CASCADE
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS list_items (
        key   BLOB NOT NULL,
        seq   INTEGER NOT NULL,
        value BLOB NOT NULL,
        PRIMARY KEY (key, seq),
        FOREIGN KEY (key) REFERENCES keys(key) ON DELETE CASCADE
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS zset_members (
        key    BLOB NOT NULL,
        member BLOB NOT NULL,
        score  REAL NOT NULL,
        PRIMARY KEY (key, member),
        FOREIGN KEY (key) REFERENCES keys(key) ON DELETE CASCADE
      ) WITHOUT ROWID;
      CREATE INDEX IF NOT EXISTS idx_zset_range
        ON zset_members(key, score, member);
    `);
  }

  // ── meta / ttl ──────────────────────────────────────────────────────────--

  /**
   * Resolve a key's live metadata (§5.3.1). An expired key reads as absent but
   * is NOT deleted here: physical removal is owned solely by the reaper
   * ({@link sweepExpired}). Decoupling logical expiry from physical delete keeps
   * the read path write-free, so a GET that lands on an expired key no longer
   * contends with the writer lock (was a p99 spike under TTL churn).
   */
  #meta(key: Uint8Array, now: number): { type: RedisType; expireAtMs: number | null } | null {
    const row = this.#stmt(
      "SELECT type, expire_at_ms FROM keys WHERE key = ?",
    ).get(key) as { type: RedisType; expire_at_ms: number | null } | null;
    if (!row) return null;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) return null;
    return { type: row.type, expireAtMs: row.expire_at_ms };
  }

  /**
   * Write-path revive guard + metadata read in one keys lookup. Because
   * {@link #meta} no longer deletes expired keys, a logically-expired row can
   * still be physically present. Any mutator that may (re)create a key must
   * purge that stale row first, or its `INSERT ... ON CONFLICT DO NOTHING`
   * would keep the old type and resurrect orphaned child rows (WRONGTYPE /
   * data leak). Returns the live metadata (null if missing/just purged), so
   * callers that also need the type/TTL don't pay a second keys lookup.
   */
  #metaPurging(
    key: Uint8Array,
    now: number,
  ): { type: RedisType; expireAtMs: number | null } | null {
    const row = this.#stmt(
      "SELECT type, expire_at_ms FROM keys WHERE key = ?",
    ).get(key) as { type: RedisType; expire_at_ms: number | null } | null;
    if (!row) return null;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) {
      this.#deleteKey(key);
      return null;
    }
    return { type: row.type, expireAtMs: row.expire_at_ms };
  }

  /** {@link #metaPurging} + WRONGTYPE check: the standard mutator preamble. */
  #expectTypePurging(key: Uint8Array, want: RedisType, now: number): boolean {
    const meta = this.#metaPurging(key, now);
    if (!meta) return false;
    if (meta.type !== want) throw new TypeMismatchError(meta.type, want);
    return true;
  }

  /** Like {@link #meta} but throws TypeMismatchError if a live key isn't `want`. */
  #expectType(key: Uint8Array, want: RedisType, now: number): boolean {
    const meta = this.#meta(key, now);
    if (!meta) return false;
    if (meta.type !== want) throw new TypeMismatchError(meta.type, want);
    return true;
  }

  #deleteKey(key: Uint8Array): void {
    // ON DELETE CASCADE removes child rows in kv/hash_fields/set_members.
    this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
    this.#onWrite(key);
  }

  typeOf(key: Uint8Array, now: number): RedisType | null {
    return this.#meta(key, now)?.type ?? null;
  }

  exists(key: Uint8Array, now: number): boolean {
    return this.#meta(key, now) !== null;
  }

  del(keys: Uint8Array[], now: number): number {
    return this.withTransaction(() => {
      let n = 0;
      for (const key of keys) {
        if (this.#meta(key, now) !== null) {
          this.#deleteKey(key);
          n++;
        }
      }
      return n;
    });
  }

  expireSet(key: Uint8Array, atMs: number, now: number): boolean {
    if (this.#meta(key, now) === null) return false;
    this.#stmt("UPDATE keys SET expire_at_ms = ? WHERE key = ?").run(atMs, key);
    this.#onWrite(key);
    return true;
  }

  pttl(key: Uint8Array, now: number): number {
    const meta = this.#meta(key, now);
    if (!meta) return -2;
    if (meta.expireAtMs === null) return -1;
    return Math.max(0, meta.expireAtMs - now);
  }

  persist(key: Uint8Array, now: number): boolean {
    const meta = this.#meta(key, now);
    if (!meta || meta.expireAtMs === null) return false;
    this.#stmt("UPDATE keys SET expire_at_ms = NULL WHERE key = ?").run(key);
    this.#onWrite(key);
    return true;
  }

  /**
   * One bounded sweep batch per call (the reaper ticks every ~100ms, so a
   * burst of expirations is reclaimed incrementally instead of blocking every
   * connection for one giant transaction — measured 380ms p99 before the cap).
   */
  sweepExpired(now: number): number {
    return this.withTransaction(() => {
      const rows = this.#stmt(
        "DELETE FROM keys WHERE key IN (" +
          "SELECT key FROM keys WHERE expire_at_ms IS NOT NULL AND expire_at_ms <= ? " +
          `LIMIT ${SWEEP_BATCH}) RETURNING key`,
      ).all(now) as Array<{ key: Uint8Array }>;
      for (const r of rows) this.#onWrite(r.key);
      return rows.length;
    });
  }

  dbsize(now: number): number {
    // Count live keys without sweeping: INFO/DBSIZE polling must never pay
    // (or trigger) a reclamation pass.
    const row = this.#stmt(
      "SELECT COUNT(*) AS n FROM keys WHERE expire_at_ms IS NULL OR expire_at_ms > ?",
    ).get(now) as { n: number };
    return row.n;
  }

  flushAll(): void {
    this.withTransaction(() => {
      // Children first: value tables reference keys.
      this.#stmt("DELETE FROM kv").run();
      this.#stmt("DELETE FROM hash_fields").run();
      this.#stmt("DELETE FROM set_members").run();
      this.#stmt("DELETE FROM list_items").run();
      this.#stmt("DELETE FROM zset_members").run();
      this.#stmt("DELETE FROM keys").run();
    });
    this.#onFlushAll();
  }

  // ── string / kv ─────────────────────────────────────────────────────────--

  kvGet(key: Uint8Array, now: number): Uint8Array | null {
    return this.kvGetEx(key, now)?.value ?? null;
  }

  /**
   * Value + expiry in a single lookup: type check, lazy expiry, and the kv row
   * all come from one keys⋈kv point query (was two statements per GET — the
   * hottest read path). Also lets the hot cache fill without a pttl read-back.
   */
  kvGetEx(
    key: Uint8Array,
    now: number,
  ): { value: Uint8Array; expireAtMs: number | null } | null {
    const row = this.#stmt(
      "SELECT k.type AS type, k.expire_at_ms AS expire_at_ms, v.value AS value " +
        "FROM keys k LEFT JOIN kv v ON v.key = k.key WHERE k.key = ?",
    ).get(key) as
      | { type: RedisType; expire_at_ms: number | null; value: Uint8Array | null }
      | null;
    if (!row) return null;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) return null;
    if (row.type !== "string") throw new TypeMismatchError(row.type, "string");
    return row.value === null ? null : { value: row.value, expireAtMs: row.expire_at_ms };
  }

  kvSet(
    key: Uint8Array,
    value: Uint8Array,
    now: number,
    opts: SetOptions = {},
  ): "set" | "noop" {
    return this.withTransaction(() => {
      const meta = this.#metaPurging(key, now);
      if (opts.mode === "NX" && meta !== null) return "noop";
      if (opts.mode === "XX" && meta === null) return "noop";
      if (meta !== null && meta.type !== "string") {
        // Overwriting any existing type with a string is allowed in Redis.
        this.#deleteKey(key);
      }
      const expire =
        opts.expireAtMs !== undefined && opts.expireAtMs !== null
          ? opts.expireAtMs
          : opts.keepTtl && meta?.type === "string"
            ? (meta.expireAtMs ?? null)
            : null;
      this.#stmt(
        "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'string', ?) " +
          "ON CONFLICT(key) DO UPDATE SET type='string', expire_at_ms=excluded.expire_at_ms",
      ).run(key, expire);
      this.#stmt(
        "INSERT INTO kv(key, value) VALUES (?, ?) " +
          "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      ).run(key, value);
      this.#onWrite(key);
      return "set";
    });
  }

  incrBy(key: Uint8Array, delta: bigint, now: number): bigint {
    return this.withTransaction(() => {
      this.#metaPurging(key, now);
      const cur = this.kvGet(key, now);
      let n: bigint;
      if (cur === null) {
        n = 0n;
      } else {
        n = parseIntStrict(cur);
      }
      const next = n + delta;
      const buf = ENC.encode(next.toString());
      this.#stmt(
        "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'string', NULL) " +
          "ON CONFLICT(key) DO UPDATE SET type='string'",
      ).run(key);
      this.#stmt(
        "INSERT INTO kv(key, value) VALUES (?, ?) " +
          "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      ).run(key, buf);
      this.#onWrite(key);
      return next;
    });
  }

  incrByFloat(key: Uint8Array, delta: number, now: number): number {
    return this.withTransaction(() => {
      this.#metaPurging(key, now);
      const cur = this.kvGet(key, now);
      const n = cur === null ? 0 : parseFloatStrict(cur);
      const next = n + delta;
      if (!Number.isFinite(next)) {
        throw new RespError("ERR", "increment would produce NaN or Infinity");
      }
      const buf = ENC.encode(formatFloat(next));
      this.#stmt(
        "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'string', NULL) " +
          "ON CONFLICT(key) DO UPDATE SET type='string'",
      ).run(key);
      this.#stmt(
        "INSERT INTO kv(key, value) VALUES (?, ?) " +
          "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      ).run(key, buf);
      this.#onWrite(key);
      return next;
    });
  }

  append(key: Uint8Array, value: Uint8Array, now: number): number {
    return this.withTransaction(() => {
      const cur = this.kvGet(key, now);
      const next =
        cur === null ? value : concatBytes(cur, value);
      this.kvSet(key, next, now, { keepTtl: true });
      return next.length;
    });
  }

  // ── hash ───────────────────────────────────────────────────────────────---

  hSet(
    key: Uint8Array,
    pairs: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
    now: number,
  ): number {
    return this.withTransaction(() => {
      this.#expectTypePurging(key, "hash", now);
      this.#stmt(
        "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'hash', NULL) " +
          "ON CONFLICT(key) DO NOTHING",
      ).run(key);
      let added = 0;
      const exists = this.#stmt("SELECT 1 FROM hash_fields WHERE key = ? AND field = ?");
      const upsert = this.#stmt(
        "INSERT INTO hash_fields(key, field, value) VALUES (?, ?, ?) " +
          "ON CONFLICT(key, field) DO UPDATE SET value=excluded.value",
      );
      for (const [field, value] of pairs) {
        if (!exists.get(key, field)) added++;
        upsert.run(key, field, value);
      }
      this.#onWrite(key);
      return added;
    });
  }

  hGet(key: Uint8Array, field: Uint8Array, now: number): Uint8Array | null {
    // Single keys⋈hash_fields point query: type check, lazy expiry, and the
    // field lookup in one statement (was two).
    const row = this.#stmt(
      "SELECT k.type AS type, k.expire_at_ms AS expire_at_ms, h.value AS value " +
        "FROM keys k LEFT JOIN hash_fields h ON h.key = k.key AND h.field = ? " +
        "WHERE k.key = ?",
    ).get(field, key) as
      | { type: RedisType; expire_at_ms: number | null; value: Uint8Array | null }
      | null;
    if (!row) return null;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) return null;
    if (row.type !== "hash") throw new TypeMismatchError(row.type, "hash");
    return row.value;
  }

  hDel(key: Uint8Array, fields: Uint8Array[], now: number): number {
    return this.withTransaction(() => {
      if (!this.#expectType(key, "hash", now)) return 0;
      const del = this.#stmt("DELETE FROM hash_fields WHERE key = ? AND field = ?");
      let n = 0;
      for (const f of fields) {
        const changes = (del.run(key, f) as { changes: number }).changes;
        n += changes;
      }
      this.#dropIfEmptyHash(key);
      this.#onWrite(key);
      return n;
    });
  }

  hGetAll(key: Uint8Array, now: number): Array<[Uint8Array, Uint8Array]> {
    if (!this.#expectType(key, "hash", now)) return [];
    const rows = this.#stmt(
      "SELECT field, value FROM hash_fields WHERE key = ?",
    ).all(key) as Array<{ field: Uint8Array; value: Uint8Array }>;
    return rows.map((r) => [r.field, r.value]);
  }

  hKeys(key: Uint8Array, now: number): Uint8Array[] {
    if (!this.#expectType(key, "hash", now)) return [];
    return (
      this.#stmt("SELECT field FROM hash_fields WHERE key = ?").all(key) as Array<{
        field: Uint8Array;
      }>
    ).map((r) => r.field);
  }

  hVals(key: Uint8Array, now: number): Uint8Array[] {
    if (!this.#expectType(key, "hash", now)) return [];
    return (
      this.#stmt("SELECT value FROM hash_fields WHERE key = ?").all(key) as Array<{
        value: Uint8Array;
      }>
    ).map((r) => r.value);
  }

  hLen(key: Uint8Array, now: number): number {
    if (!this.#expectType(key, "hash", now)) return 0;
    const row = this.#stmt(
      "SELECT COUNT(*) AS n FROM hash_fields WHERE key = ?",
    ).get(key) as { n: number };
    return row.n;
  }

  hExists(key: Uint8Array, field: Uint8Array, now: number): boolean {
    const row = this.#stmt(
      "SELECT k.type AS type, k.expire_at_ms AS expire_at_ms, h.field AS hit " +
        "FROM keys k LEFT JOIN hash_fields h ON h.key = k.key AND h.field = ? " +
        "WHERE k.key = ?",
    ).get(field, key) as
      | { type: RedisType; expire_at_ms: number | null; hit: Uint8Array | null }
      | null;
    if (!row) return false;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) return false;
    if (row.type !== "hash") throw new TypeMismatchError(row.type, "hash");
    return row.hit !== null;
  }

  hIncrBy(key: Uint8Array, field: Uint8Array, delta: bigint, now: number): bigint {
    return this.withTransaction(() => {
      const cur = this.hGet(key, field, now);
      const n = cur === null ? 0n : parseIntStrict(cur);
      const next = n + delta;
      this.hSet(key, [[field, ENC.encode(next.toString())]], now);
      return next;
    });
  }

  hIncrByFloat(key: Uint8Array, field: Uint8Array, delta: number, now: number): number {
    return this.withTransaction(() => {
      const cur = this.hGet(key, field, now);
      const n = cur === null ? 0 : parseFloatStrict(cur);
      const next = n + delta;
      this.hSet(key, [[field, ENC.encode(formatFloat(next))]], now);
      return next;
    });
  }

  #dropIfEmptyHash(key: Uint8Array): void {
    // Existence probe, not COUNT(*): a 100k-field hash must not pay a full
    // index scan on every HDEL just to learn it is non-empty.
    const any = this.#stmt("SELECT 1 FROM hash_fields WHERE key = ? LIMIT 1").get(key);
    if (any === null) this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
  }

  // ── set ────────────────────────────────────────────────────────────────---

  sAdd(key: Uint8Array, members: Uint8Array[], now: number): number {
    return this.withTransaction(() => {
      this.#expectTypePurging(key, "set", now);
      this.#stmt(
        "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'set', NULL) " +
          "ON CONFLICT(key) DO NOTHING",
      ).run(key);
      const ins = this.#stmt(
        "INSERT INTO set_members(key, member) VALUES (?, ?) " +
          "ON CONFLICT(key, member) DO NOTHING",
      );
      let added = 0;
      for (const m of members) {
        added += (ins.run(key, m) as { changes: number }).changes;
      }
      this.#onWrite(key);
      return added;
    });
  }

  sRem(key: Uint8Array, members: Uint8Array[], now: number): number {
    return this.withTransaction(() => {
      if (!this.#expectType(key, "set", now)) return 0;
      const del = this.#stmt("DELETE FROM set_members WHERE key = ? AND member = ?");
      let n = 0;
      for (const m of members) n += (del.run(key, m) as { changes: number }).changes;
      this.#dropIfEmptySet(key);
      this.#onWrite(key);
      return n;
    });
  }

  sIsMember(key: Uint8Array, member: Uint8Array, now: number): boolean {
    const row = this.#stmt(
      "SELECT k.type AS type, k.expire_at_ms AS expire_at_ms, s.member AS hit " +
        "FROM keys k LEFT JOIN set_members s ON s.key = k.key AND s.member = ? " +
        "WHERE k.key = ?",
    ).get(member, key) as
      | { type: RedisType; expire_at_ms: number | null; hit: Uint8Array | null }
      | null;
    if (!row) return false;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) return false;
    if (row.type !== "set") throw new TypeMismatchError(row.type, "set");
    return row.hit !== null;
  }

  sMembers(key: Uint8Array, now: number): Uint8Array[] {
    if (!this.#expectType(key, "set", now)) return [];
    return (
      this.#stmt("SELECT member FROM set_members WHERE key = ?").all(key) as Array<{
        member: Uint8Array;
      }>
    ).map((r) => r.member);
  }

  sCard(key: Uint8Array, now: number): number {
    if (!this.#expectType(key, "set", now)) return 0;
    const row = this.#stmt(
      "SELECT COUNT(*) AS n FROM set_members WHERE key = ?",
    ).get(key) as { n: number };
    return row.n;
  }

  sRandMember(
    key: Uint8Array,
    count: number | null,
    now: number,
  ): Uint8Array[] | Uint8Array | null {
    if (!this.#expectType(key, "set", now)) {
      return count === null ? null : [];
    }
    // Random selection stays in SQL: materializing all members into JS just to
    // pick a few measured ~5ms per op on a 100k set.
    if (count === null) {
      const row = this.#randomMember(key);
      return row ?? null;
    }
    if (count >= 0) {
      return (
        this.#stmt(
          "SELECT member FROM set_members WHERE key = ? ORDER BY random() LIMIT ?",
        ).all(key, count) as Array<{ member: Uint8Array }>
      ).map((r) => r.member);
    }
    // Negative count: |count| picks WITH repeats — needs independent draws, so
    // the full-load path is genuinely required here.
    const members = this.sMembers(key, now);
    const out: Uint8Array[] = [];
    if (members.length === 0) return out;
    for (let i = 0; i < -count; i++) {
      out.push(members[Math.floor(Math.random() * members.length)]!);
    }
    return out;
  }

  sPop(
    key: Uint8Array,
    count: number | null,
    now: number,
  ): Uint8Array[] | Uint8Array | null {
    return this.withTransaction(() => {
      if (!this.#expectType(key, "set", now)) {
        return count === null ? null : [];
      }
      if (count === null) {
        // Single pop: uniform COUNT+OFFSET pick is ~3x cheaper than
        // ORDER BY random() (no per-row random + top-k sort).
        const member = this.#randomMember(key);
        if (member === undefined) return null;
        this.#stmt("DELETE FROM set_members WHERE key = ? AND member = ?").run(key, member);
        this.#dropIfEmptySet(key);
        this.#onWrite(key);
        return member;
      }
      const k = Math.max(0, count);
      const rows =
        k === 0
          ? []
          : (this.#stmt(
              "DELETE FROM set_members WHERE key = ? AND member IN (" +
                "SELECT member FROM set_members WHERE key = ? ORDER BY random() LIMIT ?" +
                ") RETURNING member",
            ).all(key, key, k) as Array<{ member: Uint8Array }>);
      if (rows.length > 0) {
        this.#dropIfEmptySet(key);
        this.#onWrite(key);
      }
      return rows.map((r) => r.member);
    });
  }

  /** Uniform random member of a set key, or undefined when empty. */
  #randomMember(key: Uint8Array): Uint8Array | undefined {
    const n = (
      this.#stmt("SELECT COUNT(*) AS n FROM set_members WHERE key = ?").get(key) as {
        n: number;
      }
    ).n;
    if (n === 0) return undefined;
    const row = this.#stmt(
      "SELECT member FROM set_members WHERE key = ? LIMIT 1 OFFSET ?",
    ).get(key, Math.floor(Math.random() * n)) as { member: Uint8Array } | null;
    return row?.member;
  }

  #dropIfEmptySet(key: Uint8Array): void {
    const any = this.#stmt("SELECT 1 FROM set_members WHERE key = ? LIMIT 1").get(key);
    if (any === null) this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
  }

  // ── list ───────────────────────────────────────────────────────────────---
  //
  // Invariant: a list's seqs always form a contiguous integer interval
  // [min, max], because the supported ops only push/pop at the two ends.
  // This makes LINDEX a point lookup (seq = min + rank) and LRANGE a pure
  // BETWEEN index-range scan — no OFFSET. Adding LREM/LINSERT/LSET later
  // breaks this invariant and requires renumbering (or a different scheme).

  /**
   * Live seq interval of a list key, or null when it has no rows.
   * Two single-aggregate queries: SQLite's min/max optimization turns each
   * into one index seek, but a combined MIN+MAX query falls back to a full
   * scan of the key's rows (measured 7ms on a 100k list).
   */
  #listBounds(key: Uint8Array): { min: number; max: number } | null {
    const mn = this.#stmt(
      "SELECT MIN(seq) AS v FROM list_items WHERE key = ?",
    ).get(key) as { v: number | null };
    if (mn.v === null) return null;
    const mx = this.#stmt(
      "SELECT MAX(seq) AS v FROM list_items WHERE key = ?",
    ).get(key) as { v: number | null };
    return { min: mn.v, max: mx.v! };
  }

  lPush(key: Uint8Array, values: Uint8Array[], now: number): number {
    return this.#push(key, values, "L", now);
  }

  rPush(key: Uint8Array, values: Uint8Array[], now: number): number {
    return this.#push(key, values, "R", now);
  }

  #push(key: Uint8Array, values: Uint8Array[], side: "L" | "R", now: number): number {
    return this.withTransaction(() => {
      this.#expectTypePurging(key, "list", now);
      this.#stmt(
        "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'list', NULL) " +
          "ON CONFLICT(key) DO NOTHING",
      ).run(key);
      const bounds = this.#listBounds(key);
      let min = bounds ? bounds.min : 0;
      let max = bounds ? bounds.max : -1;
      const ins = this.#stmt("INSERT INTO list_items(key, seq, value) VALUES (?, ?, ?)");
      for (const v of values) {
        if (side === "L") ins.run(key, --min, v);
        else ins.run(key, ++max, v);
      }
      this.#onWrite(key);
      return max - min + 1;
    });
  }

  lPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    return this.#pop(key, count, "L", now);
  }

  rPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    return this.#pop(key, count, "R", now);
  }

  #pop(
    key: Uint8Array,
    count: number | null,
    side: "L" | "R",
    now: number,
  ): Uint8Array[] | Uint8Array | null {
    return this.withTransaction(() => {
      // Missing key pops to null even with a count (unlike SPOP's empty array).
      if (!this.#expectType(key, "list", now)) return null;
      const bounds = this.#listBounds(key);
      if (!bounds) return null; // unreachable: empty lists drop their key row
      const len = bounds.max - bounds.min + 1;
      const k = count === null ? 1 : Math.min(count, len);
      if (k === 0) return [];
      // Pop order: heads ascending from min, tails descending from max.
      const lo = side === "L" ? bounds.min : bounds.max - k + 1;
      const hi = side === "L" ? bounds.min + k - 1 : bounds.max;
      const rows = this.#stmt(
        "SELECT value FROM list_items WHERE key = ? AND seq BETWEEN ? AND ? " +
          `ORDER BY seq ${side === "L" ? "ASC" : "DESC"}`,
      ).all(key, lo, hi) as Array<{ value: Uint8Array }>;
      this.#stmt("DELETE FROM list_items WHERE key = ? AND seq BETWEEN ? AND ?").run(
        key,
        lo,
        hi,
      );
      this.#dropIfEmptyList(key);
      this.#onWrite(key);
      const values = rows.map((r) => r.value);
      return count === null ? values[0]! : values;
    });
  }

  lRange(key: Uint8Array, start: number, stop: number, now: number): Uint8Array[] {
    if (!this.#expectType(key, "list", now)) return [];
    const bounds = this.#listBounds(key);
    if (!bounds) return [];
    const len = bounds.max - bounds.min + 1;
    const s = Math.max(0, start < 0 ? len + start : start);
    const e = Math.min(len - 1, stop < 0 ? len + stop : stop);
    if (s > e) return [];
    return (
      this.#stmt(
        "SELECT value FROM list_items WHERE key = ? AND seq BETWEEN ? AND ? ORDER BY seq",
      ).all(key, bounds.min + s, bounds.min + e) as Array<{ value: Uint8Array }>
    ).map((r) => r.value);
  }

  lLen(key: Uint8Array, now: number): number {
    if (!this.#expectType(key, "list", now)) return 0;
    // Contiguous-seq invariant: length = max - min + 1, two index seeks.
    const bounds = this.#listBounds(key);
    return bounds ? bounds.max - bounds.min + 1 : 0;
  }

  lIndex(key: Uint8Array, index: number, now: number): Uint8Array | null {
    if (!this.#expectType(key, "list", now)) return null;
    const bounds = this.#listBounds(key);
    if (!bounds) return null;
    const len = bounds.max - bounds.min + 1;
    const i = index < 0 ? len + index : index;
    if (i < 0 || i >= len) return null;
    const row = this.#stmt(
      "SELECT value FROM list_items WHERE key = ? AND seq = ?",
    ).get(key, bounds.min + i) as { value: Uint8Array } | null;
    return row ? row.value : null;
  }

  #dropIfEmptyList(key: Uint8Array): void {
    const any = this.#stmt("SELECT 1 FROM list_items WHERE key = ? LIMIT 1").get(key);
    if (any === null) this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
  }

  // ── zset ───────────────────────────────────────────────────────────────---

  zAdd(
    key: Uint8Array,
    entries: ReadonlyArray<readonly [number, Uint8Array]>,
    now: number,
    opts: ZAddOptions = {},
  ): number {
    return this.withTransaction(() => {
      this.#expectTypePurging(key, "zset", now);
      const get = this.#stmt("SELECT score FROM zset_members WHERE key = ? AND member = ?");
      const ins = this.#stmt("INSERT INTO zset_members(key, member, score) VALUES (?, ?, ?)");
      const upd = this.#stmt(
        "UPDATE zset_members SET score = ? WHERE key = ? AND member = ?",
      );
      // XX against a missing key must not create an empty key row, so the
      // keys upsert is deferred until the first member actually lands.
      let keyEnsured = false;
      let added = 0;
      let changed = 0;
      for (const [score, member] of entries) {
        const cur = get.get(key, member) as { score: number } | null;
        if (cur === null) {
          if (opts.mode === "XX") continue;
          if (!keyEnsured) {
            this.#stmt(
              "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'zset', NULL) " +
                "ON CONFLICT(key) DO NOTHING",
            ).run(key);
            keyEnsured = true;
          }
          ins.run(key, member, score);
          added++;
        } else {
          if (opts.mode === "NX") continue;
          if (opts.gt && !(score > cur.score)) continue;
          if (opts.lt && !(score < cur.score)) continue;
          if (score !== cur.score) {
            upd.run(score, key, member);
            changed++;
          }
        }
      }
      this.#onWrite(key);
      return added + (opts.ch ? changed : 0);
    });
  }

  zIncr(
    key: Uint8Array,
    delta: number,
    member: Uint8Array,
    now: number,
    opts: ZAddOptions = {},
  ): number | null {
    return this.withTransaction(() => {
      this.#expectTypePurging(key, "zset", now);
      const cur = this.#stmt(
        "SELECT score FROM zset_members WHERE key = ? AND member = ?",
      ).get(key, member) as { score: number } | null;
      if (cur === null) {
        if (opts.mode === "XX") return null;
        this.#stmt(
          "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, 'zset', NULL) " +
            "ON CONFLICT(key) DO NOTHING",
        ).run(key);
        this.#stmt("INSERT INTO zset_members(key, member, score) VALUES (?, ?, ?)").run(
          key,
          member,
          delta,
        );
        this.#onWrite(key);
        return delta;
      }
      if (opts.mode === "NX") return null;
      const next = cur.score + delta;
      if (Number.isNaN(next)) {
        throw new RespError("ERR", "resulting score is not a number (NaN)");
      }
      if (opts.gt && !(next > cur.score)) return null;
      if (opts.lt && !(next < cur.score)) return null;
      this.#stmt("UPDATE zset_members SET score = ? WHERE key = ? AND member = ?").run(
        next,
        key,
        member,
      );
      this.#onWrite(key);
      return next;
    });
  }

  zScore(key: Uint8Array, member: Uint8Array, now: number): number | null {
    const row = this.#zPoint(key, member, now);
    return row === null ? null : row.score;
  }

  /** Single keys⋈zset_members point query shared by zScore/zRank. */
  #zPoint(key: Uint8Array, member: Uint8Array, now: number): { score: number } | null {
    const row = this.#stmt(
      "SELECT k.type AS type, k.expire_at_ms AS expire_at_ms, z.score AS score " +
        "FROM keys k LEFT JOIN zset_members z ON z.key = k.key AND z.member = ? " +
        "WHERE k.key = ?",
    ).get(member, key) as
      | { type: RedisType; expire_at_ms: number | null; score: number | null }
      | null;
    if (!row) return null;
    if (row.expire_at_ms !== null && row.expire_at_ms <= now) return null;
    if (row.type !== "zset") throw new TypeMismatchError(row.type, "zset");
    return row.score === null ? null : { score: row.score };
  }

  zCard(key: Uint8Array, now: number): number {
    if (!this.#expectType(key, "zset", now)) return 0;
    const row = this.#stmt(
      "SELECT COUNT(*) AS n FROM zset_members WHERE key = ?",
    ).get(key) as { n: number };
    return row.n;
  }

  zRem(key: Uint8Array, members: Uint8Array[], now: number): number {
    return this.withTransaction(() => {
      if (!this.#expectType(key, "zset", now)) return 0;
      const del = this.#stmt("DELETE FROM zset_members WHERE key = ? AND member = ?");
      let n = 0;
      for (const m of members) n += (del.run(key, m) as { changes: number }).changes;
      this.#dropIfEmptyZset(key);
      this.#onWrite(key);
      return n;
    });
  }

  zRank(key: Uint8Array, member: Uint8Array, now: number): number | null {
    const cur = this.#zPoint(key, member, now);
    if (cur === null) return null;
    // Count predecessors via an index range scan — O(rank), the best SQLite
    // offers without an auxiliary order-statistic structure.
    const row = this.#stmt(ZSET_RANGE_SQL.rank).get(key, cur.score, cur.score, member) as {
      n: number;
    };
    return row.n;
  }

  zRangeByRank(
    key: Uint8Array,
    start: number,
    stop: number,
    rev: boolean,
    now: number,
  ): Array<[Uint8Array, number]> {
    if (!this.#expectType(key, "zset", now)) return [];
    let s: number;
    let e: number;
    if (start >= 0 && stop >= 0) {
      // Non-negative ranges never need the cardinality: LIMIT past the end
      // just returns fewer rows. Skipping zCard avoids an O(n) COUNT scan.
      s = start;
      e = stop;
    } else {
      const len = this.zCard(key, now);
      s = Math.max(0, start < 0 ? len + start : start);
      e = Math.min(len - 1, stop < 0 ? len + stop : stop);
    }
    if (s > e) return [];
    // LIMIT/OFFSET here skips entries inside the (key, score, member) index
    // run — same O(start + n) class as the ZRANK count-scan, no row
    // materialization and no sort (guarded by the query-plan test).
    const rows = this.#stmt(
      rev ? ZSET_RANGE_SQL.byRankDesc : ZSET_RANGE_SQL.byRankAsc,
    ).all(key, e - s + 1, s) as Array<{ member: Uint8Array; score: number }>;
    return rows.map((r) => [r.member, r.score]);
  }

  zRangeByScore(
    key: Uint8Array,
    min: ScoreBound,
    max: ScoreBound,
    limit: { offset: number; count: number } | null,
    now: number,
  ): Array<[Uint8Array, number]> {
    if (!this.#expectType(key, "zset", now)) return [];
    // ±Infinity binds as a REAL and compares correctly, so unbounded sides
    // need no special-casing. count < 0 (and the no-LIMIT case) maps to
    // SQLite's LIMIT -1 = unlimited.
    const sql = ZSET_RANGE_SQL.byScore(min.exclusive ? ">" : ">=", max.exclusive ? "<" : "<=");
    const count = limit === null || limit.count < 0 ? -1 : limit.count;
    const offset = limit === null ? 0 : limit.offset;
    const rows = this.#stmt(sql).all(key, min.value, max.value, count, offset) as Array<{
      member: Uint8Array;
      score: number;
    }>;
    return rows.map((r) => [r.member, r.score]);
  }

  #dropIfEmptyZset(key: Uint8Array): void {
    const any = this.#stmt("SELECT 1 FROM zset_members WHERE key = ? LIMIT 1").get(key);
    if (any === null) this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
  }

  // ── atomicity / lifecycle ───────────────────────────────────────────────--

  withTransaction<T>(fn: () => T): T {
    // bun:sqlite's Database.transaction wraps in BEGIN/COMMIT/ROLLBACK and is
    // re-entrant (nested calls become SAVEPOINTs), which we rely on since many
    // primitives call each other.
    return this.#db.transaction(fn)();
  }

  close(): void {
    this.#db.close();
  }

  /**
   * Test-only: EXPLAIN QUERY PLAN detail lines for `sql` (the performance
   * regression guard asserts index usage without flaky timing measurements).
   */
  explainQueryPlan(sql: string): string[] {
    // Bind NULL for every placeholder: values never change the plan shape.
    const params = (sql.match(/\?/g) ?? []).map(() => null);
    const rows = this.#db.query(`EXPLAIN QUERY PLAN ${sql}`).all(...params) as Array<{
      detail: string;
    }>;
    return rows.map((r) => r.detail);
  }

  // ── statement cache ─────────────────────────────────────────────────────--

  #stmtCache = new Map<string, ReturnType<Database["query"]>>();
  #stmt(sql: string): ReturnType<Database["query"]> {
    let s = this.#stmtCache.get(sql);
    if (!s) {
      s = this.#db.query(sql);
      this.#stmtCache.set(sql, s);
    }
    return s;
  }
}

// ── helpers ─────────────────────────────────────────────────────────────────

function parseIntStrict(bytes: Uint8Array): bigint {
  const s = DEC.decode(bytes).trim();
  if (!/^[+-]?\d+$/.test(s)) {
    throw new NotIntegerError();
  }
  return BigInt(s);
}

function parseFloatStrict(bytes: Uint8Array): number {
  const s = DEC.decode(bytes).trim();
  if (s.length === 0) throw new NotFloatError();
  const lower = s.toLowerCase();
  if (lower === "inf" || lower === "+inf") return Infinity;
  if (lower === "-inf") return -Infinity;
  const n = Number(s);
  if (Number.isNaN(n)) throw new NotFloatError();
  return n;
}

function formatFloat(n: number): string {
  if (n === Infinity) return "inf";
  if (n === -Infinity) return "-inf";
  // Trim trailing zeros the way Redis does for INCRBYFLOAT output.
  let s = n.toPrecision(17);
  if (s.includes(".") && !s.includes("e") && !s.includes("E")) {
    s = s.replace(/0+$/, "").replace(/\.$/, "");
  }
  return Number(s).toString();
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}
