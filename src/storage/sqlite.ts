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
import type { RedisType, SetOptions, StorageEngine } from "./types";

export interface SqliteStorageOptions {
  /** Hook invoked with a key whenever it is mutated (drives WATCH versioning). */
  readonly onWrite?: (key: Uint8Array) => void;
}

export class SqliteStorage implements StorageEngine {
  #db: Database;
  #onWrite: (key: Uint8Array) => void;

  constructor(path = ":memory:", opts: SqliteStorageOptions = {}) {
    this.#db = new Database(path, { create: true, strict: false });
    this.#onWrite = opts.onWrite ?? (() => {});
    this.#init();
  }

  #init(): void {
    this.#db.exec("PRAGMA journal_mode = WAL;");
    this.#db.exec("PRAGMA synchronous = NORMAL;");
    this.#db.exec("PRAGMA foreign_keys = ON;");
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
    `);
  }

  // ── meta / ttl ──────────────────────────────────────────────────────────--

  /**
   * Resolve a key's live metadata, lazily expiring it first (§5.3.1).
   * Returns null when the key is absent or just expired (and deletes it).
   */
  #meta(key: Uint8Array, now: number): { type: RedisType; expireAtMs: number | null } | null {
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

  #ensureKey(key: Uint8Array, type: RedisType): void {
    this.#stmt(
      "INSERT INTO keys(key, type, expire_at_ms) VALUES (?, ?, NULL) " +
        "ON CONFLICT(key) DO NOTHING",
    ).run(key, type);
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

  sweepExpired(now: number): number {
    return this.withTransaction(() => {
      const rows = this.#stmt(
        "SELECT key FROM keys WHERE expire_at_ms IS NOT NULL AND expire_at_ms <= ?",
      ).all(now) as Array<{ key: Uint8Array }>;
      for (const r of rows) this.#deleteKey(r.key);
      return rows.length;
    });
  }

  dbsize(now: number): number {
    this.sweepExpired(now);
    const row = this.#stmt("SELECT COUNT(*) AS n FROM keys").get() as { n: number };
    return row.n;
  }

  // ── string / kv ─────────────────────────────────────────────────────────--

  kvGet(key: Uint8Array, now: number): Uint8Array | null {
    if (!this.#expectType(key, "string", now)) return null;
    const row = this.#stmt("SELECT value FROM kv WHERE key = ?").get(key) as
      | { value: Uint8Array }
      | null;
    return row ? row.value : null;
  }

  kvSet(
    key: Uint8Array,
    value: Uint8Array,
    now: number,
    opts: SetOptions = {},
  ): "set" | "noop" {
    return this.withTransaction(() => {
      const meta = this.#meta(key, now);
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
      const cur = this.kvGet(key, now);
      let n: bigint;
      if (cur === null) {
        n = 0n;
      } else {
        n = parseIntStrict(cur);
      }
      const next = n + delta;
      const buf = new TextEncoder().encode(next.toString());
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
      const cur = this.kvGet(key, now);
      const n = cur === null ? 0 : parseFloatStrict(cur);
      const next = n + delta;
      if (!Number.isFinite(next)) {
        throw new RespError("ERR", "increment would produce NaN or Infinity");
      }
      const buf = new TextEncoder().encode(formatFloat(next));
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
      this.#expectType(key, "hash", now);
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
    if (!this.#expectType(key, "hash", now)) return null;
    const row = this.#stmt(
      "SELECT value FROM hash_fields WHERE key = ? AND field = ?",
    ).get(key, field) as { value: Uint8Array } | null;
    return row ? row.value : null;
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
    if (!this.#expectType(key, "hash", now)) return false;
    return (
      this.#stmt("SELECT 1 FROM hash_fields WHERE key = ? AND field = ?").get(
        key,
        field,
      ) !== null
    );
  }

  hIncrBy(key: Uint8Array, field: Uint8Array, delta: bigint, now: number): bigint {
    return this.withTransaction(() => {
      const cur = this.hGet(key, field, now);
      const n = cur === null ? 0n : parseIntStrict(cur);
      const next = n + delta;
      this.hSet(key, [[field, new TextEncoder().encode(next.toString())]], now);
      return next;
    });
  }

  hIncrByFloat(key: Uint8Array, field: Uint8Array, delta: number, now: number): number {
    return this.withTransaction(() => {
      const cur = this.hGet(key, field, now);
      const n = cur === null ? 0 : parseFloatStrict(cur);
      const next = n + delta;
      this.hSet(key, [[field, new TextEncoder().encode(formatFloat(next))]], now);
      return next;
    });
  }

  #dropIfEmptyHash(key: Uint8Array): void {
    const row = this.#stmt(
      "SELECT COUNT(*) AS n FROM hash_fields WHERE key = ?",
    ).get(key) as { n: number };
    if (row.n === 0) this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
  }

  // ── set ────────────────────────────────────────────────────────────────---

  sAdd(key: Uint8Array, members: Uint8Array[], now: number): number {
    return this.withTransaction(() => {
      this.#expectType(key, "set", now);
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
    if (!this.#expectType(key, "set", now)) return false;
    return (
      this.#stmt("SELECT 1 FROM set_members WHERE key = ? AND member = ?").get(
        key,
        member,
      ) !== null
    );
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
    const members = this.sMembers(key, now);
    if (count === null) {
      if (members.length === 0) return null;
      return members[Math.floor(Math.random() * members.length)]!;
    }
    if (count >= 0) {
      return shuffle(members).slice(0, count);
    }
    // Negative count: allow repeats, |count| elements.
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
      const members = this.sMembers(key, now);
      if (count === null) {
        if (members.length === 0) return null;
        const pick = members[Math.floor(Math.random() * members.length)]!;
        this.sRem(key, [pick], now);
        return pick;
      }
      const picks = shuffle(members).slice(0, Math.max(0, count));
      if (picks.length > 0) this.sRem(key, picks, now);
      return picks;
    });
  }

  #dropIfEmptySet(key: Uint8Array): void {
    const row = this.#stmt(
      "SELECT COUNT(*) AS n FROM set_members WHERE key = ?",
    ).get(key) as { n: number };
    if (row.n === 0) this.#stmt("DELETE FROM keys WHERE key = ?").run(key);
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
  const s = new TextDecoder().decode(bytes).trim();
  if (!/^[+-]?\d+$/.test(s)) {
    throw new NotIntegerError();
  }
  return BigInt(s);
}

function parseFloatStrict(bytes: Uint8Array): number {
  const s = new TextDecoder().decode(bytes).trim();
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

function shuffle<T>(arr: T[]): T[] {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j]!, a[i]!];
  }
  return a;
}
