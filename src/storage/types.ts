/**
 * L6 storage abstraction (the storage SSOT, §3.2).
 *
 * The command engine depends only on this interface — never on SQLite directly.
 * It exposes storage *primitives* (kv / field-map / member-set / ttl), not Redis
 * commands, so a different backend or an in-memory mock could be swapped in.
 *
 * Binary safety: keys, values, fields and members are all raw byte arrays.
 * Methods perform lazy expiry internally (an expired key reads as absent and is
 * removed). Type collisions throw {@link ../engine/errors.TypeMismatchError}.
 */

export type RedisType = "string" | "hash" | "set" | "list" | "zset";

export interface SetOptions {
  /** Absolute expiry epoch-ms to assign, or null to leave/clear per `keepTtl`. */
  readonly expireAtMs?: number | null;
  /** NX: only set if key does not exist. XX: only if it exists. */
  readonly mode?: "NX" | "XX";
  /** Preserve an existing TTL instead of clearing it. */
  readonly keepTtl?: boolean;
}

export interface StorageEngine {
  // ── meta / ttl ────────────────────────────────────────────────────────────
  /** Stored type of a live key, or null if missing/expired. */
  typeOf(key: Uint8Array, now: number): RedisType | null;
  exists(key: Uint8Array, now: number): boolean;
  /** Delete keys; returns how many existed. */
  del(keys: Uint8Array[], now: number): number;
  /** Set absolute expiry; returns false if key missing. */
  expireSet(key: Uint8Array, atMs: number, now: number): boolean;
  /** Remaining ms: -2 missing, -1 no expiry, else > 0. */
  pttl(key: Uint8Array, now: number): number;
  /** Remove expiry; returns true if a TTL was removed. */
  persist(key: Uint8Array, now: number): boolean;
  /** Active sweep of expired rows; returns rows removed. */
  sweepExpired(now: number): number;
  /** Total live key count (after lazy considerations are out of scope here). */
  dbsize(now: number): number;

  // ── string / kv ─────────────────────────────────────────────────────────--
  kvGet(key: Uint8Array, now: number): Uint8Array | null;
  /** Returns the value actually stored, or null when an NX/XX guard blocked it. */
  kvSet(key: Uint8Array, value: Uint8Array, now: number, opts?: SetOptions): "set" | "noop";
  /** Atomic add of `delta`; returns new value. Throws notInt on non-integer. */
  incrBy(key: Uint8Array, delta: bigint, now: number): bigint;
  /** Atomic float add; returns new value formatted by caller. */
  incrByFloat(key: Uint8Array, delta: number, now: number): number;
  append(key: Uint8Array, value: Uint8Array, now: number): number;

  // ── hash ───────────────────────────────────────────────────────────────---
  /** Set fields; returns count of newly-created (not overwritten) fields. */
  hSet(key: Uint8Array, pairs: ReadonlyArray<readonly [Uint8Array, Uint8Array]>, now: number): number;
  hGet(key: Uint8Array, field: Uint8Array, now: number): Uint8Array | null;
  hDel(key: Uint8Array, fields: Uint8Array[], now: number): number;
  hGetAll(key: Uint8Array, now: number): Array<[Uint8Array, Uint8Array]>;
  hKeys(key: Uint8Array, now: number): Uint8Array[];
  hVals(key: Uint8Array, now: number): Uint8Array[];
  hLen(key: Uint8Array, now: number): number;
  hExists(key: Uint8Array, field: Uint8Array, now: number): boolean;
  hIncrBy(key: Uint8Array, field: Uint8Array, delta: bigint, now: number): bigint;
  hIncrByFloat(key: Uint8Array, field: Uint8Array, delta: number, now: number): number;

  // ── set ────────────────────────────────────────────────────────────────---
  sAdd(key: Uint8Array, members: Uint8Array[], now: number): number;
  sRem(key: Uint8Array, members: Uint8Array[], now: number): number;
  sIsMember(key: Uint8Array, member: Uint8Array, now: number): boolean;
  sMembers(key: Uint8Array, now: number): Uint8Array[];
  sCard(key: Uint8Array, now: number): number;
  /** Up to `count` random members without removal (count<0 => with repeats). */
  sRandMember(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null;
  /** Remove and return up to `count` random members. */
  sPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null;

  // ── atomicity ─────────────────────────────────────────────────────────────
  /** Run `fn` inside a single SQLite transaction (single-writer assumption). */
  withTransaction<T>(fn: () => T): T;

  /** Close underlying resources. */
  close(): void;
}
