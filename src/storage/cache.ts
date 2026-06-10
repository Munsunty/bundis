/**
 * HotCacheStorage — write-through in-memory hot cache over a StorageEngine.
 *
 * Policy (write-through + adaptive time-to-idle + LRU byte cap):
 * - kvSet writes through to the inner engine, then caches the fresh value, so
 *   a key is hot from the moment it is written.
 * - kvGet serves live cache entries without touching SQLite; misses read
 *   through and fill the cache.
 * - An entry is evicted when idle longer than its effective TTI. The TTI grows
 *   with accumulated hits — `min(maxFactor·base, base·(1 + log2(1 + hits)))` —
 *   so frequently-read keys survive longer idle gaps. Hit counts halve on every
 *   sweep so past popularity decays.
 * - Total cached bytes are capped; the least-recently-used entries are evicted
 *   first when the cap is exceeded. Values larger than 1/8 of the cap are never
 *   cached (one huge value must not flush the whole working set).
 *
 * Coherence (single-writer, single-process):
 * - Every mutation in the inner engine fires its onWrite hook; the server wires
 *   that hook to {@link invalidate}, so all write paths (DEL, EXPIRE, INCR,
 *   hash/set ops, sweeps, type overwrites) evict stale entries automatically.
 * - Inside an explicit transaction (MULTI/EXEC) fills are suppressed: a rolled
 *   back transaction must not leave uncommitted values in the cache.
 *   Invalidations still apply immediately (evicting too much is always safe).
 * - Cache entries carry expireAtMs and are checked lazily on every hit, so an
 *   expired key never serves from cache.
 */

import type { RedisType, SetOptions, StorageEngine } from "./types";

interface CacheEntry {
  value: Uint8Array;
  expireAtMs: number | null;
  lastAccess: number;
  hits: number;
  bytes: number;
}

export interface HotCacheOptions {
  /** Hard ceiling for cached bytes (keys + values + overhead). */
  readonly maxBytes: number;
  /** Base time-to-idle in ms. */
  readonly baseIdleMs: number;
  /** Cap on the adaptive TTI as a multiple of baseIdleMs (default 8). */
  readonly maxIdleFactor?: number;
  /** Min interval between idle sweeps in ms (default 5000). */
  readonly sweepEveryMs?: number;
}

export interface CacheStats {
  entries: number;
  bytes: number;
  maxBytes: number;
  hits: number;
  misses: number;
  fills: number;
  invalidations: number;
  evictedIdle: number;
  evictedLru: number;
}

const ENTRY_OVERHEAD = 96; // rough per-entry bookkeeping cost in bytes

/** Sentinel: the key vanished between write/read and the expiry read-back. */
const ABSENT = Symbol("absent");

export class HotCacheStorage implements StorageEngine {
  // Map iteration order doubles as the LRU order: a hit re-inserts its entry.
  #cache = new Map<string, CacheEntry>();
  #bytes = 0;
  #inTxn = false;
  #lastSweep = 0;

  #hits = 0;
  #misses = 0;
  #fills = 0;
  #invalidations = 0;
  #evictedIdle = 0;
  #evictedLru = 0;

  readonly #maxBytes: number;
  readonly #baseIdleMs: number;
  readonly #maxIdleMs: number;
  readonly #sweepEveryMs: number;
  readonly #maxEntryBytes: number;

  constructor(
    private readonly inner: StorageEngine,
    opts: HotCacheOptions,
  ) {
    this.#maxBytes = opts.maxBytes;
    this.#baseIdleMs = opts.baseIdleMs;
    this.#maxIdleMs = opts.baseIdleMs * (opts.maxIdleFactor ?? 8);
    this.#sweepEveryMs = opts.sweepEveryMs ?? 5000;
    this.#maxEntryBytes = Math.max(1, Math.floor(opts.maxBytes / 8));
  }

  /** Evict one key (no-op if absent). Wired to the inner engine's onWrite. */
  invalidate = (key: Uint8Array): void => {
    const k = mapKey(key);
    const entry = this.#cache.get(k);
    if (entry) {
      this.#cache.delete(k);
      this.#bytes -= entry.bytes;
      this.#invalidations++;
    }
  };

  stats(): CacheStats {
    return {
      entries: this.#cache.size,
      bytes: this.#bytes,
      maxBytes: this.#maxBytes,
      hits: this.#hits,
      misses: this.#misses,
      fills: this.#fills,
      invalidations: this.#invalidations,
      evictedIdle: this.#evictedIdle,
      evictedLru: this.#evictedLru,
    };
  }

  // ── cached fast paths ────────────────────────────────────────────────────-

  kvGet(key: Uint8Array, now: number): Uint8Array | null {
    const entry = this.#liveEntry(key, now);
    if (entry) {
      this.#hits++;
      return entry.value;
    }
    this.#misses++;
    const value = this.inner.kvGet(key, now);
    if (value !== null && !this.#inTxn) {
      const exp = this.#expireOf(key, now);
      if (exp !== ABSENT) this.#fill(key, value, exp, now);
    }
    return value;
  }

  kvSet(key: Uint8Array, value: Uint8Array, now: number, opts?: SetOptions): "set" | "noop" {
    const result = this.inner.kvSet(key, value, now, opts);
    // onWrite already invalidated any stale entry; cache the committed value.
    // A "noop" (NX/XX guard) fires no onWrite, so the old entry stays valid.
    if (result === "set" && !this.#inTxn) {
      // If the write carried an already-past expiry, the read-back's lazy
      // expiry deletes the row and reports ABSENT — caching then would create
      // an immortal ghost entry that SQLite no longer backs.
      const exp = this.#expireOf(key, now);
      if (exp !== ABSENT) this.#fill(key, value, exp, now);
    }
    return result;
  }

  exists(key: Uint8Array, now: number): boolean {
    if (this.#liveEntry(key, now)) {
      this.#hits++;
      return true;
    }
    this.#misses++;
    return this.inner.exists(key, now);
  }

  typeOf(key: Uint8Array, now: number): RedisType | null {
    if (this.#liveEntry(key, now)) {
      this.#hits++;
      return "string";
    }
    this.#misses++;
    return this.inner.typeOf(key, now);
  }

  pttl(key: Uint8Array, now: number): number {
    const entry = this.#liveEntry(key, now);
    if (entry) {
      this.#hits++;
      return entry.expireAtMs === null ? -1 : Math.max(0, entry.expireAtMs - now);
    }
    this.#misses++;
    return this.inner.pttl(key, now);
  }

  sweepExpired(now: number): number {
    if (now - this.#lastSweep >= this.#sweepEveryMs) {
      this.#lastSweep = now;
      this.#idleSweep(now);
    }
    // Inner sweep fires onWrite per removed key → cache invalidation included.
    return this.inner.sweepExpired(now);
  }

  withTransaction<T>(fn: () => T): T {
    // Suppress fills while a MULTI/EXEC body runs: if the transaction rolls
    // back, the cache must not retain values that were never committed.
    const outer = this.#inTxn;
    this.#inTxn = true;
    try {
      return this.inner.withTransaction(fn);
    } finally {
      this.#inTxn = outer;
    }
  }

  close(): void {
    this.#cache.clear();
    this.#bytes = 0;
    this.inner.close();
  }

  // ── pure delegations (onWrite keeps the cache coherent) ──────────────────-

  del(keys: Uint8Array[], now: number): number {
    // Eager eviction (not just onWrite): inner.del only fires onWrite for keys
    // whose row exists, so this guarantees DEL always heals any divergence.
    for (const k of keys) this.invalidate(k);
    return this.inner.del(keys, now);
  }
  expireSet(key: Uint8Array, atMs: number, now: number): boolean {
    return this.inner.expireSet(key, atMs, now);
  }
  persist(key: Uint8Array, now: number): boolean {
    return this.inner.persist(key, now);
  }
  dbsize(now: number): number {
    return this.inner.dbsize(now);
  }
  flushAll(): void {
    this.invalidateAll();
    this.inner.flushAll();
  }
  /** Drop every cache entry (FLUSHDB or any whole-keyspace event). */
  invalidateAll(): void {
    this.#invalidations += this.#cache.size;
    this.#cache.clear();
    this.#bytes = 0;
  }
  incrBy(key: Uint8Array, delta: bigint, now: number): bigint {
    return this.inner.incrBy(key, delta, now);
  }
  incrByFloat(key: Uint8Array, delta: number, now: number): number {
    return this.inner.incrByFloat(key, delta, now);
  }
  append(key: Uint8Array, value: Uint8Array, now: number): number {
    return this.inner.append(key, value, now);
  }
  hSet(key: Uint8Array, pairs: ReadonlyArray<readonly [Uint8Array, Uint8Array]>, now: number): number {
    return this.inner.hSet(key, pairs, now);
  }
  hGet(key: Uint8Array, field: Uint8Array, now: number): Uint8Array | null {
    return this.inner.hGet(key, field, now);
  }
  hDel(key: Uint8Array, fields: Uint8Array[], now: number): number {
    return this.inner.hDel(key, fields, now);
  }
  hGetAll(key: Uint8Array, now: number): Array<[Uint8Array, Uint8Array]> {
    return this.inner.hGetAll(key, now);
  }
  hKeys(key: Uint8Array, now: number): Uint8Array[] {
    return this.inner.hKeys(key, now);
  }
  hVals(key: Uint8Array, now: number): Uint8Array[] {
    return this.inner.hVals(key, now);
  }
  hLen(key: Uint8Array, now: number): number {
    return this.inner.hLen(key, now);
  }
  hExists(key: Uint8Array, field: Uint8Array, now: number): boolean {
    return this.inner.hExists(key, field, now);
  }
  hIncrBy(key: Uint8Array, field: Uint8Array, delta: bigint, now: number): bigint {
    return this.inner.hIncrBy(key, field, delta, now);
  }
  hIncrByFloat(key: Uint8Array, field: Uint8Array, delta: number, now: number): number {
    return this.inner.hIncrByFloat(key, field, delta, now);
  }
  sAdd(key: Uint8Array, members: Uint8Array[], now: number): number {
    return this.inner.sAdd(key, members, now);
  }
  sRem(key: Uint8Array, members: Uint8Array[], now: number): number {
    return this.inner.sRem(key, members, now);
  }
  sIsMember(key: Uint8Array, member: Uint8Array, now: number): boolean {
    return this.inner.sIsMember(key, member, now);
  }
  sMembers(key: Uint8Array, now: number): Uint8Array[] {
    return this.inner.sMembers(key, now);
  }
  sCard(key: Uint8Array, now: number): number {
    return this.inner.sCard(key, now);
  }
  sRandMember(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    return this.inner.sRandMember(key, count, now);
  }
  sPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    return this.inner.sPop(key, count, now);
  }

  // ── internals ────────────────────────────────────────────────────────────-

  /** Live cache entry for key, refreshed as most-recently-used; null if absent/expired. */
  #liveEntry(key: Uint8Array, now: number): CacheEntry | null {
    const k = mapKey(key);
    const entry = this.#cache.get(k);
    if (!entry) return null;
    if (entry.expireAtMs !== null && entry.expireAtMs <= now) {
      // Key TTL passed: drop from cache; the inner lazy path owns row deletion.
      this.#cache.delete(k);
      this.#bytes -= entry.bytes;
      return null;
    }
    entry.lastAccess = now;
    entry.hits++;
    this.#cache.delete(k); // re-insert → most recently used
    this.#cache.set(k, entry);
    return entry;
  }

  #fill(key: Uint8Array, value: Uint8Array, expireAtMs: number | null, now: number): void {
    const bytes = key.length + value.byteLength + ENTRY_OVERHEAD;
    if (bytes > this.#maxEntryBytes) return;
    const k = mapKey(key);
    const prev = this.#cache.get(k);
    if (prev) {
      this.#bytes -= prev.bytes;
      this.#cache.delete(k);
    }
    this.#cache.set(k, {
      value,
      expireAtMs,
      lastAccess: now,
      hits: prev ? prev.hits : 0, // overwrite keeps earned popularity
      bytes,
    });
    this.#bytes += bytes;
    this.#fills++;
    while (this.#bytes > this.#maxBytes && this.#cache.size > 0) {
      const oldest = this.#cache.keys().next().value as string;
      const evicted = this.#cache.get(oldest)!;
      this.#cache.delete(oldest);
      this.#bytes -= evicted.bytes;
      this.#evictedLru++;
    }
  }

  /**
   * Resulting expiry of a just-read/just-written string key: epoch ms, null
   * for "no expiry", or ABSENT when the key no longer exists (-2) — callers
   * must skip the fill in that case.
   */
  #expireOf(key: Uint8Array, now: number): number | null | typeof ABSENT {
    const ttl = this.inner.pttl(key, now);
    if (ttl === -2) return ABSENT;
    return ttl === -1 ? null : now + ttl;
  }

  /** Evict idle entries; decay hit counts so popularity is not permanent. */
  #idleSweep(now: number): void {
    for (const [k, entry] of this.#cache) {
      const grown = this.#baseIdleMs * (1 + Math.log2(1 + entry.hits));
      const tti = Math.min(this.#maxIdleMs, grown);
      if (now - entry.lastAccess > tti) {
        this.#cache.delete(k);
        this.#bytes -= entry.bytes;
        this.#evictedIdle++;
      } else {
        entry.hits >>= 1;
      }
    }
  }
}

/** Binary-safe Map key for raw key bytes. */
function mapKey(key: Uint8Array): string {
  return Buffer.from(key).toString("latin1");
}
