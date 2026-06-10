import { describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";
import { HotCacheStorage, type HotCacheOptions } from "../../../src/storage/cache";

const enc = new TextEncoder();
const b = (s: string) => enc.encode(s);

/** Mirror the server wiring: inner onWrite chains into cache invalidation. */
function makeCached(opts: Partial<HotCacheOptions> = {}) {
  let cache: HotCacheStorage | null = null;
  const inner = new SqliteStorage(":memory:", {
    onWrite: (k) => cache?.invalidate(k),
  });
  cache = new HotCacheStorage(inner, {
    maxBytes: opts.maxBytes ?? 1024 * 1024,
    baseIdleMs: opts.baseIdleMs ?? 1000,
    maxIdleFactor: opts.maxIdleFactor,
    sweepEveryMs: opts.sweepEveryMs ?? 0,
  });
  return { cache, inner };
}

describe("HotCacheStorage", () => {
  test("write-through: a SET key serves reads from memory", () => {
    const { cache } = makeCached();
    cache.kvSet(b("k"), b("v"), 1000);
    expect(cache.kvGet(b("k"), 1001)).toEqual(b("v"));
    const s = cache.stats();
    expect(s.hits).toBe(1);
    expect(s.misses).toBe(0);
    expect(s.entries).toBe(1);
  });

  test("read-fill: a miss reads through and the next read hits", () => {
    const { cache, inner } = makeCached();
    inner.kvSet(b("k"), b("v"), 1000); // bypasses the cache layer
    expect(cache.kvGet(b("k"), 1001)).toEqual(b("v")); // miss + fill
    expect(cache.kvGet(b("k"), 1002)).toEqual(b("v")); // hit
    const s = cache.stats();
    expect(s.misses).toBe(1);
    expect(s.hits).toBe(1);
  });

  test("key TTL is respected on the hit path", () => {
    const { cache } = makeCached();
    cache.kvSet(b("k"), b("v"), 1000, { expireAtMs: 1100 });
    expect(cache.kvGet(b("k"), 1050)).toEqual(b("v")); // live hit
    expect(cache.kvGet(b("k"), 1200)).toBeNull(); // expired: cache + inner agree
    expect(cache.exists(b("k"), 1200)).toBe(false);
  });

  test("cached pttl matches storage semantics", () => {
    const { cache } = makeCached();
    cache.kvSet(b("k"), b("v"), 1000, { expireAtMs: 2000 });
    expect(cache.pttl(b("k"), 1500)).toBe(500);
    cache.kvSet(b("p"), b("v"), 1000);
    expect(cache.pttl(b("p"), 1500)).toBe(-1);
  });

  test("mutations invalidate: DEL, EXPIRE-to-past, INCR", () => {
    const { cache } = makeCached();
    cache.kvSet(b("k"), b("5"), 1000);
    cache.kvGet(b("k"), 1001);
    cache.del([b("k")], 1002);
    expect(cache.kvGet(b("k"), 1003)).toBeNull();

    cache.kvSet(b("e"), b("v"), 1000);
    cache.expireSet(b("e"), 1500, 1001);
    expect(cache.kvGet(b("e"), 2000)).toBeNull(); // expired via new TTL

    cache.kvSet(b("n"), b("5"), 1000);
    cache.kvGet(b("n"), 1001); // cached "5"
    expect(cache.incrBy(b("n"), 2n, 1002)).toBe(7n);
    expect(cache.kvGet(b("n"), 1003)).toEqual(b("7")); // stale "5" must be gone
  });

  test("NX noop fires no invalidation and the old value stays cached", () => {
    const { cache } = makeCached();
    cache.kvSet(b("k"), b("v1"), 1000);
    expect(cache.kvSet(b("k"), b("v2"), 1001, { mode: "NX" })).toBe("noop");
    expect(cache.kvGet(b("k"), 1002)).toEqual(b("v1"));
    expect(cache.stats().hits).toBe(1);
  });

  test("rolled-back transaction leaves no uncommitted value in the cache", () => {
    const { cache } = makeCached();
    expect(() =>
      cache.withTransaction(() => {
        cache.kvSet(b("t"), b("dirty"), 1000);
        throw new Error("rollback");
      }),
    ).toThrow("rollback");
    expect(cache.kvGet(b("t"), 1001)).toBeNull();
  });

  test("committed transaction values are readable (filled on first read)", () => {
    const { cache } = makeCached();
    cache.withTransaction(() => {
      cache.kvSet(b("t"), b("v"), 1000);
    });
    expect(cache.kvGet(b("t"), 1001)).toEqual(b("v")); // miss → fill
    expect(cache.kvGet(b("t"), 1002)).toEqual(b("v")); // hit
    expect(cache.stats().hits).toBe(1);
  });

  test("byte cap evicts least-recently-used first", () => {
    // entry = 1-byte key + 100-byte value + 96 overhead = 197 bytes.
    // Cap fits 9 entries; per-entry limit (cap/8 = 222) still admits each one.
    const entryBytes = 1 + 100 + 96;
    const { cache } = makeCached({ maxBytes: entryBytes * 9 + 10 });
    const val = new Uint8Array(100);
    const names = ["a", "b", "c", "d", "e", "f", "g", "h", "i"];
    names.forEach((name, idx) => cache.kvSet(b(name), val, 1000 + idx));
    cache.kvGet(b("a"), 1100); // refresh a → LRU head is now b
    cache.kvSet(b("j"), val, 1101); // over cap → evicts b
    const s = cache.stats();
    expect(s.evictedLru).toBe(1);
    expect(s.entries).toBe(9);
    const hitsBefore = cache.stats().hits;
    cache.kvGet(b("b"), 1102); // must be a miss (evicted), value still in SQLite
    expect(cache.stats().hits).toBe(hitsBefore);
    expect(cache.kvGet(b("a"), 1103)).toEqual(val); // survived
  });

  test("oversized values are served but never cached", () => {
    const { cache } = makeCached({ maxBytes: 1000 }); // max entry = 125 bytes
    const big = new Uint8Array(500);
    cache.kvSet(b("big"), big, 1000);
    expect(cache.kvGet(b("big"), 1001)).toEqual(big);
    expect(cache.stats().entries).toBe(0);
    expect(cache.stats().hits).toBe(0);
  });

  test("idle sweep evicts cold entries; hits extend TTI up to the cap", () => {
    const { cache } = makeCached({ baseIdleMs: 100, maxIdleFactor: 8 });
    cache.kvSet(b("hot"), b("v"), 1000);
    cache.kvSet(b("cold"), b("v"), 1000);
    for (let i = 0; i < 1000; i++) cache.kvGet(b("hot"), 1000 + i); // hits → TTI grows to cap (800ms)
    const now1 = 1000 + 1000 + 250; // hot lastAccess ≈ 1999 → idle 251ms; cold idle 1250ms
    cache.sweepExpired(now1);
    expect(cache.kvGet(b("cold"), now1 + 1)).toEqual(b("v")); // gone from cache → miss-fill (value persists)
    const missesAfterCold = cache.stats().misses;
    cache.kvGet(b("hot"), now1 + 2); // should still be a cache hit
    expect(cache.stats().misses).toBe(missesAfterCold);
    // far past the 8x cap → even the hot key is evicted
    const now2 = now1 + 100 * 8 + 1000;
    cache.sweepExpired(now2);
    const missesBefore = cache.stats().misses;
    cache.kvGet(b("hot"), now2 + 1);
    expect(cache.stats().misses).toBe(missesBefore + 1);
  });

  test("kvSet with an already-past expiry never creates a ghost entry", () => {
    const { cache, inner } = makeCached();
    // The row is written expired; the expiry read-back lazily deletes it.
    // The cache must NOT retain the value as immortal (poisoning regression).
    cache.kvSet(b("k"), b("ghost"), 1000, { expireAtMs: 1000 });
    expect(cache.kvGet(b("k"), 1001)).toBeNull();
    expect(cache.exists(b("k"), 1001)).toBe(false);
    expect(cache.pttl(b("k"), 1001)).toBe(-2);
    expect(cache.typeOf(b("k"), 1001)).toBeNull();
    expect(inner.exists(b("k"), 1001)).toBe(false);
    expect(cache.stats().entries).toBe(0);
  });

  test("DEL heals cache/SQLite divergence even when the inner row is gone", () => {
    const { cache } = makeCached();
    cache.kvSet(b("k"), b("v"), 1000);
    cache.del([b("k")], 1001);
    expect(cache.kvGet(b("k"), 1002)).toBeNull();
    expect(cache.stats().entries).toBe(0);
  });

  test("meta fast paths count misses so the hit ratio is coherent", () => {
    const { cache } = makeCached();
    cache.exists(b("nope"), 1000);
    cache.typeOf(b("nope"), 1000);
    cache.pttl(b("nope"), 1000);
    expect(cache.stats().misses).toBe(3);
    expect(cache.stats().hits).toBe(0);
  });

  test("WRONGTYPE still propagates from the inner engine", () => {
    const { cache } = makeCached();
    cache.hSet(b("h"), [[b("f"), b("v")]], 1000);
    expect(() => cache.kvGet(b("h"), 1001)).toThrow();
    expect(cache.typeOf(b("h"), 1002)).toBe("hash");
  });

  test("overwriting a hash key with SET caches the new string", () => {
    const { cache } = makeCached();
    cache.hSet(b("k"), [[b("f"), b("v")]], 1000);
    cache.kvSet(b("k"), b("now-string"), 1001);
    expect(cache.kvGet(b("k"), 1002)).toEqual(b("now-string"));
    expect(cache.typeOf(b("k"), 1003)).toBe("string");
  });
});
