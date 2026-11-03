/**
 * OverlayStorage differential test.
 *
 * The same random command sequence runs against (A) a plain SqliteStorage and
 * (B) an OverlayStorage over another SqliteStorage. B goes through cycles of
 * pass-through → buffering (IDLE) → sliced flush → pass-through. Every
 * command's result (or error) and the full visible state must match A, and the
 * base engine under B must receive no writes while buffering except from
 * flushStep. After the final flush the two SQLite files must hold the same data.
 */

import { describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";
import { OverlayStorage } from "../../../src/storage/overlay";
import type { StorageEngine } from "../../../src/storage/types";

const E = new TextEncoder();
const b = (s: string): Uint8Array => E.encode(s);

const MUTATORS = new Set([
  "del", "expireSet", "persist", "sweepExpired", "flushAll", "kvSet", "incrBy",
  "incrByFloat", "append", "hSet", "hDel", "hIncrBy", "hIncrByFloat", "sAdd", "sRem",
  "sPop", "lPush", "rPush", "lPop", "rPop", "zAdd", "zIncr", "zRem", "tombstone",
  "tombstoneAll", "setExpireRaw",
]);

function rng(seed: number): () => number {
  let x = seed >>> 0 || 1;
  return () => {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    return (x >>> 0) / 0x100000000;
  };
}

/** Normalize results so A and B compare structurally. */
function norm(v: unknown, sortArrays = false): unknown {
  if (v instanceof Uint8Array) return "b:" + Buffer.from(v).toString("latin1");
  if (typeof v === "bigint") return "n:" + v.toString();
  if (Array.isArray(v)) {
    const out = v.map((x) => norm(x));
    return sortArrays ? out.map((x) => JSON.stringify(x)).sort() : out;
  }
  return v;
}

function run(fn: () => unknown, sortArrays: boolean): unknown {
  try {
    return { ok: norm(fn(), sortArrays) };
  } catch (err) {
    const e = err as { code?: string; name?: string; message?: string };
    return { err: e.code ?? e.name ?? String(err) };
  }
}

function snapshot(s: StorageEngine, keys: Uint8Array[], now: number): unknown {
  const out: Record<string, unknown> = { dbsize: s.dbsize(now) };
  for (const k of keys) {
    const t = s.typeOf(k, now);
    const name = Buffer.from(k).toString();
    let val: unknown = null;
    if (t === "string") val = norm(s.kvGet(k, now));
    if (t === "hash") val = norm(s.hGetAll(k, now), true);
    if (t === "set") val = norm(s.sMembers(k, now), true);
    if (t === "zset") val = norm(s.zRangeByRank(k, 0, -1, false, now));
    if (t === "list") val = norm(s.lRange(k, 0, -1, now));
    out[name] = { t, ttl: s.pttl(k, now) === -1 ? -1 : s.pttl(k, now) >= 0 ? "set" : -2, val };
  }
  return out;
}

function guardBase(base: SqliteStorage, overlay: () => OverlayStorage): SqliteStorage {
  return new Proxy(base, {
    get(target, prop, recv) {
      const v = Reflect.get(target, prop, recv);
      if (typeof v !== "function") return v;
      return (...args: unknown[]) => {
        const o = overlay();
        if (MUTATORS.has(String(prop)) && o.active && !o.applying) {
          throw new Error(`base write "${String(prop)}" while buffering`);
        }
        return (v as (...a: unknown[]) => unknown).apply(target, args);
      };
    },
  });
}

function differential(seed: number, steps: number): void {
  const r = rng(seed);
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(r() * xs.length)]!;
  const keys = Array.from({ length: 6 }, (_, i) => b(`k${i}`));
  const fields = ["f0", "f1", "f2", "f3"].map(b);
  const vals = ["1", "2", "10", "x", "-3", "2.5"].map(b);

  const A = new SqliteStorage(":memory:");
  let overlay!: OverlayStorage;
  const baseB = guardBase(new SqliteStorage(":memory:"), () => overlay);
  overlay = new OverlayStorage(baseB);
  const B: StorageEngine = overlay;

  let now = 1_000_000;
  // phase: 0 pass-through, 1 buffering, 2 flushing (slices interleaved with ops)
  let phase = 0;

  const ops: Array<() => [string, (s: StorageEngine) => unknown, boolean]> = [
    () => {
      const k = pick(keys), v = pick(vals);
      const opts = pick([{}, { mode: "NX" as const }, { mode: "XX" as const }, { keepTtl: true },
        { expireAtMs: now + Math.floor(r() * 50) }, { expireAtMs: now - 1 }]);
      return ["kvSet", (s) => s.kvSet(k, v, now, opts), false];
    },
    () => { const k = pick(keys); return ["kvGet", (s) => s.kvGet(k, now), false]; },
    () => { const k = pick(keys); return ["incrBy", (s) => s.incrBy(k, 3n, now), false]; },
    () => { const k = pick(keys); return ["incrByFloat", (s) => s.incrByFloat(k, 0.5, now), false]; },
    () => { const k = pick(keys); return ["append", (s) => s.append(k, b("z"), now), false]; },
    () => { const ks = [pick(keys), pick(keys)]; return ["del", (s) => s.del(ks, now), false]; },
    () => {
      const k = pick(keys), at = now + Math.floor(r() * 60) - 5;
      return ["expireSet", (s) => s.expireSet(k, at, now), false];
    },
    () => { const k = pick(keys); return ["persist", (s) => s.persist(k, now), false]; },
    () => { const k = pick(keys); return ["pttl", (s) => s.pttl(k, now), false]; },
    () => { const k = pick(keys); return ["typeOf", (s) => s.typeOf(k, now), false]; },
    () => { const k = pick(keys); return ["exists", (s) => s.exists(k, now), false]; },
    () => {
      const k = pick(keys), pairs = [[pick(fields), pick(vals)], [pick(fields), pick(vals)]] as const;
      return ["hSet", (s) => s.hSet(k, pairs, now), false];
    },
    () => { const k = pick(keys), f = pick(fields); return ["hGet", (s) => s.hGet(k, f, now), false]; },
    () => { const k = pick(keys), fs = [pick(fields), pick(fields)]; return ["hDel", (s) => s.hDel(k, fs, now), false]; },
    () => { const k = pick(keys); return ["hLen", (s) => s.hLen(k, now), false]; },
    () => { const k = pick(keys); return ["hGetAll", (s) => s.hGetAll(k, now), true]; },
    () => { const k = pick(keys); return ["hKeys", (s) => s.hKeys(k, now), true]; },
    () => { const k = pick(keys), f = pick(fields); return ["hExists", (s) => s.hExists(k, f, now), false]; },
    () => { const k = pick(keys), f = pick(fields); return ["hIncrBy", (s) => s.hIncrBy(k, f, 2n, now), false]; },
    () => { const k = pick(keys), ms = [pick(fields), pick(fields), pick(fields)]; return ["sAdd", (s) => s.sAdd(k, ms, now), false]; },
    () => { const k = pick(keys), ms = [pick(fields)]; return ["sRem", (s) => s.sRem(k, ms, now), false]; },
    () => { const k = pick(keys), m = pick(fields); return ["sIsMember", (s) => s.sIsMember(k, m, now), false]; },
    () => { const k = pick(keys); return ["sMembers", (s) => s.sMembers(k, now), true]; },
    () => { const k = pick(keys); return ["sCard", (s) => s.sCard(k, now), false]; },
    () => {
      // Random picks differ between engines; popping the whole set does not.
      const k = pick(keys);
      return ["sPopAll", (s) => s.sPop(k, 100, now), true];
    },
    () => {
      const k = pick(keys);
      const entries = [[Math.floor(r() * 5), pick(fields)], [Math.floor(r() * 5), pick(fields)]] as const;
      const opts = pick([{}, { mode: "NX" as const }, { mode: "XX" as const }, { gt: true }, { lt: true, ch: true }]);
      return ["zAdd", (s) => s.zAdd(k, entries, now, opts), false];
    },
    () => { const k = pick(keys), m = pick(fields); return ["zIncr", (s) => s.zIncr(k, 1.5, m, now), false]; },
    () => { const k = pick(keys), ms = [pick(fields)]; return ["zRem", (s) => s.zRem(k, ms, now), false]; },
    () => { const k = pick(keys), m = pick(fields); return ["zScore", (s) => s.zScore(k, m, now), false]; },
    () => { const k = pick(keys), m = pick(fields); return ["zRank", (s) => s.zRank(k, m, now), false]; },
    () => { const k = pick(keys); return ["zCard", (s) => s.zCard(k, now), false]; },
    () => {
      const k = pick(keys), st = Math.floor(r() * 4) - 2, sp = Math.floor(r() * 4) - 1, rev = r() < 0.5;
      return ["zRangeByRank", (s) => s.zRangeByRank(k, st, sp, rev, now), false];
    },
    () => {
      const k = pick(keys);
      const lim = r() < 0.5 ? null : { offset: Math.floor(r() * 2), count: Math.floor(r() * 3) - 1 };
      const min = { value: 1, exclusive: r() < 0.5 };
      return ["zRangeByScore", (s) => s.zRangeByScore(k, min, { value: Infinity, exclusive: false }, lim, now), false];
    },
    () => { const k = pick(keys), vs = [pick(vals), pick(vals)]; return ["lPush", (s) => s.lPush(k, vs, now), false]; },
    () => { const k = pick(keys), vs = [pick(vals)]; return ["rPush", (s) => s.rPush(k, vs, now), false]; },
    () => { const k = pick(keys), c = r() < 0.5 ? null : Math.floor(r() * 4); return ["lPop", (s) => s.lPop(k, c, now), false]; },
    () => { const k = pick(keys), c = r() < 0.5 ? null : Math.floor(r() * 4); return ["rPop", (s) => s.rPop(k, c, now), false]; },
    () => {
      const k = pick(keys), st = Math.floor(r() * 6) - 3, sp = Math.floor(r() * 6) - 2;
      return ["lRange", (s) => s.lRange(k, st, sp, now), false];
    },
    () => { const k = pick(keys), i = Math.floor(r() * 6) - 3; return ["lIndex", (s) => s.lIndex(k, i, now), false]; },
    () => { const k = pick(keys); return ["lLen", (s) => s.lLen(k, now), false]; },
    () => ["dbsize", (s) => s.dbsize(now), false],
    () => (r() < 0.1 ? ["flushAll", (s) => s.flushAll(), false] : ["dbsize", (s) => s.dbsize(now), false]),
  ];

  for (let step = 0; step < steps; step++) {
    // Phase transitions.
    if (phase === 0 && r() < 0.03) {
      overlay.activate();
      phase = 1;
    } else if (phase === 1 && r() < 0.03) {
      phase = 2;
    }
    if (phase === 2 && r() < 0.3) {
      // Tiny budget → one entry per slice, interleaved with new writes.
      if (overlay.flushStep(0, now) === 0 && !overlay.active) phase = 0;
    }

    const [name, fn, sortArrays] = pick(ops)();
    const ra = run(() => fn(A), sortArrays);
    const rb = run(() => fn(B), sortArrays);
    if (JSON.stringify(ra) !== JSON.stringify(rb)) {
      throw new Error(`seed ${seed} step ${step} phase ${phase} ${name}: A=${JSON.stringify(ra)} B=${JSON.stringify(rb)}`);
    }
    if (step % 7 === 0) {
      const sa = JSON.stringify(snapshot(A, keys, now));
      const sb = JSON.stringify(snapshot(B, keys, now));
      if (sa !== sb) throw new Error(`seed ${seed} step ${step} phase ${phase} state:\nA=${sa}\nB=${sb}`);
    }
    if (r() < 0.2) now += Math.floor(r() * 8);
  }

  // Drain and compare the persisted base itself.
  while (overlay.active) overlay.flushStep(1000, now);
  const sa = JSON.stringify(snapshot(A, keys, now));
  const sBase = JSON.stringify(snapshot(baseB, keys, now));
  if (sa !== sBase) throw new Error(`seed ${seed} final base state:\nA=${sa}\nB=${sBase}`);
  A.close();
  baseB.close();
}

describe("OverlayStorage (differential vs plain SQLite)", () => {
  for (let seed = 1; seed <= 40; seed++) {
    test(`random sequence seed=${seed}`, () => differential(seed, 1500));
  }
});

describe("OverlayStorage buffering", () => {
  test("cold keys stay in SQLite; only IDLE writes are buffered", () => {
    const base = new SqliteStorage(":memory:");
    const o = new OverlayStorage(base);
    const now = 1000;
    for (let i = 0; i < 100; i++) base.kvSet(b(`cold${i}`), b("v"), now);
    o.activate();
    expect(o.kvGet(b("cold5"), now)).toEqual(b("v"));
    o.kvSet(b("hot"), b("new"), now);
    o.hSet(b("h"), [[b("f"), b("1")]], now);
    expect(o.bufferedEntries).toBe(2);
    expect(base.kvGet(b("hot"), now)).toBeNull(); // not written yet
    expect(o.kvGet(b("hot"), now)).toEqual(b("new"));
    expect(o.flushStep(1000, now)).toBe(0);
    expect(o.active).toBe(false);
    expect(base.kvGet(b("hot"), now)).toEqual(b("new"));
    expect(base.hGet(b("h"), b("f"), now)).toEqual(b("1"));
    expect(base.dbsize(now)).toBe(102);
  });

  test("DEL of a big key during IDLE flushes as an O(1) tombstone in lazy mode", () => {
    const base = new SqliteStorage(":memory:", { lazyDelete: true });
    const o = new OverlayStorage(base);
    const now = 1000;
    const pairs = Array.from({ length: 50_000 }, (_, i) => [b(`f${i}`), b("x")] as const);
    base.hSet(b("big"), pairs, now);
    o.activate();
    expect(o.del([b("big")], now)).toBe(1);
    expect(o.exists(b("big"), now)).toBe(false);
    const t = performance.now();
    o.flushStep(1000, now);
    expect(performance.now() - t).toBeLessThan(20);
    expect(base.exists(b("big"), now)).toBe(false);
    // Rows are still physically present until the worker reclaims them.
    expect(base.countUpTo(b("big"), "hash", 10)).toBe(10);
  });

  test("buffer pressure fires onPressure past maxBytes", () => {
    let fired = 0;
    const o = new OverlayStorage(new SqliteStorage(":memory:"), { onPressure: () => fired++ }, { maxBytes: 1024 });
    o.activate();
    for (let i = 0; i < 20; i++) o.kvSet(b(`k${i}`), new Uint8Array(100), 1);
    expect(fired).toBeGreaterThan(0);
    expect(o.bufferedBytes).toBeGreaterThan(1024);
  });

  test("a failed flush slice keeps its entries buffered", () => {
    const base = new SqliteStorage(":memory:");
    const o = new OverlayStorage(base);
    o.activate();
    o.kvSet(b("a"), b("1"), 1);
    const orig = base.kvSet.bind(base);
    base.kvSet = () => {
      throw new Error("disk full");
    };
    expect(() => o.flushStep(1000, 1)).toThrow("disk full");
    expect(o.bufferedEntries).toBe(1);
    base.kvSet = orig;
    expect(o.flushStep(1000, 1)).toBe(0);
    expect(base.kvGet(b("a"), 1)).toEqual(b("1"));
  });
});
