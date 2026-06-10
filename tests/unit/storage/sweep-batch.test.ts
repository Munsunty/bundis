import { describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";
import { WatchRegistry } from "../../../src/sidecar/watch";

const enc = new TextEncoder();
const b = (s: string) => enc.encode(s);

describe("batched expiry sweep", () => {
  test("one sweep call reclaims at most the batch cap; repeated calls drain", () => {
    const s = new SqliteStorage(":memory:");
    for (let i = 0; i < 2500; i++) {
      s.kvSet(b(`k${i}`), b("v"), 1000, { expireAtMs: 1500 });
    }
    expect(s.sweepExpired(2000)).toBe(1000); // capped
    expect(s.sweepExpired(2000)).toBe(1000);
    expect(s.sweepExpired(2000)).toBe(500);
    expect(s.sweepExpired(2000)).toBe(0);
  });

  test("dbsize counts live keys without sweeping", () => {
    const s = new SqliteStorage(":memory:");
    s.kvSet(b("live"), b("v"), 1000);
    s.kvSet(b("dead"), b("v"), 1000, { expireAtMs: 1500 });
    expect(s.dbsize(2000)).toBe(1); // dead not counted...
    expect(s.sweepExpired(2000)).toBe(1); // ...but also not yet reclaimed
  });

  test("sweep fires onWrite per reclaimed key (cache/WATCH invalidation)", () => {
    const seen: string[] = [];
    const s = new SqliteStorage(":memory:", {
      onWrite: (k) => seen.push(new TextDecoder().decode(k)),
    });
    s.kvSet(b("x"), b("v"), 1000, { expireAtMs: 1500 });
    seen.length = 0;
    s.sweepExpired(2000);
    expect(seen).toEqual(["x"]);
  });
});

describe("bounded WatchRegistry", () => {
  test("bump is a no-op when nothing is watched; entries die with release", () => {
    const w = new WatchRegistry();
    w.bump(b("k")); // nobody watching → must not create an entry
    expect(w.size()).toBe(0);

    const v0 = w.acquire(b("k"));
    expect(w.size()).toBe(1);
    w.bump(b("k"));
    expect(w.peek(b("k"))).toBe(v0 + 1);
    w.release(b("k"));
    expect(w.size()).toBe(0);
    w.bump(b("k")); // again a no-op
    expect(w.size()).toBe(0);
  });

  test("refcounting: shared key survives until the last watcher releases", () => {
    const w = new WatchRegistry();
    w.acquire(b("k"));
    w.acquire(b("k"));
    w.release(b("k"));
    expect(w.size()).toBe(1); // second watcher still interested
    w.release(b("k"));
    expect(w.size()).toBe(0);
  });

  test("bumpAll (FLUSHALL) dirties snapshots even without per-key entries", () => {
    const w = new WatchRegistry();
    const v = w.acquire(b("k"));
    w.bumpAll();
    expect(w.peek(b("k"))).not.toBe(v);
  });
});
