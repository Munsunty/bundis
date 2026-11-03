/**
 * SqliteStorage write guard (Statement.readonly, Bun >= 1.4.4).
 *
 * Worker maintenance relies on "the main connection never writes while the
 * overlay buffers". The guard turns a violation into a thrown WriteGuardError
 * instead of a lock stall on the worker's write lock or a silent divergence
 * between buffer and SQLite.
 */

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteStorage, WriteGuardError } from "../../../src/storage/sqlite";
import { OverlayStorage } from "../../../src/storage/overlay";

const E = new TextEncoder();
const b = (s: string): Uint8Array => E.encode(s);
const supported = typeof (new Database(":memory:").prepare("SELECT 1") as { readonly?: unknown }).readonly === "boolean";

describe.if(supported)("SqliteStorage write guard", () => {
  test("forbids every write primitive while the predicate is true", () => {
    const s = new SqliteStorage(":memory:");
    s.kvSet(b("k"), b("v"), 1);
    s.hSet(b("h"), [[b("f"), b("v")]], 1);
    let forbid = true;
    s.setWriteGuard(() => forbid);
    const writes: Array<[string, () => unknown]> = [
      ["kvSet", () => s.kvSet(b("k"), b("x"), 1)],
      ["incrBy", () => s.incrBy(b("n"), 1n, 1)],
      ["del", () => s.del([b("k")], 1)],
      ["expireSet", () => s.expireSet(b("k"), 100, 1)],
      ["hSet", () => s.hSet(b("h"), [[b("g"), b("v")]], 1)],
      ["hDel", () => s.hDel(b("h"), [b("f")], 1)],
      ["sAdd", () => s.sAdd(b("s"), [b("a")], 1)],
      ["rPush", () => s.rPush(b("l"), [b("a")], 1)],
      ["zAdd", () => s.zAdd(b("z"), [[1, b("a")]], 1)],
      ["sweepExpired", () => s.sweepExpired(1)],
      ["tombstoneAll", () => s.tombstoneAll()],
      ["flushAll", () => s.flushAll()],
    ];
    for (const [name, fn] of writes) {
      expect(fn, name).toThrow(WriteGuardError);
    }
    // Nothing changed, and reads keep working under the guard.
    expect(s.kvGet(b("k"), 1)).toEqual(b("v"));
    expect(s.hGetAll(b("h"), 1)).toEqual([[b("f"), b("v")]]);
    expect(s.dbsize(1)).toBe(2);
    expect(s.diskStats().pageSize).toBeGreaterThan(0);
    // Predicate false → writes allowed again.
    forbid = false;
    expect(s.kvSet(b("k"), b("x"), 1)).toBe("set");
  });

  test("the error names the offending SQL", () => {
    const s = new SqliteStorage(":memory:");
    s.setWriteGuard(() => true);
    try {
      s.kvSet(b("k"), b("v"), 1);
      throw new Error("expected WriteGuardError");
    } catch (err) {
      expect(err).toBeInstanceOf(WriteGuardError);
      expect((err as WriteGuardError).message).toContain("INSERT INTO keys");
    }
  });

  test("catches an overlay code path that forgets to buffer", () => {
    const base = new SqliteStorage(":memory:");
    const o = new OverlayStorage(base);
    base.setWriteGuard(() => o.active && !o.applying);
    o.activate();
    // Correct path: buffered, no SQLite write.
    expect(o.kvSet(b("a"), b("1"), 1)).toBe("set");
    // Simulated bug: a mutation that bypasses the buffer while IDLE.
    expect(() => base.kvSet(b("a"), b("2"), 1)).toThrow(WriteGuardError);
    // Applying the buffer is the one allowed writer.
    expect(o.flushStep(1000, 1)).toBe(0);
    expect(base.kvGet(b("a"), 1)).toEqual(b("1"));
    expect(base.kvSet(b("a"), b("3"), 1)).toBe("set"); // inactive again
  });
});
