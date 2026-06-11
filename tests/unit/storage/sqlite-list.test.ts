import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const decAll = (bs: Uint8Array[]) => bs.map(dec);

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("SqliteStorage list", () => {
  test("lPush prepends left-to-right (each value becomes the new head)", () => {
    expect(store.lPush(enc("l"), [enc("a"), enc("b"), enc("c")], now)).toBe(3);
    expect(decAll(store.lRange(enc("l"), 0, -1, now))).toEqual(["c", "b", "a"]);
  });

  test("rPush appends in order; mixed pushes keep both ends", () => {
    expect(store.rPush(enc("l"), [enc("b"), enc("c")], now)).toBe(2);
    expect(store.lPush(enc("l"), [enc("a")], now)).toBe(3);
    expect(decAll(store.lRange(enc("l"), 0, -1, now))).toEqual(["a", "b", "c"]);
  });

  test("lLen and lIndex with negative indexes", () => {
    store.rPush(enc("l"), [enc("a"), enc("b"), enc("c")], now);
    expect(store.lLen(enc("l"), now)).toBe(3);
    expect(dec(store.lIndex(enc("l"), 0, now)!)).toBe("a");
    expect(dec(store.lIndex(enc("l"), -1, now)!)).toBe("c");
    expect(store.lIndex(enc("l"), 3, now)).toBeNull();
    expect(store.lIndex(enc("l"), -4, now)).toBeNull();
  });

  test("lRange clamps out-of-bounds and crossed ranges", () => {
    store.rPush(enc("l"), [enc("a"), enc("b"), enc("c")], now);
    expect(decAll(store.lRange(enc("l"), -100, 100, now))).toEqual(["a", "b", "c"]);
    expect(decAll(store.lRange(enc("l"), 1, 1, now))).toEqual(["b"]);
    expect(decAll(store.lRange(enc("l"), -2, -1, now))).toEqual(["b", "c"]);
    expect(store.lRange(enc("l"), 2, 1, now)).toEqual([]);
    expect(store.lRange(enc("missing"), 0, -1, now)).toEqual([]);
  });

  test("lPop/rPop single", () => {
    store.rPush(enc("l"), [enc("a"), enc("b"), enc("c")], now);
    expect(dec(store.lPop(enc("l"), null, now) as Uint8Array)).toBe("a");
    expect(dec(store.rPop(enc("l"), null, now) as Uint8Array)).toBe("c");
    expect(store.lLen(enc("l"), now)).toBe(1);
  });

  test("lPop/rPop with count pop in order and clamp to length", () => {
    store.rPush(enc("l"), [enc("a"), enc("b"), enc("c")], now);
    expect(decAll(store.lPop(enc("l"), 2, now) as Uint8Array[])).toEqual(["a", "b"]);
    store.rPush(enc("l"), [enc("d"), enc("e")], now);
    expect(decAll(store.rPop(enc("l"), 10, now) as Uint8Array[])).toEqual(["e", "d", "c"]);
  });

  test("pop on a missing key is null even with a count", () => {
    expect(store.lPop(enc("missing"), null, now)).toBeNull();
    expect(store.lPop(enc("missing"), 2, now)).toBeNull();
  });

  test("popping the last element deletes the key (exists/type/ttl)", () => {
    store.rPush(enc("l"), [enc("a")], now);
    store.expireSet(enc("l"), now + 60_000, now);
    expect(dec(store.lPop(enc("l"), null, now) as Uint8Array)).toBe("a");
    expect(store.exists(enc("l"), now)).toBe(false);
    expect(store.typeOf(enc("l"), now)).toBeNull();
    expect(store.pttl(enc("l"), now)).toBe(-2);
  });

  test("binary-safe values round-trip", () => {
    const bin = new Uint8Array([0, 1, 255, 13, 10, 0]);
    store.rPush(enc("l"), [bin], now);
    expect(store.lIndex(enc("l"), 0, now)).toEqual(bin);
  });
});
