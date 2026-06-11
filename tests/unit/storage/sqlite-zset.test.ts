import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const members = (pairs: Array<[Uint8Array, number]>) => pairs.map(([m]) => dec(m));

const inc = (v: number, exclusive = false) => ({ value: v, exclusive });

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("SqliteStorage zset", () => {
  test("zAdd counts added only; CH counts changed too", () => {
    expect(store.zAdd(enc("z"), [[1, enc("a")], [2, enc("b")]], now)).toBe(2);
    expect(store.zAdd(enc("z"), [[9, enc("a")], [3, enc("c")]], now)).toBe(1);
    expect(store.zAdd(enc("z"), [[10, enc("a")], [4, enc("d")]], now, { ch: true })).toBe(2);
    expect(store.zCard(enc("z"), now)).toBe(4);
  });

  test("zAdd NX never updates, XX never adds (and XX alone creates no key)", () => {
    expect(store.zAdd(enc("z"), [[1, enc("a")]], now, { mode: "XX" })).toBe(0);
    expect(store.exists(enc("z"), now)).toBe(false);
    store.zAdd(enc("z"), [[1, enc("a")]], now);
    expect(store.zAdd(enc("z"), [[5, enc("a")], [2, enc("b")]], now, { mode: "NX" })).toBe(1);
    expect(store.zScore(enc("z"), enc("a"), now)).toBe(1);
    expect(store.zAdd(enc("z"), [[7, enc("a")], [9, enc("nope")]], now, { mode: "XX" })).toBe(0);
    expect(store.zScore(enc("z"), enc("a"), now)).toBe(7);
    expect(store.zScore(enc("z"), enc("nope"), now)).toBeNull();
  });

  test("zAdd GT/LT only move scores in their direction", () => {
    store.zAdd(enc("z"), [[5, enc("a")]], now);
    expect(store.zAdd(enc("z"), [[3, enc("a")]], now, { gt: true, ch: true })).toBe(0);
    expect(store.zAdd(enc("z"), [[8, enc("a")]], now, { gt: true, ch: true })).toBe(1);
    expect(store.zAdd(enc("z"), [[9, enc("a")]], now, { lt: true, ch: true })).toBe(0);
    expect(store.zAdd(enc("z"), [[2, enc("a")]], now, { lt: true, ch: true })).toBe(1);
    expect(store.zScore(enc("z"), enc("a"), now)).toBe(2);
  });

  test("zIncr increments, creates, and honors guards", () => {
    expect(store.zIncr(enc("z"), 2.5, enc("a"), now)).toBe(2.5);
    expect(store.zIncr(enc("z"), 1.5, enc("a"), now)).toBe(4);
    expect(store.zIncr(enc("z"), 1, enc("a"), now, { mode: "NX" })).toBeNull();
    expect(store.zIncr(enc("z"), 1, enc("b"), now, { mode: "XX" })).toBeNull();
    expect(store.zIncr(enc("z"), -1, enc("a"), now, { gt: true })).toBeNull();
    expect(store.zScore(enc("z"), enc("a"), now)).toBe(4);
  });

  test("ties sort by member bytes ascending", () => {
    store.zAdd(enc("z"), [[1, enc("b")], [1, enc("a")], [0, enc("c")]], now);
    expect(members(store.zRangeByRank(enc("z"), 0, -1, false, now))).toEqual(["c", "a", "b"]);
    expect(members(store.zRangeByRank(enc("z"), 0, -1, true, now))).toEqual(["b", "a", "c"]);
    expect(store.zRank(enc("z"), enc("a"), now)).toBe(1);
    expect(store.zRank(enc("z"), enc("b"), now)).toBe(2);
  });

  test("zRangeByRank negative indexes and clamping", () => {
    store.zAdd(enc("z"), [[1, enc("a")], [2, enc("b")], [3, enc("c")]], now);
    expect(members(store.zRangeByRank(enc("z"), -2, -1, false, now))).toEqual(["b", "c"]);
    expect(members(store.zRangeByRank(enc("z"), -100, 100, false, now))).toEqual(["a", "b", "c"]);
    expect(store.zRangeByRank(enc("z"), 2, 1, false, now)).toEqual([]);
    expect(store.zRangeByRank(enc("missing"), 0, -1, false, now)).toEqual([]);
  });

  test("zRangeByScore bounds, exclusivity, ±inf, and LIMIT", () => {
    store.zAdd(enc("z"), [[1, enc("a")], [2, enc("b")], [3, enc("c")]], now);
    expect(members(store.zRangeByScore(enc("z"), inc(2), inc(3), null, now))).toEqual(["b", "c"]);
    expect(members(store.zRangeByScore(enc("z"), inc(2, true), inc(3), null, now))).toEqual(["c"]);
    expect(members(store.zRangeByScore(enc("z"), inc(-Infinity), inc(Infinity), null, now))).toEqual(
      ["a", "b", "c"],
    );
    expect(
      members(store.zRangeByScore(enc("z"), inc(-Infinity), inc(Infinity), { offset: 1, count: 1 }, now)),
    ).toEqual(["b"]);
    expect(
      members(store.zRangeByScore(enc("z"), inc(-Infinity), inc(Infinity), { offset: 1, count: -1 }, now)),
    ).toEqual(["b", "c"]);
    expect(store.zRangeByScore(enc("z"), inc(5), inc(1), null, now)).toEqual([]);
  });

  test("±Infinity scores store and compare", () => {
    store.zAdd(enc("z"), [[-Infinity, enc("lo")], [0, enc("mid")], [Infinity, enc("hi")]], now);
    expect(store.zScore(enc("z"), enc("hi"), now)).toBe(Infinity);
    expect(store.zScore(enc("z"), enc("lo"), now)).toBe(-Infinity);
    expect(members(store.zRangeByRank(enc("z"), 0, -1, false, now))).toEqual(["lo", "mid", "hi"]);
    // Exclusive -inf bound drops the -inf member (Redis "(-inf" semantics).
    expect(
      members(store.zRangeByScore(enc("z"), inc(-Infinity, true), inc(Infinity), null, now)),
    ).toEqual(["mid", "hi"]);
  });

  test("zRem; removing the last member deletes the key", () => {
    store.zAdd(enc("z"), [[1, enc("a")], [2, enc("b")]], now);
    expect(store.zRem(enc("z"), [enc("a"), enc("nope")], now)).toBe(1);
    expect(store.zRem(enc("z"), [enc("b")], now)).toBe(1);
    expect(store.exists(enc("z"), now)).toBe(false);
    expect(store.typeOf(enc("z"), now)).toBeNull();
    expect(store.pttl(enc("z"), now)).toBe(-2);
  });

  test("zRank is null for a missing member or key", () => {
    expect(store.zRank(enc("missing"), enc("a"), now)).toBeNull();
    store.zAdd(enc("z"), [[1, enc("a")]], now);
    expect(store.zRank(enc("z"), enc("nope"), now)).toBeNull();
  });

  test("binary-safe members round-trip", () => {
    const bin = new Uint8Array([0, 255, 13, 10]);
    store.zAdd(enc("z"), [[1, bin]], now);
    expect(store.zScore(enc("z"), bin, now)).toBe(1);
    expect(store.zRangeByRank(enc("z"), 0, -1, false, now)[0]![0]).toEqual(bin);
  });
});
