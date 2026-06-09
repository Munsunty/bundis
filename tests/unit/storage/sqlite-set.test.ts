import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";

const enc = (s: string) => new TextEncoder().encode(s);
const decAll = (bs: Uint8Array[]) => bs.map((b) => new TextDecoder().decode(b)).sort();

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("SqliteStorage set", () => {
  test("sAdd reports newly added members only", () => {
    expect(store.sAdd(enc("s"), [enc("a"), enc("b")], now)).toBe(2);
    expect(store.sAdd(enc("s"), [enc("a"), enc("c")], now)).toBe(1);
    expect(store.sCard(enc("s"), now)).toBe(3);
  });

  test("sIsMember / sMembers", () => {
    store.sAdd(enc("s"), [enc("a"), enc("b")], now);
    expect(store.sIsMember(enc("s"), enc("a"), now)).toBe(true);
    expect(store.sIsMember(enc("s"), enc("z"), now)).toBe(false);
    expect(decAll(store.sMembers(enc("s"), now))).toEqual(["a", "b"]);
  });

  test("sRem; emptying the set deletes the key", () => {
    store.sAdd(enc("s"), [enc("a")], now);
    expect(store.sRem(enc("s"), [enc("a")], now)).toBe(1);
    expect(store.exists(enc("s"), now)).toBe(false);
  });

  test("sPop with count removes and returns members", () => {
    store.sAdd(enc("s"), [enc("a"), enc("b"), enc("c")], now);
    const popped = store.sPop(enc("s"), 2, now) as Uint8Array[];
    expect(popped).toHaveLength(2);
    expect(store.sCard(enc("s"), now)).toBe(1);
  });

  test("sRandMember does not remove", () => {
    store.sAdd(enc("s"), [enc("a"), enc("b")], now);
    const got = store.sRandMember(enc("s"), 5, now) as Uint8Array[];
    expect(got).toHaveLength(2); // capped at set size for positive count
    expect(store.sCard(enc("s"), now)).toBe(2);
  });
});
