import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b));

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("SqliteStorage hash", () => {
  test("hSet returns count of new fields only", () => {
    expect(store.hSet(enc("h"), [[enc("a"), enc("1")], [enc("b"), enc("2")]], now)).toBe(2);
    expect(store.hSet(enc("h"), [[enc("a"), enc("9")], [enc("c"), enc("3")]], now)).toBe(1);
    expect(dec(store.hGet(enc("h"), enc("a"), now))).toBe("9");
  });

  test("hGetAll returns all field/value pairs", () => {
    store.hSet(enc("h"), [[enc("a"), enc("1")], [enc("b"), enc("2")]], now);
    const all = store.hGetAll(enc("h"), now).map(([f, v]) => [dec(f), dec(v)]);
    expect(all).toEqual([
      ["a", "1"],
      ["b", "2"],
    ]);
  });

  test("hDel removes fields; emptying the hash deletes the key", () => {
    store.hSet(enc("h"), [[enc("a"), enc("1")]], now);
    expect(store.hDel(enc("h"), [enc("a")], now)).toBe(1);
    expect(store.exists(enc("h"), now)).toBe(false);
  });

  test("hIncrBy starts from zero and accumulates", () => {
    expect(store.hIncrBy(enc("h"), enc("n"), 3n, now)).toBe(3n);
    expect(store.hIncrBy(enc("h"), enc("n"), 4n, now)).toBe(7n);
  });

  test("hExists / hLen", () => {
    store.hSet(enc("h"), [[enc("a"), enc("1")]], now);
    expect(store.hExists(enc("h"), enc("a"), now)).toBe(true);
    expect(store.hExists(enc("h"), enc("z"), now)).toBe(false);
    expect(store.hLen(enc("h"), now)).toBe(1);
  });
});
