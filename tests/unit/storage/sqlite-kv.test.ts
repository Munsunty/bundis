import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";
import { NotIntegerError, TypeMismatchError } from "../../../src/engine/errors";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b));

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("SqliteStorage kv", () => {
  test("set / get round trip", () => {
    store.kvSet(enc("k"), enc("v"), now);
    expect(dec(store.kvGet(enc("k"), now))).toBe("v");
  });

  test("get missing key is null", () => {
    expect(store.kvGet(enc("missing"), now)).toBeNull();
  });

  test("binary value round trip preserves bytes", () => {
    const val = new Uint8Array([0, 13, 10, 255]);
    store.kvSet(enc("b"), val, now);
    expect([...store.kvGet(enc("b"), now)!]).toEqual([0, 13, 10, 255]);
  });

  test("NX only sets when absent; XX only when present", () => {
    expect(store.kvSet(enc("k"), enc("1"), now, { mode: "NX" })).toBe("set");
    expect(store.kvSet(enc("k"), enc("2"), now, { mode: "NX" })).toBe("noop");
    expect(dec(store.kvGet(enc("k"), now))).toBe("1");
    expect(store.kvSet(enc("y"), enc("1"), now, { mode: "XX" })).toBe("noop");
  });

  test("incrBy is atomic and counts from zero", () => {
    expect(store.incrBy(enc("n"), 1n, now)).toBe(1n);
    expect(store.incrBy(enc("n"), 5n, now)).toBe(6n);
    expect(store.incrBy(enc("n"), -10n, now)).toBe(-4n);
  });

  test("incrBy throws on non-integer value", () => {
    store.kvSet(enc("s"), enc("abc"), now);
    expect(() => store.incrBy(enc("s"), 1n, now)).toThrow(NotIntegerError);
  });

  test("append returns new length and concatenates", () => {
    expect(store.append(enc("a"), enc("foo"), now)).toBe(3);
    expect(store.append(enc("a"), enc("bar"), now)).toBe(6);
    expect(dec(store.kvGet(enc("a"), now))).toBe("foobar");
  });

  test("reading a hash key as string throws TypeMismatchError", () => {
    store.hSet(enc("h"), [[enc("f"), enc("v")]], now);
    expect(() => store.kvGet(enc("h"), now)).toThrow(TypeMismatchError);
  });

  test("del removes keys and reports count", () => {
    store.kvSet(enc("a"), enc("1"), now);
    store.kvSet(enc("b"), enc("2"), now);
    expect(store.del([enc("a"), enc("b"), enc("c")], now)).toBe(2);
    expect(store.exists(enc("a"), now)).toBe(false);
  });
});
