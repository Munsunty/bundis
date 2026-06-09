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

describe("SqliteStorage withTransaction", () => {
  test("commits all writes on success", () => {
    store.withTransaction(() => {
      store.kvSet(enc("a"), enc("1"), now);
      store.kvSet(enc("b"), enc("2"), now);
    });
    expect(dec(store.kvGet(enc("a"), now))).toBe("1");
    expect(dec(store.kvGet(enc("b"), now))).toBe("2");
  });

  test("rolls back every write when the body throws", () => {
    expect(() =>
      store.withTransaction(() => {
        store.kvSet(enc("a"), enc("1"), now);
        throw new Error("boom");
      }),
    ).toThrow("boom");
    expect(store.kvGet(enc("a"), now)).toBeNull();
  });
});
