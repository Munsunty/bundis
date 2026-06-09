import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../src/storage/sqlite";

const enc = (s: string) => new TextEncoder().encode(s);

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("active expiry sweep", () => {
  test("sweepExpired deletes only expired rows and reports the count", () => {
    store.kvSet(enc("live"), enc("1"), now);
    store.kvSet(enc("soon"), enc("2"), now);
    store.expireSet(enc("soon"), now + 10, now);
    store.kvSet(enc("kept"), enc("3"), now);
    store.expireSet(enc("kept"), now + 100_000, now);

    const swept = store.sweepExpired(now + 50);
    expect(swept).toBe(1);
    expect(store.exists(enc("soon"), now + 50)).toBe(false);
    expect(store.exists(enc("live"), now + 50)).toBe(true);
    expect(store.exists(enc("kept"), now + 50)).toBe(true);
  });
});
