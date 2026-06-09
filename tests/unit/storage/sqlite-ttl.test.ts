import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { SqliteStorage } from "../../../src/storage/sqlite";

const enc = (s: string) => new TextEncoder().encode(s);

let store: SqliteStorage;
let now: number;
beforeEach(() => {
  store = new SqliteStorage(":memory:");
  now = Date.now();
});
afterEach(() => store.close());

describe("SqliteStorage ttl", () => {
  test("pttl: -2 missing, -1 no expiry, >0 with expiry", () => {
    expect(store.pttl(enc("k"), now)).toBe(-2);
    store.kvSet(enc("k"), enc("v"), now);
    expect(store.pttl(enc("k"), now)).toBe(-1);
    store.expireSet(enc("k"), now + 5000, now);
    expect(store.pttl(enc("k"), now)).toBeGreaterThan(4000);
  });

  test("lazy expiry: an expired key reads as absent and is removed", () => {
    store.kvSet(enc("k"), enc("v"), now);
    store.expireSet(enc("k"), now + 10, now);
    const later = now + 20;
    expect(store.kvGet(enc("k"), later)).toBeNull();
    expect(store.exists(enc("k"), later)).toBe(false);
  });

  test("persist removes the TTL", () => {
    store.kvSet(enc("k"), enc("v"), now);
    store.expireSet(enc("k"), now + 5000, now);
    expect(store.persist(enc("k"), now)).toBe(true);
    expect(store.pttl(enc("k"), now)).toBe(-1);
    expect(store.persist(enc("k"), now)).toBe(false); // nothing to remove now
  });

  test("expireSet on a missing key returns false", () => {
    expect(store.expireSet(enc("nope"), now + 1000, now)).toBe(false);
  });
});
