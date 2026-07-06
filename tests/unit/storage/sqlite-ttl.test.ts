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

  test("lazy expiry: an expired key reads as absent", () => {
    store.kvSet(enc("k"), enc("v"), now);
    store.expireSet(enc("k"), now + 10, now);
    const later = now + 20;
    expect(store.kvGet(enc("k"), later)).toBeNull();
    expect(store.exists(enc("k"), later)).toBe(false);
  });

  test("reads do NOT physically delete: the row survives until a sweep", () => {
    // #3 decoupling: logical expiry (reads-as-absent) is separate from physical
    // delete (reaper-owned). Observe the side effect — after a read treats the
    // key as gone, the row is still physically present, so the sweep reclaims it.
    store.kvSet(enc("k"), enc("v"), now);
    store.expireSet(enc("k"), now + 10, now);
    const later = now + 20;
    expect(store.kvGet(enc("k"), later)).toBeNull(); // logically absent
    // A second sweep finds nothing (idempotent), proving the first did the work.
    expect(store.sweepExpired(later)).toBe(1); // physical row was still there
    expect(store.sweepExpired(later)).toBe(0);
  });

  describe("write-path revive guard (expired key is purged before recreate)", () => {
    test("expired string overwritten by a fresh SET reads cleanly", () => {
      store.kvSet(enc("k"), enc("old"), now);
      store.expireSet(enc("k"), now + 10, now);
      const later = now + 20;
      store.kvSet(enc("k"), enc("new"), later); // revive, no TTL
      expect(store.kvGet(enc("k"), later)).toEqual(enc("new"));
      expect(store.pttl(enc("k"), later)).toBe(-1); // old TTL gone
      expect(store.sweepExpired(later + 1000)).toBe(0); // nothing stale left
    });

    test("expired hash revived as a set: no orphaned fields, type flips", () => {
      store.hSet(enc("k"), [[enc("f"), enc("v")]], now);
      store.expireSet(enc("k"), now + 10, now);
      const later = now + 20;
      // SADD on the logically-expired key must start from a clean slate, not
      // resurrect the old hash field rows under a now-'set' key.
      store.sAdd(enc("k"), [enc("m")], later);
      expect(store.typeOf(enc("k"), later)).toBe("set");
      expect(store.sMembers(enc("k"), later)).toEqual([enc("m")]);
      // Old hash field must not survive: hGet would throw WRONGTYPE now, so
      // assert via a sweep that no stray rows linger after deleting the key.
      expect(store.del([enc("k")], later)).toBe(1);
      expect(store.sweepExpired(later + 1000)).toBe(0);
    });

    test("expired hash revived as a hash starts empty (no field resurrection)", () => {
      store.hSet(enc("k"), [[enc("old"), enc("1")]], now);
      store.expireSet(enc("k"), now + 10, now);
      const later = now + 20;
      store.hSet(enc("k"), [[enc("new"), enc("2")]], later);
      expect(store.hGetAll(enc("k"), later)).toEqual([[enc("new"), enc("2")]]);
    });
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
