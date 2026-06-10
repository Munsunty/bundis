import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("admin commands", () => {
  test("TYPE reports string/hash/set/none", async () => {
    await h.client.set("s", "v");
    await h.client.hset("h", "f", "v");
    await h.client.sadd("z", "m");
    expect(await h.client.send("TYPE", ["s"])).toBe("string");
    expect(await h.client.send("TYPE", ["h"])).toBe("hash");
    expect(await h.client.send("TYPE", ["z"])).toBe("set");
    expect(await h.client.send("TYPE", ["missing"])).toBe("none");
  });

  test("DBSIZE counts live keys", async () => {
    expect(await h.client.send("DBSIZE", [])).toBe(0);
    await h.client.set("a", "1");
    await h.client.set("b", "2");
    expect(await h.client.send("DBSIZE", [])).toBe(2);
  });

  test("FLUSHDB clears everything (the beforeEach idiom)", async () => {
    await h.client.set("a", "1");
    await h.client.hset("h", "f", "v");
    expect(await h.client.send("FLUSHDB", [])).toBe("OK");
    expect(await h.client.send("DBSIZE", [])).toBe(0);
    expect(await h.client.get("a")).toBeNull();
    expect(await h.client.send("FLUSHALL", ["SYNC"])).toBe("OK");
  });

  test("FLUSHALL dirties WATCH (EXEC must abort)", async () => {
    await h.client.set("w", "1");
    const c2 = await h.makeClient();
    await c2.send("WATCH", ["w"]);
    await c2.send("MULTI", []);
    await c2.set("w", "2");
    await h.client.send("FLUSHALL", []); // other connection flushes
    expect(await c2.send("EXEC", [])).toBeNull(); // optimistic lock fails
  });

  test("CONFIG GET answers init-time probes", async () => {
    const all = (await h.client.send("CONFIG", ["GET", "maxmemory*"])) as Record<string, string>;
    expect(all.maxmemory).toBeDefined();
    expect(all["maxmemory-policy"]).toBe("noeviction");
    expect(await h.client.send("CONFIG", ["SET", "appendonly", "yes"])).toBe("OK");
  });

  test("COMMAND COUNT > 0, COMMAND tolerated", async () => {
    expect(await h.client.send("COMMAND", [])).toEqual([]);
    expect((await h.client.send("COMMAND", ["COUNT"])) as number).toBeGreaterThan(50);
  });
});

describe("compat fixes", () => {
  test("EXPIRE NX/XX/GT/LT honored", async () => {
    await h.client.set("k", "v");
    expect(await h.client.send("EXPIRE", ["k", "100", "XX"])).toBe(0); // no TTL yet
    expect(await h.client.send("EXPIRE", ["k", "100", "NX"])).toBe(1); // sets
    expect(await h.client.send("EXPIRE", ["k", "200", "NX"])).toBe(0); // TTL exists
    expect(await h.client.send("EXPIRE", ["k", "50", "GT"])).toBe(0); // 50 < 100
    expect(await h.client.send("EXPIRE", ["k", "300", "GT"])).toBe(1); // 300 > 100
    expect(await h.client.send("EXPIRE", ["k", "400", "LT"])).toBe(0); // 400 > 300
    expect(await h.client.send("EXPIRE", ["k", "10", "LT"])).toBe(1);
    expect(await h.client.ttl("k")).toBeLessThanOrEqual(10);
    // GT on a key with no TTL never applies (infinity)
    await h.client.set("p", "v");
    expect(await h.client.send("EXPIRE", ["p", "100", "GT"])).toBe(0);
    expect(await h.client.send("EXPIRE", ["p", "100", "LT"])).toBe(1);
  });

  test("SET with EX/PX <= 0 errors like Redis", async () => {
    expect(h.client.send("SET", ["k", "v", "EX", "0"])).rejects.toThrow(/invalid expire time/);
    expect(h.client.send("SET", ["k", "v", "PX", "-1"])).rejects.toThrow(/invalid expire time/);
    expect(await h.client.get("k")).toBeNull();
  });

  test("SELECT rejects non-zero DB index instead of silently mapping", async () => {
    expect(await h.client.send("SELECT", ["0"])).toBe("OK");
    expect(h.client.send("SELECT", ["1"])).rejects.toThrow(/out of range/);
  });

  test("SUBSCRIBE inside MULTI is rejected and aborts EXEC", async () => {
    await h.client.send("MULTI", []);
    expect(h.client.send("SUBSCRIBE", ["ch"])).rejects.toThrow(/not allowed in transactions/);
    expect(h.client.send("EXEC", [])).rejects.toThrow(/EXECABORT/);
  });

  test("PUBSUB NUMPAT returns an integer", async () => {
    expect(await h.client.send("PUBSUB", ["NUMPAT"])).toBe(0);
  });
});
