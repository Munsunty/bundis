import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

let h: Harness;
beforeEach(async () => {
  h = await startHarness({ reaperIntervalMs: 20 });
});
afterEach(() => closeHarness(h));

describe("expiry", () => {
  test("expire then ttl reports a positive value", async () => {
    await h.client.set("k", "v");
    expect(await h.client.expire("k", 100)).toBe(1);
    expect(await h.client.ttl("k")).toBeGreaterThan(90);
  });

  test("ttl is -2 for missing key, -1 with no expiry", async () => {
    expect(await h.client.ttl("missing")).toBe(-2);
    await h.client.set("k", "v");
    expect(await h.client.ttl("k")).toBe(-1);
  });

  test("persist removes the TTL", async () => {
    await h.client.set("k", "v");
    await h.client.expire("k", 100);
    expect(await h.client.persist("k")).toBe(1);
    expect(await h.client.ttl("k")).toBe(-1);
  });

  test("lazy expiry: an expired key reads as absent", async () => {
    await h.client.set("k", "v");
    await h.client.send("PEXPIRE", ["k", "30"]);
    await sleep(60);
    expect(await h.client.get("k")).toBeNull();
  });

  test("active expiry: the reaper removes an untouched expired key", async () => {
    await h.client.set("k", "v");
    await h.client.send("PEXPIRE", ["k", "30"]);
    await sleep(120); // give the reaper (20ms) time to sweep
    expect(await h.client.exists("k")).toBe(false);
  });

  test("lazy expiry applies to list and zset keys", async () => {
    await h.client.send("RPUSH", ["l", "a"]);
    await h.client.send("ZADD", ["z", "1", "a"]);
    expect(await h.client.expire("l", 100)).toBe(1);
    expect(await h.client.ttl("z")).toBe(-1);
    await h.client.send("PEXPIRE", ["l", "30"]);
    await h.client.send("PEXPIRE", ["z", "30"]);
    await sleep(60);
    expect(await h.client.send("LRANGE", ["l", "0", "-1"])).toEqual([]);
    expect(await h.client.send("LPOP", ["l"])).toBeNull();
    expect(await h.client.send("ZSCORE", ["z", "a"])).toBeNull();
    expect(await h.client.send("ZRANGE", ["z", "0", "-1"])).toEqual([]);
  });

  test("active expiry reaps untouched list and zset keys", async () => {
    await h.client.send("RPUSH", ["l", "a"]);
    await h.client.send("ZADD", ["z", "1", "a"]);
    await h.client.send("PEXPIRE", ["l", "30"]);
    await h.client.send("PEXPIRE", ["z", "30"]);
    await sleep(120); // give the reaper (20ms) time to sweep
    expect(await h.client.exists("l")).toBe(false);
    expect(await h.client.exists("z")).toBe(false);
    expect(await h.client.send("DBSIZE", [])).toBe(0);
  });
});
