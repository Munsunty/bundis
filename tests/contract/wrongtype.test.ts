import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("WRONGTYPE semantics", () => {
  test("hash op on a string key errors with WRONGTYPE", async () => {
    await h.client.set("k", "v");
    await expect(h.client.hget("k", "f")).rejects.toThrow(/WRONGTYPE/);
  });

  test("set op on a string key errors with WRONGTYPE", async () => {
    await h.client.set("k", "v");
    await expect(h.client.sadd("k", "m")).rejects.toThrow(/WRONGTYPE/);
  });

  test("string op on a hash key errors with WRONGTYPE", async () => {
    await h.client.hmset("h", ["f", "1"]);
    await expect(h.client.incr("h")).rejects.toThrow(/WRONGTYPE/);
  });

  test("SET overwrites a key of another type (Redis semantics)", async () => {
    await h.client.sadd("k", "m");
    expect(await h.client.set("k", "now-a-string")).toBe("OK");
    expect(await h.client.get("k")).toBe("now-a-string");
  });

  test("list ops on string/zset keys error with WRONGTYPE", async () => {
    await h.client.set("str", "v");
    expect(h.client.send("LPUSH", ["str", "a"])).rejects.toThrow(/WRONGTYPE/);
    expect(h.client.send("LRANGE", ["str", "0", "-1"])).rejects.toThrow(/WRONGTYPE/);
    await h.client.send("ZADD", ["z", "1", "a"]);
    expect(h.client.send("RPUSH", ["z", "a"])).rejects.toThrow(/WRONGTYPE/);
    expect(h.client.send("LPOP", ["z"])).rejects.toThrow(/WRONGTYPE/);
  });

  test("zset ops on string/list keys error with WRONGTYPE", async () => {
    await h.client.set("str", "v");
    expect(h.client.send("ZADD", ["str", "1", "a"])).rejects.toThrow(/WRONGTYPE/);
    expect(h.client.send("ZSCORE", ["str", "a"])).rejects.toThrow(/WRONGTYPE/);
    await h.client.send("RPUSH", ["l", "a"]);
    expect(h.client.send("ZADD", ["l", "1", "a"])).rejects.toThrow(/WRONGTYPE/);
    expect(h.client.send("ZRANGE", ["l", "0", "-1"])).rejects.toThrow(/WRONGTYPE/);
  });

  test("string ops on list/zset keys error with WRONGTYPE", async () => {
    await h.client.send("RPUSH", ["l", "a"]);
    expect(h.client.get("l")).rejects.toThrow(/WRONGTYPE/);
    await h.client.send("ZADD", ["z", "1", "a"]);
    expect(h.client.incr("z")).rejects.toThrow(/WRONGTYPE/);
  });

  test("SET overwrites list and zset keys (Redis semantics)", async () => {
    await h.client.send("RPUSH", ["l", "a"]);
    expect(await h.client.set("l", "s")).toBe("OK");
    expect(await h.client.get("l")).toBe("s");
    await h.client.send("ZADD", ["z", "1", "a"]);
    expect(await h.client.set("z", "s")).toBe("OK");
    expect(await h.client.send("TYPE", ["z"])).toBe("string");
  });
});
