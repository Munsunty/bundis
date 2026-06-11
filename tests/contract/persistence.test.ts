import { afterEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";
import { mkTempDbPath, cleanupDb } from "../helpers/temp-db";

/**
 * Data must survive a process restart (§1): write to a file-backed DB, stop the
 * server, start a new one on the same file, and read the values back.
 */

let dbPath: string;
let h: Harness;
afterEach(() => {
  if (h) closeHarness(h);
  if (dbPath) cleanupDb(dbPath);
});

describe("persistence across restart", () => {
  test("string, hash and set values survive a restart", async () => {
    dbPath = mkTempDbPath();

    h = await startHarness({ dbPath });
    await h.client.set("str", "value");
    await h.client.hmset("h", ["f", "1"]);
    await h.client.sadd("s", "a", "b");
    closeHarness(h);

    // Re-open the same file in a fresh server.
    h = await startHarness({ dbPath });
    expect(await h.client.get("str")).toBe("value");
    expect(await h.client.hget("h", "f")).toBe("1");
    expect([...(await h.client.smembers("s"))].sort()).toEqual(["a", "b"]);
  });

  test("list order and zset scores survive a restart", async () => {
    dbPath = mkTempDbPath();

    h = await startHarness({ dbPath });
    await h.client.send("RPUSH", ["l", "b", "c"]);
    await h.client.send("LPUSH", ["l", "a"]);
    await h.client.send("ZADD", ["z", "1.5", "m1", "2", "m2"]);
    closeHarness(h);

    h = await startHarness({ dbPath });
    expect(await h.client.send("LRANGE", ["l", "0", "-1"])).toEqual(["a", "b", "c"]);
    expect(await h.client.send("ZSCORE", ["z", "m1"])).toBe(1.5);
    expect(await h.client.send("ZRANGE", ["z", "0", "-1", "WITHSCORES"])).toEqual([
      ["m1", 1.5],
      ["m2", 2],
    ]);
    // Push/pop still work against the recovered seq bounds.
    expect(await h.client.send("LPUSH", ["l", "z"])).toBe(4);
    expect(await h.client.send("RPOP", ["l"])).toBe("c");
  });
});
