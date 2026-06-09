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
});
