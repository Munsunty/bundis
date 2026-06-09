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
});
