import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

/**
 * MULTI/EXEC/DISCARD/WATCH driven over send() (the stock client doesn't expose
 * dedicated transaction methods). These commands disable auto-pipelining, so
 * each resolves before the next is sent.
 */

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("transactions", () => {
  test("MULTI queues then EXEC returns the result array", async () => {
    expect(await h.client.send("MULTI", [])).toBe("OK");
    expect(await h.client.send("SET", ["k", "1"])).toBe("QUEUED");
    expect(await h.client.send("INCR", ["k"])).toBe("QUEUED");
    const results = await h.client.send("EXEC", []);
    expect(results).toEqual(["OK", 2]);
    expect(await h.client.get("k")).toBe("2");
  });

  test("DISCARD throws away the queue", async () => {
    await h.client.send("MULTI", []);
    await h.client.send("SET", ["k", "1"]);
    expect(await h.client.send("DISCARD", [])).toBe("OK");
    expect(await h.client.get("k")).toBeNull();
  });

  test("WATCH + external change aborts EXEC (nil)", async () => {
    await h.client.set("k", "1");
    await h.client.send("WATCH", ["k"]);
    // A different connection mutates the watched key.
    const other = await h.makeClient();
    await other.set("k", "changed");
    other.close();

    await h.client.send("MULTI", []);
    await h.client.send("SET", ["k", "2"]);
    const res = await h.client.send("EXEC", []);
    expect(res).toBeNull();
    expect(await h.client.get("k")).toBe("changed"); // our SET did not run
  });

  test("WATCH with no change commits normally", async () => {
    await h.client.set("k", "1");
    await h.client.send("WATCH", ["k"]);
    await h.client.send("MULTI", []);
    await h.client.send("INCR", ["k"]);
    const res = await h.client.send("EXEC", []);
    expect(res).toEqual([2]);
  });
});
