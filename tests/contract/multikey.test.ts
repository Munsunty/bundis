import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("multi-key commands", () => {
  test("mset / mget", async () => {
    expect(await h.client.send("MSET", ["a", "1", "b", "2"])).toBe("OK");
    expect(await h.client.mget("a", "b", "missing")).toEqual(["1", "2", null]);
  });

  test("msetnx fails if any key exists", async () => {
    await h.client.set("a", "0");
    expect(await h.client.send("MSETNX", ["a", "1", "b", "2"])).toBe(0);
    expect(await h.client.get("b")).toBeNull(); // atomic: nothing was written
  });

  test("msetnx succeeds when all keys are absent", async () => {
    expect(await h.client.send("MSETNX", ["x", "1", "y", "2"])).toBe(1);
    expect(await h.client.mget("x", "y")).toEqual(["1", "2"]);
  });

  test("setex sets a value with TTL", async () => {
    expect(await h.client.send("SETEX", ["k", "100", "v"])).toBe("OK");
    expect(await h.client.get("k")).toBe("v");
    expect(await h.client.ttl("k")).toBeGreaterThan(90);
  });

  test("setnx", async () => {
    expect(await h.client.send("SETNX", ["k", "1"])).toBe(1);
    expect(await h.client.send("SETNX", ["k", "2"])).toBe(0);
    expect(await h.client.get("k")).toBe("1");
  });
});
