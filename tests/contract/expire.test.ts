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
});
