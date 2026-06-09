import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("set commands", () => {
  test("sadd / scard / smembers", async () => {
    expect(await h.client.sadd("s", "a", "b", "a")).toBe(2);
    expect(await h.client.scard("s")).toBe(2);
    expect([...(await h.client.smembers("s"))].sort()).toEqual(["a", "b"]);
  });

  test("sismember", async () => {
    await h.client.sadd("s", "a");
    expect(await h.client.sismember("s", "a")).toBe(true);
    expect(await h.client.sismember("s", "z")).toBe(false);
  });

  test("srem", async () => {
    await h.client.sadd("s", "a", "b");
    expect(await h.client.srem("s", "a")).toBe(1);
    expect(await h.client.scard("s")).toBe(1);
  });

  test("spop with count removes members", async () => {
    await h.client.sadd("s", "a", "b", "c");
    const popped = await h.client.spop("s", 2);
    expect(Array.isArray(popped)).toBe(true);
    expect((popped as string[]).length).toBe(2);
    expect(await h.client.scard("s")).toBe(1);
  });

  test("srandmember with positive count does not remove", async () => {
    await h.client.sadd("s", "a", "b");
    const got = await h.client.srandmember("s", 5);
    expect((got as string[]).length).toBe(2);
    expect(await h.client.scard("s")).toBe(2);
  });
});
