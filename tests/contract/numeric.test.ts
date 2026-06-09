import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("numeric commands", () => {
  test("incr / decr / incrby / decrby", async () => {
    expect(await h.client.incr("n")).toBe(1);
    expect(await h.client.incr("n")).toBe(2);
    expect(await h.client.decr("n")).toBe(1);
    expect(await h.client.send("INCRBY", ["n", "10"])).toBe(11);
    expect(await h.client.send("DECRBY", ["n", "5"])).toBe(6);
  });

  test("incrbyfloat returns a string", async () => {
    expect(await h.client.send("INCRBYFLOAT", ["f", "1.5"])).toBe("1.5");
    expect(await h.client.send("INCRBYFLOAT", ["f", "2.5"])).toBe("4");
  });

  test("INCR on a non-integer value raises an error", async () => {
    await h.client.set("s", "abc");
    await expect(h.client.incr("s")).rejects.toBeDefined();
  });
});
