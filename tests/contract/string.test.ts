import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("string commands", () => {
  test("set / get", async () => {
    expect(await h.client.set("k", "v")).toBe("OK");
    expect(await h.client.get("k")).toBe("v");
  });

  test("get missing key returns null", async () => {
    expect(await h.client.get("missing")).toBeNull();
  });

  test("del returns number removed", async () => {
    await h.client.set("a", "1");
    await h.client.set("b", "2");
    expect(await h.client.del("a", "b", "c")).toBe(2);
  });

  test("append returns new length", async () => {
    expect(await h.client.append("s", "foo")).toBe(3);
    expect(await h.client.append("s", "bar")).toBe(6);
    expect(await h.client.get("s")).toBe("foobar");
  });

  test("strlen", async () => {
    await h.client.set("s", "hello");
    expect(await h.client.strlen("s")).toBe(5);
  });

  test("getset returns old value", async () => {
    await h.client.set("k", "old");
    expect(await h.client.send("GETSET", ["k", "new"])).toBe("old");
    expect(await h.client.get("k")).toBe("new");
  });

  test("SET with EX sets a positive TTL", async () => {
    await h.client.send("SET", ["k", "v", "EX", "100"]);
    expect(await h.client.ttl("k")).toBeGreaterThan(90);
  });

  test("SET NX only sets when absent", async () => {
    expect(await h.client.send("SET", ["k", "1", "NX"])).toBe("OK");
    expect(await h.client.send("SET", ["k", "2", "NX"])).toBeNull();
    expect(await h.client.get("k")).toBe("1");
  });

  test("SET XX only sets when present", async () => {
    expect(await h.client.send("SET", ["k", "1", "XX"])).toBeNull();
    await h.client.set("k", "0");
    expect(await h.client.send("SET", ["k", "1", "XX"])).toBe("OK");
  });

  test("SET GET returns the previous value", async () => {
    await h.client.set("k", "old");
    expect(await h.client.send("SET", ["k", "new", "GET"])).toBe("old");
    expect(await h.client.get("k")).toBe("new");
  });
});
