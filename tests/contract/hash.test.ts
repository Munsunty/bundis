import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("hash commands", () => {
  test("hset returns new field count; hget reads back", async () => {
    expect(await h.client.hset("h", { a: "1", b: "2" })).toBe(2);
    expect(await h.client.hget("h", "a")).toBe("1");
  });

  test("hmget returns values and nulls", async () => {
    await h.client.hmset("h", ["a", "1"]);
    expect(await h.client.hmget("h", "a", "missing")).toEqual(["1", null]);
  });

  test("hgetall returns an object", async () => {
    await h.client.hmset("h", ["a", "1", "b", "2"]);
    expect(await h.client.hgetall("h")).toEqual({ a: "1", b: "2" });
  });

  test("hdel / hlen / hkeys / hvals", async () => {
    await h.client.hmset("h", ["a", "1", "b", "2"]);
    expect(await h.client.hdel("h", "a")).toBe(1);
    expect(await h.client.hlen("h")).toBe(1);
    expect(await h.client.hkeys("h")).toEqual(["b"]);
    expect(await h.client.hvals("h")).toEqual(["2"]);
  });

  test("hincrby / hincrbyfloat", async () => {
    expect(await h.client.hincrby("h", "n", 3)).toBe(3);
    expect(await h.client.hincrby("h", "n", 4)).toBe(7);
    expect(await h.client.send("HINCRBYFLOAT", ["h", "f", "1.5"])).toBe("1.5");
  });

  test("hexists", async () => {
    await h.client.hmset("h", ["a", "1"]);
    expect(await h.client.hexists("h", "a")).toBe(true);
    expect(await h.client.hexists("h", "z")).toBe(false);
  });
});
