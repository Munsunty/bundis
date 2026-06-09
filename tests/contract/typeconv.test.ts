import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

/**
 * The §2.4 type-conversion contract, asserted through the stock client. This is
 * the file that pins our reply types: if any handler emits the wrong RESP type,
 * the JS value the client returns changes shape and one of these fails.
 */

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("type-conversion contract (§2.4)", () => {
  test("EXISTS returns a boolean", async () => {
    await h.client.set("k", "v");
    const yes = await h.client.exists("k");
    const no = await h.client.exists("nope");
    expect(typeof yes).toBe("boolean");
    expect(yes).toBe(true);
    expect(no).toBe(false);
  });

  test("SISMEMBER returns a boolean", async () => {
    await h.client.sadd("s", "a");
    expect(await h.client.sismember("s", "a")).toBe(true);
    expect(await h.client.sismember("s", "z")).toBe(false);
  });

  test("HEXISTS returns a boolean", async () => {
    await h.client.hmset("h", ["f", "1"]);
    expect(await h.client.hexists("h", "f")).toBe(true);
    expect(await h.client.hexists("h", "z")).toBe(false);
  });

  test("GET miss returns null", async () => {
    expect(await h.client.get("absent")).toBeNull();
  });

  test("getBuffer returns a Uint8Array", async () => {
    await h.client.set("k", "v");
    const buf = await h.client.getBuffer("k");
    expect(buf).toBeInstanceOf(Uint8Array);
    expect([...buf!]).toEqual([118]); // 'v'
  });

  test("HGETALL returns an object", async () => {
    await h.client.hmset("h", ["f1", "1", "f2", "2"]);
    const all = await h.client.hgetall("h");
    expect(Array.isArray(all)).toBe(false);
    expect(all).toEqual({ f1: "1", f2: "2" });
  });

  test("SMEMBERS returns an array", async () => {
    await h.client.sadd("s", "a", "b");
    const members = await h.client.smembers("s");
    expect(Array.isArray(members)).toBe(true);
    expect([...members].sort()).toEqual(["a", "b"]);
  });

  test("MGET returns (string|null)[]", async () => {
    await h.client.set("a", "1");
    const got = await h.client.mget("a", "missing");
    expect(got).toEqual(["1", null]);
  });

  test("INCR returns a number, INCRBYFLOAT returns a string", async () => {
    expect(await h.client.incr("n")).toBe(1);
    expect(typeof (await h.client.incr("n"))).toBe("number");
    expect(await h.client.send("INCRBYFLOAT", ["f", "1.5"])).toBe("1.5");
  });

  test("SET NX failure returns null", async () => {
    await h.client.set("k", "1");
    expect(await h.client.send("SET", ["k", "2", "NX"])).toBeNull();
  });
});
