import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

/**
 * §2.4 mapping rows for the zset commands, asserted through the stock client's
 * raw `send` path. Scores travel as RESP3 doubles; the WITHSCORES shape is the
 * RESP3 pair form ([member, score] arrays) per Redis 7's RESP3 reply schema.
 */

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("zset commands", () => {
  test("zadd returns added count; zcard/zrem are numbers", async () => {
    expect(await h.client.send("ZADD", ["z", "1", "a", "2", "b"])).toBe(2);
    expect(await h.client.send("ZADD", ["z", "9", "a", "3", "c"])).toBe(1); // a updated, not added
    expect(await h.client.send("ZCARD", ["z"])).toBe(3);
    expect(await h.client.send("ZREM", ["z", "a", "nope"])).toBe(1);
    expect(await h.client.send("ZCARD", ["z"])).toBe(2);
  });

  test("zadd CH counts changes; NX/XX/GT/LT guards apply", async () => {
    await h.client.send("ZADD", ["z", "5", "a"]);
    expect(await h.client.send("ZADD", ["z", "CH", "6", "a", "1", "b"])).toBe(2);
    expect(await h.client.send("ZADD", ["z", "NX", "9", "a"])).toBe(0);
    expect(await h.client.send("ZSCORE", ["z", "a"])).toBe(6);
    expect(await h.client.send("ZADD", ["z", "XX", "CH", "7", "a", "1", "new"])).toBe(1);
    expect(await h.client.send("ZADD", ["z", "GT", "CH", "3", "a"])).toBe(0);
    expect(await h.client.send("ZADD", ["z", "LT", "CH", "3", "a"])).toBe(1);
    expect(
      h.client.send("ZADD", ["z", "NX", "GT", "1", "a"]),
    ).rejects.toThrow(/not compatible/);
  });

  test("zadd INCR returns the new score as a number, or null when guarded", async () => {
    expect(await h.client.send("ZADD", ["z", "INCR", "2.5", "a"])).toBe(2.5);
    expect(await h.client.send("ZADD", ["z", "INCR", "1.5", "a"])).toBe(4);
    expect(await h.client.send("ZADD", ["z", "NX", "INCR", "1", "a"])).toBeNull();
    expect(
      h.client.send("ZADD", ["z", "INCR", "1", "a", "2", "b"]),
    ).rejects.toThrow(/single increment-element pair/);
  });

  test("zscore: number with float precision, or null", async () => {
    await h.client.send("ZADD", ["z", "1.5", "a", "2", "b"]);
    expect(await h.client.send("ZSCORE", ["z", "a"])).toBe(1.5);
    expect(await h.client.send("ZSCORE", ["z", "b"])).toBe(2);
    expect(await h.client.send("ZSCORE", ["z", "nope"])).toBeNull();
    expect(await h.client.send("ZSCORE", ["missing", "a"])).toBeNull();
  });

  test("infinity scores survive the wire", async () => {
    await h.client.send("ZADD", ["z", "+inf", "hi", "-inf", "lo"]);
    expect(await h.client.send("ZSCORE", ["z", "hi"])).toBe(Infinity);
    expect(await h.client.send("ZSCORE", ["z", "lo"])).toBe(-Infinity);
  });

  test("zrank: number or null; ties rank by member lexicographic order", async () => {
    await h.client.send("ZADD", ["z", "1", "b", "1", "a", "0", "c"]);
    expect(await h.client.send("ZRANK", ["z", "c"])).toBe(0);
    expect(await h.client.send("ZRANK", ["z", "a"])).toBe(1);
    expect(await h.client.send("ZRANK", ["z", "b"])).toBe(2);
    expect(await h.client.send("ZRANK", ["z", "nope"])).toBeNull();
    expect(await h.client.send("ZRANK", ["missing", "a"])).toBeNull();
  });

  test("zrange/zrevrange: members in order; empty array when missing", async () => {
    await h.client.send("ZADD", ["z", "1", "a", "2", "b", "3", "c"]);
    expect(await h.client.send("ZRANGE", ["z", "0", "-1"])).toEqual(["a", "b", "c"]);
    expect(await h.client.send("ZRANGE", ["z", "-2", "-1"])).toEqual(["b", "c"]);
    expect(await h.client.send("ZREVRANGE", ["z", "0", "-1"])).toEqual(["c", "b", "a"]);
    expect(await h.client.send("ZRANGE", ["missing", "0", "-1"])).toEqual([]);
  });

  test("zrange WITHSCORES: RESP3 [member, score-number] pairs", async () => {
    await h.client.send("ZADD", ["z", "1", "a", "2.5", "b"]);
    expect(await h.client.send("ZRANGE", ["z", "0", "-1", "WITHSCORES"])).toEqual([
      ["a", 1],
      ["b", 2.5],
    ]);
    expect(await h.client.send("ZREVRANGE", ["z", "0", "-1", "WITHSCORES"])).toEqual([
      ["b", 2.5],
      ["a", 1],
    ]);
  });

  test("zrangebyscore: bounds, exclusivity, infinity, LIMIT", async () => {
    await h.client.send("ZADD", ["z", "1", "a", "2", "b", "3", "c"]);
    expect(await h.client.send("ZRANGEBYSCORE", ["z", "2", "3"])).toEqual(["b", "c"]);
    expect(await h.client.send("ZRANGEBYSCORE", ["z", "(2", "3"])).toEqual(["c"]);
    expect(await h.client.send("ZRANGEBYSCORE", ["z", "-inf", "+inf"])).toEqual(["a", "b", "c"]);
    expect(
      await h.client.send("ZRANGEBYSCORE", ["z", "-inf", "+inf", "LIMIT", "1", "1"]),
    ).toEqual(["b"]);
    expect(
      await h.client.send("ZRANGEBYSCORE", ["z", "-inf", "+inf", "LIMIT", "1", "-1"]),
    ).toEqual(["b", "c"]);
    expect(
      await h.client.send("ZRANGEBYSCORE", ["z", "1", "3", "WITHSCORES", "LIMIT", "0", "1"]),
    ).toEqual([["a", 1]]);
    expect(await h.client.send("ZRANGEBYSCORE", ["z", "5", "9"])).toEqual([]);
    expect(h.client.send("ZRANGEBYSCORE", ["z", "junk", "3"])).rejects.toThrow(/not a float/);
  });

  test("removing the last member removes the key", async () => {
    await h.client.send("ZADD", ["z", "1", "a"]);
    expect(await h.client.send("ZREM", ["z", "a"])).toBe(1);
    expect(await h.client.exists("z")).toBe(false);
    expect(await h.client.send("TYPE", ["z"])).toBe("none");
    expect(await h.client.ttl("z")).toBe(-2);
  });

  test("TYPE reports zset", async () => {
    await h.client.send("ZADD", ["z", "1", "a"]);
    expect(await h.client.send("TYPE", ["z"])).toBe("zset");
  });

  test("pipelined zset commands keep arrival order", async () => {
    const [r1, r2, r3] = await Promise.all([
      h.client.send("ZADD", ["z", "1", "a", "2", "b"]),
      h.client.send("ZRANGE", ["z", "0", "-1"]),
      h.client.send("ZSCORE", ["z", "b"]),
    ]);
    expect(r1).toBe(2);
    expect(r2).toEqual(["a", "b"]);
    expect(r3).toBe(2);
  });
});
