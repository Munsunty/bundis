import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

/**
 * Differential test (§8): replay the same command sequence against a real
 * Redis and against bundis with the stock client, and deep-compare the JS
 * values it returns. This is what pins the wire-shape hypotheses (RESP3
 * doubles, WITHSCORES pair form, LPOP count on a missing key) to ground truth
 * instead of documentation guesses.
 *
 * Opt-in: set REDIS_URL (e.g. redis://127.0.0.1:6379) — skipped otherwise.
 * The real Redis database is flushed; point it at a throwaway instance.
 */

const REDIS_URL = process.env.REDIS_URL;

const SEQUENCE: Array<[string, string[]]> = [
  ["RPUSH", ["dt:l", "b", "c"]],
  ["LPUSH", ["dt:l", "a"]],
  ["LRANGE", ["dt:l", "0", "-1"]],
  ["LRANGE", ["dt:l", "-2", "-1"]],
  ["LRANGE", ["dt:missing", "0", "-1"]],
  ["LLEN", ["dt:l"]],
  ["LLEN", ["dt:missing"]],
  ["LINDEX", ["dt:l", "-1"]],
  ["LINDEX", ["dt:l", "9"]],
  ["LPOP", ["dt:l"]],
  ["RPOP", ["dt:l", "2"]],
  ["LPOP", ["dt:l"]],
  ["LPOP", ["dt:missing"]],
  ["LPOP", ["dt:missing", "2"]],
  ["EXISTS", ["dt:l"]],
  ["TYPE", ["dt:l"]],
  ["ZADD", ["dt:z", "1", "b", "1", "a", "0.5", "c"]],
  ["ZADD", ["dt:z", "CH", "2", "a", "3", "d"]],
  ["ZADD", ["dt:z", "NX", "9", "a"]],
  ["ZADD", ["dt:z", "XX", "CH", "9", "nope"]],
  ["ZADD", ["dt:z", "GT", "CH", "1", "a"]],
  ["ZADD", ["dt:z", "INCR", "1.5", "a"]],
  ["ZADD", ["dt:z", "NX", "INCR", "1", "a"]],
  ["ZCARD", ["dt:z"]],
  ["ZSCORE", ["dt:z", "a"]],
  ["ZSCORE", ["dt:z", "nope"]],
  ["ZRANK", ["dt:z", "b"]],
  ["ZRANK", ["dt:z", "nope"]],
  ["ZRANGE", ["dt:z", "0", "-1"]],
  ["ZRANGE", ["dt:z", "0", "-1", "WITHSCORES"]],
  ["ZREVRANGE", ["dt:z", "0", "1", "WITHSCORES"]],
  ["ZRANGEBYSCORE", ["dt:z", "(0.5", "+inf"]],
  ["ZRANGEBYSCORE", ["dt:z", "-inf", "+inf", "WITHSCORES", "LIMIT", "1", "2"]],
  ["ZRANGEBYSCORE", ["dt:missing", "-inf", "+inf"]],
  ["ZREM", ["dt:z", "a", "nope"]],
  ["ZREM", ["dt:z", "b", "c", "d"]],
  ["EXISTS", ["dt:z"]],
  ["TYPE", ["dt:z"]],
];

describe.skipIf(!REDIS_URL)("differential: real Redis vs bundis", () => {
  let h: Harness;
  let real: RedisClient;
  beforeEach(async () => {
    h = await startHarness();
    real = new RedisClient(REDIS_URL!);
    await real.connect();
    await real.send("FLUSHDB", []);
  });
  afterEach(() => {
    real.close();
    closeHarness(h);
  });

  test("identical command sequence yields identical client JS values", async () => {
    for (const [cmd, args] of SEQUENCE) {
      const fromBundis = await h.client.send(cmd, args).then(
        (v) => ({ ok: v }),
        (e: Error) => ({ err: String(e) }),
      );
      const fromRedis = await real.send(cmd, args).then(
        (v) => ({ ok: v }),
        (e: Error) => ({ err: String(e) }),
      );
      expect({ cmd, args, reply: fromBundis }).toEqual({ cmd, args, reply: fromRedis });
    }
  });
});
