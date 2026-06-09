import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

/**
 * The stock client auto-pipelines concurrent commands. The server must process
 * them in arrival order and reply in the same order (§4.1.2), or Promise.all
 * results come back mismatched.
 */

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("auto-pipelining", () => {
  test("many concurrent INCRs total correctly", async () => {
    const N = 100;
    const results = await Promise.all(
      Array.from({ length: N }, () => h.client.incr("counter")),
    );
    expect(results.sort((a, b) => a - b)).toEqual(
      Array.from({ length: N }, (_, i) => i + 1),
    );
    expect(await h.client.get("counter")).toBe(String(N));
  });

  test("interleaved sets and gets stay consistent and ordered", async () => {
    const keys = Array.from({ length: 50 }, (_, i) => `k${i}`);
    await Promise.all(keys.map((k, i) => h.client.set(k, String(i))));
    const got = await Promise.all(keys.map((k) => h.client.get(k)));
    expect(got).toEqual(keys.map((_, i) => String(i)));
  });

  test("replies match request order across mixed command types", async () => {
    await h.client.set("s", "hello");
    await h.client.sadd("set", "a", "b");
    const [ping, val, len, card] = await Promise.all([
      h.client.ping(),
      h.client.get("s"),
      h.client.strlen("s"),
      h.client.scard("set"),
    ]);
    expect(ping).toBe("PONG");
    expect(val).toBe("hello");
    expect(len).toBe(5);
    expect(card).toBe(2);
  });
});
