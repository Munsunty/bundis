import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";
import { spawnServer } from "../../src";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("pub/sub", () => {
  test("a subscriber receives published messages on its channel", async () => {
    const sub = await h.makeClient();
    const received: Array<{ msg: string; channel: string }> = [];
    await sub.subscribe("news", (msg: string, channel: string) => {
      received.push({ msg, channel });
    });

    const n = await h.client.publish("news", "hello");
    expect(n).toBe(1);
    await sleep(50);
    expect(received).toEqual([{ msg: "hello", channel: "news" }]);
  });

  test("pattern subscriptions match by glob (delivery count)", async () => {
    const sub = await h.makeClient();
    await sub.send("PSUBSCRIBE", ["news.*"]);
    await sleep(10);
    expect(await h.client.publish("news.sports", "goal")).toBe(1);
    expect(await h.client.publish("weather.now", "rain")).toBe(0);
    sub.close();
  });

  test("after unsubscribe no further messages arrive", async () => {
    const sub = await h.makeClient();
    const received: string[] = [];
    await sub.subscribe("ch", (msg: string) => received.push(msg));
    await h.client.publish("ch", "first");
    await sleep(30);
    await sub.unsubscribe("ch");
    await sleep(10);
    await h.client.publish("ch", "second");
    await sleep(30);
    expect(received).toEqual(["first"]);
  });

  test("PUBLISH to a channel with no subscribers returns 0", async () => {
    expect(await h.client.publish("empty", "x")).toBe(0);
  });
});

describe("pub/sub across a spawned (separate-process) server", () => {
  // The hub routes between connections inside one server process; the two
  // clients living in distinct OS processes is irrelevant as long as both
  // point at the *same* spawned server. (Two separate servers would NOT
  // exchange messages — multi-process broadcast is a non-goal, §6.2.)
  test("publisher and subscriber on separate clients exchange a message", async () => {
    const server = await spawnServer({ port: 0, dbPath: ":memory:" });
    const pub = new RedisClient(server.url);
    const sub = new RedisClient(server.url);
    try {
      await pub.connect();
      await sub.connect();

      const received: Array<{ msg: string; channel: string }> = [];
      await sub.subscribe("jobs", (msg: string, channel: string) => {
        received.push({ msg, channel });
      });

      const n = await pub.publish("jobs", "task-1");
      expect(n).toBe(1);
      await sleep(50);
      expect(received).toEqual([{ msg: "task-1", channel: "jobs" }]);
    } finally {
      pub.close();
      sub.close();
      await server.stop();
    }
  });
});
