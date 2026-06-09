import { afterEach, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
afterEach(() => h && closeHarness(h));

describe("handshake / connection", () => {
  test("connect succeeds and PING returns PONG", async () => {
    h = await startHarness();
    expect(await h.client.ping()).toBe("PONG");
  });

  test("PING with an argument echoes it", async () => {
    h = await startHarness();
    expect(await h.client.send("PING", ["hello"])).toBe("hello");
  });

  test("SELECT is accepted", async () => {
    h = await startHarness();
    expect(await h.client.send("SELECT", ["0"])).toBe("OK");
  });

  test("INFO reports a redis_version", async () => {
    h = await startHarness();
    const info = (await h.client.send("INFO", [])) as string;
    expect(info).toContain("redis_version:");
  });

  test("ECHO round-trips", async () => {
    h = await startHarness();
    expect(await h.client.send("ECHO", ["abc"])).toBe("abc");
  });

  test("password-protected server: wrong password fails, correct succeeds", async () => {
    h = await startHarness({ password: "s3cret" });
    // The harness client connected with the correct password already.
    expect(await h.client.ping()).toBe("PONG");

    const bad = new RedisClient(`redis://:wrong@127.0.0.1:${h.port}`);
    await expect(bad.connect()).rejects.toBeDefined();
    bad.close();
  });
});
