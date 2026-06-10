import { describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import { embedServer, spawnServer } from "../../src";

describe("launchers", () => {
  test("embedServer runs in-process and serves a stock client", async () => {
    const server = embedServer({ port: 0, dbPath: ":memory:" });
    const client = new RedisClient(server.url);
    try {
      await client.set("k", "embedded");
      expect(await client.get("k")).toBe("embedded");
      expect(server.port).toBeGreaterThan(0);
    } finally {
      client.close();
      server.stop();
    }
  });

  test("spawnServer runs in a separate process and serves a stock client", async () => {
    const server = await spawnServer({ port: 0, dbPath: ":memory:" });
    const client = new RedisClient(server.url);
    try {
      await client.set("k", "spawned");
      expect(await client.get("k")).toBe("spawned");
      expect(server.pid).toBeGreaterThan(0);
      expect(server.pid).not.toBe(process.pid);
    } finally {
      client.close();
      await server.stop();
    }
  });

  test("spawnServer enforces a configured password", async () => {
    const server = await spawnServer({ port: 0, dbPath: ":memory:", password: "sekret" });
    const client = new RedisClient(server.url); // url already carries the password
    try {
      await client.set("k", "authed");
      expect(await client.get("k")).toBe("authed");
    } finally {
      client.close();
      await server.stop();
    }
  });
});
