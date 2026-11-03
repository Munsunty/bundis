/**
 * Worker maintenance mode, verified through the stock Bun.RedisClient.
 *
 * While a cycle runs, writes buffer in memory and the worker thread reclaims
 * rows on its own connection; the event loop must keep answering, every
 * buffered write must land in SQLite afterwards (and survive a clean stop),
 * and the runtime knobs must be adjustable via CONFIG SET.
 */

import { afterEach, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import { existsSync, writeFileSync } from "node:fs";
import { closeHarness, startHarness, type Harness } from "../helpers/server-harness";
import { cleanupDb, mkTempDbPath } from "../helpers/temp-db";
import { lockPath } from "../../src/sidecar/lockfile";

type Status = Record<string, string | number>;

const QUIET = { "maint-interval-sec": "0", "maint-expired-threshold": "0" };

const opened: Array<{ h: Harness; path: string }> = [];
afterEach(() => {
  for (const { h, path } of opened.splice(0)) {
    try {
      closeHarness(h);
    } catch {
      // already stopped
    }
    cleanupDb(path);
    cleanupDb(lockPath(path));
  }
});

async function workerServer(params: Record<string, string> = {}, path = mkTempDbPath()): Promise<Harness> {
  const h = await startHarness({ dbPath: path, maintenance: "worker", maintParams: { ...QUIET, ...params } });
  opened.push({ h, path });
  return h;
}

async function status(c: RedisClient): Promise<Status> {
  return (await c.send("MAINT", ["STATUS"])) as Status;
}

async function bigHash(c: RedisClient, key: string, fields: number): Promise<void> {
  for (let i = 0; i < fields; i += 10_000) {
    const args = [key];
    for (let j = i; j < Math.min(fields, i + 10_000); j++) args.push(`f${j}`, "x".repeat(40));
    await c.send("HSET", args);
  }
}

async function untilState(c: RedisClient, want: string, cyclesAtLeast = 1, timeoutMs = 20_000): Promise<Status> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const st = await status(c);
    if (st.maint_state === want && Number(st.maint_cycles) >= cyclesAtLeast) return st;
    if (Date.now() > end) throw new Error(`timeout waiting for ${want}: ${JSON.stringify(st)}`);
    await Bun.sleep(2);
  }
}

describe("maintenance: lazy delete + background reclaim", () => {
  test("DEL of a huge hash is O(1); a cycle reclaims its rows", async () => {
    const h = await workerServer();
    const c = h.client;
    await bigHash(c, "big", 200_000);
    const t = performance.now();
    expect(await c.del("big")).toBe(1);
    expect(performance.now() - t).toBeLessThan(50);
    expect(await c.exists("big")).toBe(false);
    expect(await c.send("HLEN", ["big"])).toBe(0);

    expect(await c.send("MAINT", ["RUN"])).toBe("OK");
    const st = await untilState(c, "normal");
    expect(st.maint_rows_reclaimed).toBe(200_000);
    expect(st.maint_keys_reclaimed).toBe(1);
    expect(st.maint_last_error).toBe("");
    // The key can be reused after reclaim.
    await c.send("HSET", ["big", "a", "1"]);
    expect(await c.send("HGETALL", ["big"])).toEqual({ a: "1" });
  });

  test("expired keys are reclaimed by the worker, not the main-thread reaper", async () => {
    const h = await workerServer();
    const c = h.client;
    for (let i = 0; i < 50; i++) await c.send("SET", [`t${i}`, "v", "PX", "1"]);
    await Bun.sleep(150); // several reaper ticks
    let st = await status(c);
    expect(st.maint_expired_backlog).toBe(50); // main thread did not sweep
    expect(await c.send("DBSIZE", [])).toBe(0);
    await c.send("MAINT", ["RUN"]);
    st = await untilState(c, "normal");
    expect(st.maint_expired_backlog).toBe(0);
    expect(st.maint_keys_reclaimed).toBe(50);
  });

  test("the expired-backlog threshold triggers a cycle on its own", async () => {
    const h = await workerServer({ "maint-expired-threshold": "20" });
    const c = h.client;
    for (let i = 0; i < 25; i++) await c.send("SET", [`t${i}`, "v", "PX", "1"]);
    const st = await untilState(c, "normal", 1, 5_000);
    expect(st.maint_last_reason).toBe("expired");
  });
});

describe("maintenance: IDLE buffering", () => {
  test("event loop stays responsive and every IDLE write lands in SQLite", async () => {
    const h = await workerServer();
    const c = h.client;
    const w = await h.makeClient();
    await c.set("hot", "v");
    await c.send("HSET", ["h", "f", "1"]);
    await c.send("RPUSH", ["l", "a", "b"]);
    await bigHash(c, "big", 300_000);
    await c.del("big");

    await c.send("MAINT", ["RUN"]);
    await untilState(c, "idle", 1, 5_000);
    let maxMs = 0;
    let n = 0;
    let st: Status;
    do {
      const t = performance.now();
      await w.get("hot");
      await w.set(`k${n}`, String(n));
      await w.send("HINCRBY", ["h", "f", "1"]);
      await w.send("LPUSH", ["l", `x${n}`]);
      maxMs = Math.max(maxMs, performance.now() - t);
      n++;
      st = await status(c);
    } while (st.maint_state !== "normal");

    expect(n).toBeGreaterThan(10);
    expect(maxMs).toBeLessThan(100); // vs ~1s+ for a synchronous delete of 300k rows
    expect(Number(st.maint_flushed_entries)).toBeGreaterThanOrEqual(n);
    expect(st.maint_buffer_entries).toBe(0);
    expect(await c.get("k0")).toBe("0");
    expect(await c.get(`k${n - 1}`)).toBe(String(n - 1));
    expect(await c.send("HGET", ["h", "f"])).toBe(String(1 + n));
    expect(await c.send("LLEN", ["l"])).toBe(2 + n);
    expect(await c.send("LINDEX", ["l", "-1"])).toBe("b");
    expect(await c.send("DBSIZE", [])).toBe(n + 3);
  });

  test("keys written during IDLE stay in the hot cache after the buffer is applied", async () => {
    const h = await workerServer();
    const c = h.client;
    await bigHash(c, "big", 300_000);
    await c.del("big");
    await c.send("MAINT", ["RUN"]);
    await untilState(c, "idle", 1, 5_000);
    await c.set("cached", "v");
    await untilState(c, "normal");
    const hits = async (): Promise<number> =>
      Number(/cache_hits:(\d+)/.exec(String(await c.send("INFO", [])))?.[1]);
    const before = await hits();
    expect(await c.get("cached")).toBe("v");
    expect(await hits()).toBe(before + 1); // served from cache, not re-read from SQLite
  });

  test("clean stop during IDLE drains the buffer (restart keeps the writes)", async () => {
    const path = mkTempDbPath();
    const h = await workerServer({}, path);
    const c = h.client;
    await bigHash(c, "big", 300_000);
    await c.del("big");
    await c.send("MAINT", ["RUN"]);
    await untilState(c, "idle", 1, 5_000);
    await c.set("during", "idle");
    await c.send("SADD", ["s", "a", "b"]);
    expect((await status(c)).maint_state).toBe("idle");
    closeHarness(h);

    const h2 = await startHarness({ dbPath: path });
    try {
      expect(await h2.client.get("during")).toBe("idle");
      expect(((await h2.client.smembers("s")) as string[]).sort()).toEqual(["a", "b"]);
      expect(await h2.client.exists("big")).toBe(false);
    } finally {
      closeHarness(h2);
    }
  });

  test("WATCH during IDLE: a buffered write aborts EXEC; applying the buffer does not", async () => {
    const h = await workerServer();
    const c = h.client;
    const other = await h.makeClient();
    await bigHash(c, "big", 300_000);
    await c.del("big");
    await c.set("w", "0");
    await c.send("MAINT", ["RUN"]);
    await untilState(c, "idle", 1, 5_000);

    await c.send("WATCH", ["w"]);
    await other.set("w", "changed"); // buffered write → bumps the watch
    await c.send("MULTI", []);
    await c.send("SET", ["w", "mine"]);
    expect(await c.send("EXEC", [])).toBeNull();

    await c.send("WATCH", ["w"]); // watch taken after the buffered write
    expect((await status(c)).maint_state).toBe("idle");
    await untilState(c, "normal"); // buffer applied to SQLite meanwhile
    await c.send("MULTI", []);
    await c.send("SET", ["w", "mine"]);
    expect(await c.send("EXEC", [])).toEqual(["OK"]);
    expect(await c.get("w")).toBe("mine");
  });
});

describe("maintenance: runtime configuration", () => {
  test("CONFIG GET/SET maint-* round-trips and validates all-or-nothing", async () => {
    const h = await workerServer({ "maint-chunk-rows": "500" });
    const c = h.client;
    expect(await c.send("CONFIG", ["GET", "maint-chunk-rows"])).toEqual({ "maint-chunk-rows": "500" });
    expect(await c.send("CONFIG", ["SET", "maint-chunk-rows", "4000", "maint-flush-budget-ms", "2"])).toBe("OK");
    expect(await c.send("CONFIG", ["GET", "maint-chunk-rows", "maint-flush-budget-ms"])).toEqual({
      "maint-chunk-rows": "4000",
      "maint-flush-budget-ms": "2",
    });
    // One bad value rejects the whole command.
    await expect(
      c.send("CONFIG", ["SET", "maint-chunk-rows", "100", "maint-flush-budget-ms", "0"]),
    ).rejects.toThrow(/maint-flush-budget-ms/);
    expect(await c.send("CONFIG", ["GET", "maint-chunk-rows"])).toEqual({ "maint-chunk-rows": "4000" });
    await expect(c.send("CONFIG", ["SET", "maint-eviction-policy", "allkeys-lru"])).rejects.toThrow();
    await expect(c.send("CONFIG", ["SET", "maint-enabled", "maybe"])).rejects.toThrow();
    // Unrelated Redis parameters stay lenient.
    expect(await c.send("CONFIG", ["SET", "notify-keyspace-events", "Ex"])).toBe("OK");
  });

  test("maint-enabled no stops automatic cycles; MAINT RUN still works", async () => {
    const h = await workerServer({ "maint-expired-threshold": "5", "maint-enabled": "no" });
    const c = h.client;
    for (let i = 0; i < 10; i++) await c.send("SET", [`t${i}`, "v", "PX", "1"]);
    await Bun.sleep(1_300);
    expect((await status(c)).maint_cycles).toBe(0);
    await c.send("MAINT", ["RUN"]);
    expect((await untilState(c, "normal")).maint_keys_reclaimed).toBe(10);
  });

  test("volatile-ttl eviction keeps the key count under maint-maxkeys", async () => {
    const h = await workerServer({ "maint-maxkeys": "100", "maint-eviction-policy": "volatile-ttl" });
    const c = h.client;
    for (let i = 0; i < 100; i++) await c.set(`p${i}`, "v"); // no TTL: never evicted
    for (let i = 0; i < 50; i++) await c.send("SET", [`v${i}`, "v", "EX", String(1000 + i)]);
    await c.send("MAINT", ["RUN"]);
    const st = await untilState(c, "normal");
    expect(st.maint_evicted_keys).toBe(50);
    expect(await c.send("DBSIZE", [])).toBe(100);
    expect(await c.get("p0")).toBe("v");
    expect(await c.get("v0")).toBeNull(); // soonest-expiring evicted first
  });

  test("INFO reports a Maintenance section", async () => {
    const h = await workerServer();
    const info = String(await h.client.send("INFO", []));
    expect(info).toContain("# Maintenance");
    expect(info).toContain("maint_state:normal");
    expect(info).toContain("disk_auto_vacuum:incremental");
  });

  test("maintenance off: MAINT and maint-* CONFIG SET are rejected", async () => {
    const path = mkTempDbPath();
    const h = await startHarness({ dbPath: path, maintenance: "off" });
    opened.push({ h, path });
    await expect(h.client.send("MAINT", ["STATUS"])).rejects.toThrow(/maintenance is off/);
    await expect(h.client.send("CONFIG", ["SET", "maint-chunk-rows", "100"])).rejects.toThrow(/maintenance is off/);
  });
});

describe("maintenance: single-writer lock file", () => {
  test("a second server on the same file is refused (worker or exclusive mode)", async () => {
    const path = mkTempDbPath();
    await workerServer({}, path);
    expect(existsSync(lockPath(path))).toBe(true);
    await expect(startHarness({ dbPath: path, maintenance: "worker" })).rejects.toThrow(/already in use/);
    await expect(startHarness({ dbPath: path })).rejects.toThrow(/already in use/);
  });

  test("a stale lock from a dead process is taken over; stop() releases it", async () => {
    const path = mkTempDbPath();
    writeFileSync(lockPath(path), "999999999");
    const h = await workerServer({}, path);
    await h.client.set("a", "1");
    closeHarness(h);
    expect(existsSync(lockPath(path))).toBe(false);
  });
});

describe("maintenance: lazy FLUSHALL", () => {
  test("FLUSHALL tombstones everything; the worker reclaims the rows", async () => {
    const h = await workerServer();
    const c = h.client;
    await bigHash(c, "big", 50_000);
    for (let i = 0; i < 100; i++) await c.set(`k${i}`, "v");
    expect(await c.send("FLUSHALL", [])).toBe("OK");
    expect(await c.send("DBSIZE", [])).toBe(0);
    expect(await c.exists("big")).toBe(false);
    await c.set("k1", "new"); // reuse a tombstoned key right away
    expect(await c.get("k1")).toBe("new");
    await c.send("MAINT", ["RUN"]);
    const st = await untilState(c, "normal");
    expect(st.maint_keys_reclaimed).toBe(100); // big + 99 strings (k1 was purged on rewrite)
    expect(st.maint_expired_backlog).toBe(0);
    expect(await c.send("DBSIZE", [])).toBe(1);
  });
});
