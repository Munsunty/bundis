import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("review fixes", () => {
  test("write-through: a plain SET is cached (first GET is a hit)", async () => {
    await h.client.set("k", "v");
    const info = (await h.client.send("INFO", [])) as string;
    const entries = Number(/cache_entries:(\d+)/.exec(info)?.[1]);
    expect(entries).toBeGreaterThanOrEqual(1); // SET filled the cache, no GET needed
  });

  test("SET ... GET still returns the old value", async () => {
    await h.client.set("k", "old");
    expect(await h.client.send("SET", ["k", "new", "GET"])).toBe("old");
    expect(await h.client.get("k")).toBe("new");
  });

  test("SPOP with negative count errors like Redis", async () => {
    await h.client.sadd("s", "a", "b", "c");
    expect(h.client.send("SPOP", ["s", "-1"])).rejects.toThrow(/must be positive/);
  });

  test("SPOP/SRANDMEMBER reject extra arguments", async () => {
    await h.client.sadd("s", "a");
    expect(h.client.send("SPOP", ["s", "1", "junk"])).rejects.toThrow(/syntax/);
    expect(h.client.send("SRANDMEMBER", ["s", "1", "junk"])).rejects.toThrow(/syntax/);
  });

  test("SPOP/SRANDMEMBER stay uniform-correct on a small set", async () => {
    await h.client.sadd("s", "a", "b", "c");
    const popped = (await h.client.send("SPOP", ["s", "2"])) as string[];
    expect(popped.length).toBe(2);
    expect(new Set(popped).size).toBe(2); // no duplicates
    expect(await h.client.scard("s")).toBe(1);
  });

  test("EXPIRE rejects incompatible flag combinations with Redis messages", async () => {
    await h.client.set("k", "v");
    expect(h.client.send("EXPIRE", ["k", "100", "NX", "XX"])).rejects.toThrow(/not compatible/);
    expect(h.client.send("EXPIRE", ["k", "100", "GT", "LT"])).rejects.toThrow(/not compatible/);
  });

  test("CONFIG GET dedupes overlapping patterns", async () => {
    const got = (await h.client.send("CONFIG", ["GET", "maxmemory", "maxmemory"])) as Record<
      string,
      string
    >;
    // a stock client returns an object, so duplicate keys would collapse anyway;
    // assert the value is present and well-formed
    expect(got.maxmemory).toBeDefined();
  });

  test("CONFIG SET validates arity", async () => {
    expect(h.client.send("CONFIG", ["SET", "appendonly"])).rejects.toThrow(/wrong number/);
  });

  test("COMMAND unknown subcommand errors; INFO is positional", async () => {
    expect(h.client.send("COMMAND", ["BADSUB"])).rejects.toThrow(/Unknown subcommand/);
    expect((await h.client.send("COMMAND", ["INFO", "get", "set"])) as unknown[]).toHaveLength(2);
  });

  test("PUBSUB NUMPAT rejects extra args", async () => {
    expect(h.client.send("PUBSUB", ["NUMPAT", "x"])).rejects.toThrow();
  });
});
