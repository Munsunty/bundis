import { describe, expect, test } from "bun:test";
import { loadConfig } from "../../src/config";

const MB = 1024 * 1024;

describe("memory configuration", () => {
  test("defaults: 256MB budget, hot cache = budget/4", () => {
    const c = loadConfig([]);
    expect(c.maxMemoryBytes).toBe(256 * MB);
    expect(c.cacheMaxBytes).toBe(64 * MB);
    expect(c.cacheIdleMs).toBe(300_000);
  });

  test("--max-memory-mb scales the derived cache cap", () => {
    const c = loadConfig(["--max-memory-mb", "512"]);
    expect(c.maxMemoryBytes).toBe(512 * MB);
    expect(c.cacheMaxBytes).toBe(128 * MB);
  });

  test("explicit --cache-mb wins over the derived default", () => {
    const c = loadConfig(["--max-memory-mb", "512", "--cache-mb", "16"]);
    expect(c.cacheMaxBytes).toBe(16 * MB);
  });

  test("--cache-mb 0 disables the hot cache", () => {
    const c = loadConfig(["--cache-mb", "0"]);
    expect(c.cacheMaxBytes).toBe(0);
  });
});
