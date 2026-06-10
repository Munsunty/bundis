/**
 * INCR regression bisection probe.
 *
 * Pre-cache run measured INCR at ~62k ops/s; post-cache at ~50.6k while SET
 * stayed flat. Each leg below toggles exactly one suspect so the cause is
 * isolated empirically. Serial execution, fresh server + file-WAL DB per leg,
 * same workload shape as B3 (single-key INCR, depth 100, 100k ops, 3 reps).
 *
 * Run: bun run bench/incr-probe.ts
 */

import { tmpdir } from "node:os";
import { unlinkSync } from "node:fs";
import { join } from "node:path";
import {
  asciiPayload,
  connect,
  keyArray,
  measureThroughput,
  median,
  preload,
  settle,
  startBenchServer,
} from "./lib";

const VAL64 = asciiPayload(64);
const TOTAL = 100_000;
const DEPTH = 100;
const REPS = 3;

let seq = 0;
function tmpDb(): string {
  return join(tmpdir(), `bundis-incr-probe-${process.pid}-${seq++}.db`);
}

interface Leg {
  name: string;
  cacheMb: number;
  maxMemoryMb?: number;
  preloadKeys: number;
  /** Cache the counter key itself before measuring (SET then GET to fill). */
  counterCached: boolean;
}

const LEGS: Leg[] = [
  { name: "current default (cache 64MB, budget 256MB, 100k preload)", cacheMb: 64, preloadKeys: 100_000, counterCached: false },
  { name: "cache OFF (budget 256MB, 100k preload)", cacheMb: 0, preloadKeys: 100_000, counterCached: false },
  { name: "cache OFF + tiny budget (page cache ~2MB, pre-cache-era pragma)", cacheMb: 0, maxMemoryMb: 4, preloadKeys: 100_000, counterCached: false },
  { name: "cache ON, NO preload (empty cache Map)", cacheMb: 64, preloadKeys: 0, counterCached: false },
  { name: "cache ON, counter key IS cached (invalidate hits every INCR)", cacheMb: 64, preloadKeys: 100_000, counterCached: true },
  { name: "cache ON + tiny budget (cache effect with old page cache)", cacheMb: 64, maxMemoryMb: 4, preloadKeys: 100_000, counterCached: false },
];

for (const leg of LEGS) {
  const dbPath = tmpDb();
  const server = await startBenchServer("spawn", {
    dbPath,
    cacheMb: leg.cacheMb,
    maxMemoryMb: leg.maxMemoryMb,
  });
  const c = await connect(server.url);
  try {
    if (leg.preloadKeys > 0) {
      await preload(c, keyArray("hot", leg.preloadKeys), VAL64);
    }
    if (leg.counterCached) {
      await c.set("ctr", "0");
      await c.get("ctr"); // fill the cache entry so every INCR invalidates a live entry
    }
    const reps: number[] = [];
    for (let r = 0; r < REPS; r++) {
      const res = await measureThroughput(TOTAL, DEPTH, () => c.incr("ctr"));
      reps.push(res.opsPerSec);
      await settle();
    }
    console.log(
      `${median(reps).toFixed(0).padStart(7)} ops/s  [${reps.map((x) => Math.round(x / 1000) + "k").join(", ")}]  ${leg.name}`,
    );
  } finally {
    c.close();
    await server.stop();
    for (const sfx of ["", "-wal", "-shm"]) {
      try {
        unlinkSync(dbPath + sfx);
      } catch {
        // absent is fine
      }
    }
  }
  await settle(200);
}
process.exit(0);
