/**
 * bundis benchmark suite — runs every case in bench/PLAN.md serially and writes
 * bench/results.json. Progress goes to stderr; the JSON file is the artifact.
 *
 * Usage:
 *   bun run bench/run.ts            # full run (~3-5 min)
 *   bun run bench/run.ts --quick    # 10x smaller, smoke-check the harness
 *   bun run bench/run.ts --only A,B # run only matching case-id prefixes
 *
 * Methodology invariants (see bench/lib.ts header):
 * - server in a separate process (spawnServer); embed only inside G1
 * - file-backed WAL DB is the default; :memory: only in E1's ceiling leg
 * - fresh server + fresh DB per case; clients close before server stop
 * - closed-loop numbers are service time @ concurrency 1; open-loop for F3
 */

import { tmpdir } from "node:os";
import { unlinkSync, statSync, copyFileSync } from "node:fs";
import { join } from "node:path";
import { RedisClient } from "bun";
import {
  asciiPayload,
  binaryPayload,
  connect,
  key,
  keyArray,
  lcg,
  measureOpenLoop,
  measureServiceTime,
  measureThroughput,
  median,
  preload,
  repeatThroughput,
  Results,
  settle,
  startBenchServer,
  startCpuSampler,
  type BenchServer,
} from "./lib";

const argv = Bun.argv.slice(2);
const QUICK = argv.includes("--quick");
const ONLY = (() => {
  const i = argv.indexOf("--only");
  if (i === -1) return null;
  const v = argv[i + 1];
  if (v === undefined) {
    console.error("--only requires a comma-separated list of case-id prefixes");
    process.exit(1);
  }
  return v.split(",").map((s) => s.trim().toUpperCase());
})();
const F = QUICK ? 0.1 : 1; // global size factor
const REPS = QUICK ? 1 : 3;
const n = (x: number) => Math.max(50, Math.round(x * F));
const wrapIdx = (i: number, m: number) => ((i % m) + m) % m;

const VAL64 = asciiPayload(64);
const results = new Results();

const wants = (id: string): boolean =>
  ONLY === null || ONLY.some((p) => id.toUpperCase().startsWith(p));

// ── server lifecycle ─────────────────────────────────────────────────────---

let dbSeq = 0;
function tmpDb(tag: string): string {
  return join(tmpdir(), `bundis-bench-${process.pid}-${tag}-${dbSeq++}.db`);
}

function rmDb(path: string): void {
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      unlinkSync(path + suffix);
    } catch {
      // absent is fine
    }
  }
}

/** Fresh spawned server on a fresh file-WAL DB (the headline config). */
async function withServer(
  fn: (client: RedisClient, server: BenchServer) => Promise<void>,
  opts: {
    dbPath?: string;
    keepDb?: boolean;
    reaperIntervalMs?: number;
    mode?: "spawn" | "embed";
    cacheMb?: number;
  } = {},
): Promise<void> {
  const dbPath = opts.dbPath ?? tmpDb("case");
  const server = await startBenchServer(opts.mode ?? "spawn", {
    dbPath,
    reaperIntervalMs: opts.reaperIntervalMs,
    cacheMb: opts.cacheMb,
  });
  let client: RedisClient | null = null;
  try {
    client = await connect(server.url);
    await fn(client, server);
  } finally {
    client?.close();
    await server.stop();
    if (dbPath !== ":memory:" && !opts.keepDb) rmDb(dbPath);
  }
  await settle();
}

// ── A. single-op service time (closed loop, concurrency 1) ───────────────---

async function caseA(): Promise<void> {
  const N = n(50_000);
  const WARM = n(10_000);
  const HOT = n(100_000);
  await withServer(async (c) => {
    const hot = keyArray("hot", HOT);
    const miss = keyArray("nope", N + WARM);
    await preload(c, hot, VAL64);

    if (wants("A1")) {
      const s = await measureServiceTime(N, WARM, (i) => c.set(hot[i % HOT]!, VAL64));
      results.add("A1", "SET 64B overwrite service time", { n: N }, { ...s });
    }
    if (wants("A2")) {
      const s = await measureServiceTime(N, WARM, (i) => c.get(hot[i % HOT]!));
      results.add("A2", "GET hit service time", { n: N }, { ...s });
    }
    if (wants("A3")) {
      const s = await measureServiceTime(N, WARM, (i) => c.get(miss[i]!));
      results.add("A3", "GET miss service time (= meta-lookup path)", { n: N }, { ...s });
    }
    if (wants("A4")) {
      const s = await measureServiceTime(N, WARM, () => c.incr("ctr"));
      results.add("A4", "INCR service time", { n: N }, { ...s });
    }
    if (wants("A6")) {
      const a6 = keyArray("a6", N + WARM);
      await preload(c, a6, VAL64);
      const s = await measureServiceTime(N, WARM, (i) => c.del(a6[i]!));
      results.add("A6", "DEL service time", { n: N }, { ...s });
    }
    if (wants("A7")) {
      const fields = Array.from({ length: N + WARM }, (_, i) => `f${i}`);
      const sw = await measureServiceTime(N, WARM, (i) => c.hset("h", fields[i]!, VAL64));
      const sr = await measureServiceTime(N, WARM, (i) => c.hget("h", fields[i % N]!));
      results.add("A7", "HSET / HGET service time", { n: N }, { hset: sw, hget: sr });
    }
    if (wants("A8")) {
      const members = Array.from({ length: N + WARM }, (_, i) => `m${i}`);
      const sw = await measureServiceTime(N, WARM, (i) => c.sadd("s", members[i]!));
      const sr = await measureServiceTime(N, WARM, (i) => c.sismember("s", members[i % N]!));
      results.add("A8", "SADD / SISMEMBER service time", { n: N }, { sadd: sw, sismember: sr });
    }
    if (wants("A9")) {
      const s = await measureServiceTime(N, WARM, (i) => c.expire(hot[i % HOT]!, 3600));
      results.add("A9", "EXPIRE service time", { n: N }, { ...s });
    }
  });
}

// ── B. pipelined throughput (sliding window) ─────────────────────────────---

async function caseB(): Promise<void> {
  const TOTAL = n(100_000);
  const DEPTH = 100;
  const HOT = n(100_000);
  await withServer(async (c, server) => {
    const hot = keyArray("hot", HOT);
    await preload(c, hot, VAL64);

    if (wants("B1")) {
      const r = await repeatThroughput(REPS, () =>
        measureThroughput(TOTAL, DEPTH, (i) => c.set(hot[wrapIdx(i, HOT)]!, VAL64)),
      );
      results.add("B1", "SET overwrite throughput", { total: TOTAL, depth: DEPTH }, { ...r });
    }
    if (wants("B1B")) {
      const r = await repeatThroughput(REPS, (rep) => {
        const fresh = keyArray(`b1b${rep}`, TOTAL);
        return measureThroughput(TOTAL, DEPTH, (i) =>
          c.set(i < 0 ? key(`b1bw${rep}`, -i) : fresh[i]!, VAL64),
        );
      });
      results.add("B1b", "SET unique-insert throughput", { total: TOTAL, depth: DEPTH }, { ...r });
    }
    if (wants("B2")) {
      const cpu = startCpuSampler(server.pid);
      const r = await repeatThroughput(REPS, () =>
        measureThroughput(TOTAL, DEPTH, (i) => c.get(hot[wrapIdx(i, HOT)]!)),
      );
      results.add("B2", "GET throughput", { total: TOTAL, depth: DEPTH }, { ...r, cpu: await cpu.stop() });
    }
    if (wants("B3")) {
      const r = await repeatThroughput(REPS, () =>
        measureThroughput(TOTAL, DEPTH, () => c.incr("ctr")),
      );
      results.add("B3", "INCR throughput", { total: TOTAL, depth: DEPTH }, { ...r });
    }
    if (wants("B4")) {
      const CALLS = n(10_000);
      const tuples: string[][] = Array.from({ length: CALLS }, (_, i) =>
        Array.from({ length: 10 }, (_, j) => hot[(i * 10 + j) % HOT]!),
      );
      const r = await repeatThroughput(REPS, () =>
        measureThroughput(CALLS, 50, (i) => c.mget(...tuples[wrapIdx(i, CALLS)]!)),
      );
      results.add("B4", "MGET(10) throughput", { calls: CALLS, depth: 50 }, { ...r, keysPerSec: r.opsPerSec * 10 });
    }
    if (wants("B5")) {
      const out: Record<string, number> = {};
      for (const depth of [1, 10, 100, 1000]) {
        const total = depth === 1 ? n(10_000) : TOTAL;
        const r = await measureThroughput(total, depth, (i) => c.get(hot[wrapIdx(i, HOT)]!));
        out[`depth${depth}`] = r.opsPerSec;
        await settle();
      }
      results.add("B5", "GET throughput vs pipeline depth", { depths: [1, 10, 100, 1000] }, out);
    }
  });
}

// ── C. payload size scaling ──────────────────────────────────────────────---

async function caseC(): Promise<void> {
  if (!wants("C1")) return;
  const sizes = [
    { size: 1024, total: n(30_000), depth: 100 },
    { size: 16_384, total: n(6000), depth: 32 },
    { size: 262_144, total: n(800), depth: 8 },
  ];
  const out: Record<string, unknown> = {};
  for (const { size, total, depth } of sizes) {
    await withServer(async (c, server) => {
      const KS = 1000;
      const keys = keyArray(`c${size}`, KS);
      const payload = asciiPayload(size);
      const set = await measureThroughput(total, depth, (i) => c.set(keys[wrapIdx(i, KS)]!, payload));
      const cpu = startCpuSampler(server.pid);
      const get = await measureThroughput(total, depth, (i) => c.get(keys[wrapIdx(i, KS)]!));
      const cpuStats = await cpu.stop();
      const extra: Record<string, number> = {};
      if (size === 262_144) {
        const d1 = await measureThroughput(n(300), 1, (i) => c.get(keys[wrapIdx(i, KS)]!), 50);
        extra.getDepth1OpsPerSec = d1.opsPerSec;
      }
      const mbps = (ops: number) => Math.round((ops * size) / 1e6);
      out[`${size}B`] = {
        setOpsPerSec: set.opsPerSec,
        setMBps: mbps(set.opsPerSec),
        getOpsPerSec: get.opsPerSec,
        getMBps: mbps(get.opsPerSec),
        cpu: cpuStats,
        ...extra,
      };
    });
  }
  results.add("C1", "SET/GET vs payload size", { sizes: sizes.map((s) => s.size) }, out);
}

// ── D. concurrency (all clients share the bench process — see report note) ──

async function clientPool(url: string, count: number): Promise<RedisClient[]> {
  return Promise.all(Array.from({ length: count }, () => connect(url)));
}

/** Sliding window per client; aggregate ops/sec over the joint wall clock. */
async function pooledRun(
  clients: RedisClient[],
  totalOps: number,
  depthPerClient: number,
  op: (c: RedisClient, i: number) => Promise<unknown>,
): Promise<number> {
  const per = Math.floor(totalOps / clients.length);
  const t0 = Bun.nanoseconds();
  await Promise.all(
    clients.map(async (c, ci) => {
      let next = 0;
      const worker = async () => {
        for (;;) {
          const i = next++;
          if (i >= per) return;
          await op(c, ci * per + i);
        }
      };
      await Promise.all(Array.from({ length: depthPerClient }, worker));
    }),
  );
  const elapsedMs = (Bun.nanoseconds() - t0) / 1e6;
  return Math.round((per * clients.length) / (elapsedMs / 1000));
}

async function caseD(): Promise<void> {
  const TOTAL = n(96_000);
  const HOT = n(100_000);
  await withServer(async (c, server) => {
    const hot = keyArray("hot", HOT);
    await preload(c, hot, VAL64);

    if (wants("D1")) {
      const out: Record<string, unknown> = {};
      for (const count of [1, 4, 16, 64]) {
        const depthPer = Math.max(2, Math.round(128 / count));
        const clients = await clientPool(server.url, count);
        // Full-size warmup pass: the cache-hit path needs ~100k ops before the
        // JIT settles (observed 90k→320k ops/s transition around that point).
        await pooledRun(clients, TOTAL, depthPer, (cc, i) => cc.get(hot[i % HOT]!));
        const cpu = startCpuSampler(server.pid);
        const ops = await pooledRun(clients, TOTAL, depthPer, (cc, i) => cc.get(hot[i % HOT]!));
        out[`clients${count}`] = { opsPerSec: ops, depthPerClient: depthPer, cpu: await cpu.stop() };
        for (const cc of clients) cc.close();
        await settle();
      }
      results.add("D1", "GET throughput vs client count", { total: TOTAL }, out);
    }
    if (wants("D2")) {
      const out: Record<string, number> = {};
      const d2 = keyArray("d2", HOT);
      await preload(c, d2, VAL64); // every timed SET is an overwrite in all legs
      for (const readPct of [80, 50, 20]) {
        const clients = await clientPool(server.url, 16);
        const rand = lcg(42);
        const isRead = Array.from({ length: TOTAL }, () => rand() % 100 < readPct);
        const mixedOp = (cc: RedisClient, i: number) =>
          isRead[i % TOTAL]! ? cc.get(hot[i % HOT]!) : cc.set(d2[i % HOT]!, VAL64);
        await pooledRun(clients, 16 * 2000, 8, mixedOp); // same-mix warmup
        out[`read${readPct}`] = await pooledRun(clients, TOTAL, 8, mixedOp);
        for (const cc of clients) cc.close();
        await settle();
      }
      results.add("D2", "mixed GET/SET, 16 clients", { total: TOTAL, mixes: ["80/20", "50/50", "20/80"] }, out);
    }
  });
}

// ── E. persistence / keyspace ────────────────────────────────────────────---

async function caseE(): Promise<void> {
  const TOTAL = n(50_000);
  const DEPTH = 100;
  const HOT = n(100_000);

  if (wants("E1")) {
    const run = async (dbPath: string) => {
      let set = 0;
      let get = 0;
      await withServer(
        async (c) => {
          const hot = keyArray("hot", HOT);
          await preload(c, hot, VAL64);
          set = (
            await repeatThroughput(REPS, () =>
              measureThroughput(TOTAL, DEPTH, (i) => c.set(hot[wrapIdx(i, HOT)]!, VAL64)),
            )
          ).opsPerSec;
          get = (
            await repeatThroughput(REPS, () =>
              measureThroughput(TOTAL, DEPTH, (i) => c.get(hot[wrapIdx(i, HOT)]!)),
            )
          ).opsPerSec;
        },
        { dbPath },
      );
      return { setOpsPerSec: set, getOpsPerSec: get };
    };
    const file = await run(tmpDb("e1"));
    const mem = await run(":memory:");
    results.add(
      "E1",
      "file WAL (headline config) vs :memory: ceiling",
      { total: TOTAL, depth: DEPTH },
      { fileWal: file, memory: mem },
    );
  }

  if (wants("E2")) {
    const out: Record<string, number> = {};
    for (const keyspace of [n(1000), n(100_000)]) {
      await withServer(async (c) => {
        const ks = keyArray("ks", keyspace);
        await preload(c, ks, VAL64);
        const rand = lcg(7);
        const r = await measureThroughput(TOTAL, DEPTH, () => c.get(ks[rand() % keyspace]!));
        out[`keys${keyspace}`] = r.opsPerSec;
      });
    }
    results.add("E2", "random GET vs keyspace size", { total: TOTAL, depth: DEPTH }, out);
  }

  if (wants("E3")) {
    const golden = tmpDb("e3-golden");
    const KEYS = n(100_000);
    await withServer(async (c) => preload(c, keyArray("cold", KEYS), VAL64), {
      dbPath: golden,
      keepDb: true,
    });
    const readyMs: number[] = [];
    const firstGetMs: number[] = [];
    try {
      for (let r = 0; r < 5; r++) {
        const copy = tmpDb(`e3-rep${r}`);
        copyFileSync(golden, copy); // fresh path per repeat: no page-cache reuse of the inode
        const t0 = Bun.nanoseconds();
        const server = await startBenchServer("spawn", { dbPath: copy });
        let c: RedisClient | null = null;
        try {
          const t1 = Bun.nanoseconds();
          c = await connect(server.url);
          await c.get(key("cold", 1));
          const t2 = Bun.nanoseconds();
          readyMs.push((t1 - t0) / 1e6);
          firstGetMs.push((t2 - t1) / 1e6);
        } finally {
          c?.close();
          await server.stop();
          rmDb(copy);
        }
      }
    } finally {
      rmDb(golden);
    }
    const r1 = (x: number) => Math.round(x * 10) / 10;
    results.add(
      "E3",
      "cold start with 100k-key file DB",
      { keys: KEYS, repeats: 5, note: "fresh file copy per repeat" },
      {
        spawnToReadyMedianMs: r1(median(readyMs)),
        connectFirstGetMedianMs: r1(median(firstGetMs)),
        readyMsSamples: readyMs.map(r1),
        firstGetMsSamples: firstGetMs.map(r1),
      },
    );
  }

  if (wants("E4")) {
    const file = tmpDb("e4");
    await withServer(
      async (c) => {
        const CHUNK = n(20_000);
        const CHUNKS = 10;
        const keys = keyArray("e4", CHUNK * CHUNKS);
        const chunks: number[] = [];
        for (let ch = 0; ch < CHUNKS; ch++) {
          const base = ch * CHUNK;
          const r = await measureThroughput(
            CHUNK,
            100,
            (i) => c.set(i < 0 ? key("e4w", -i) : keys[base + i]!, VAL64),
            ch === 0 ? 5000 : 0, // warm once; later chunks are the steady state
          );
          chunks.push(r.opsPerSec);
        }
        const sizeMB = (p: string) => {
          try {
            return Math.round(statSync(p).size / 1e5) / 10;
          } catch {
            return 0;
          }
        };
        results.add(
          "E4",
          "sustained unique-insert on file WAL (checkpoint visibility)",
          { totalKeys: CHUNK * CHUNKS, chunkSize: CHUNK },
          {
            chunkOpsPerSec: chunks,
            minChunk: Math.min(...chunks),
            maxChunk: Math.max(...chunks),
            dbMB: sizeMB(file),
            walMB: sizeMB(file + "-wal"),
          },
        );
      },
      { dbPath: file },
    );
  }
}

// ── F. feature paths ─────────────────────────────────────────────────────---

async function caseF(): Promise<void> {
  if (wants("F1")) {
    await withServer(async (c) => {
      const TXNS = n(200);
      const SIZE = 100;
      const ops = TXNS * SIZE;
      const keysFor = (tag: string) => keyArray(tag, SIZE);

      // untimed warmup so leg 1 doesn't absorb all cold-start cost
      await measureThroughput(5000, 100, (i) => c.set(key("f1warm", Math.abs(i) % SIZE), VAL64), 0);

      // leg 1: windowed pipelined SETs
      const pipeKeys = keysFor("f1p");
      const t0 = Bun.nanoseconds();
      await measureThroughput(ops, 100, (i) => c.set(pipeKeys[wrapIdx(i, SIZE)]!, VAL64), 0);
      const pipeMs = (Bun.nanoseconds() - t0) / 1e6;

      // leg 2: sequential awaited SETs (the no-pipelining floor)
      const seqKeys = keysFor("f1s");
      const t1 = Bun.nanoseconds();
      for (let i = 0; i < ops; i++) await c.set(seqKeys[i % SIZE]!, VAL64);
      const seqMs = (Bun.nanoseconds() - t1) / 1e6;

      // leg 3: MULTI/EXEC of 100 SETs per txn
      const txnKeys = keysFor("f1t");
      const t2 = Bun.nanoseconds();
      for (let t = 0; t < TXNS; t++) {
        await c.send("MULTI", []);
        const ps: Promise<unknown>[] = [];
        for (let j = 0; j < SIZE; j++) ps.push(c.set(txnKeys[j]!, VAL64));
        await Promise.all(ps);
        await c.send("EXEC", []);
      }
      const txnMs = (Bun.nanoseconds() - t2) / 1e6;

      // leg 4: MSET of 100 pairs per call
      const msetKeys = keysFor("f1m");
      const msetArgs: string[] = [];
      for (let j = 0; j < SIZE; j++) msetArgs.push(msetKeys[j]!, VAL64);
      const t3 = Bun.nanoseconds();
      for (let t = 0; t < TXNS; t++) await c.send("MSET", msetArgs);
      const msetMs = (Bun.nanoseconds() - t3) / 1e6;

      const rate = (ms: number) => Math.round(ops / (ms / 1000));
      results.add(
        "F1",
        "100-SET batch: pipelined vs sequential vs MULTI/EXEC vs MSET",
        { txns: TXNS, perTxn: SIZE },
        {
          pipelinedSetsPerSec: rate(pipeMs),
          sequentialSetsPerSec: rate(seqMs),
          multiExecSetsPerSec: rate(txnMs),
          msetSetsPerSec: rate(msetMs),
        },
      );
    });
  }

  if (wants("F2")) {
    const out: Record<string, number> = {};
    for (const subs of [1, 10, 50]) {
      await withServer(async (pub, server) => {
        const MSGS = n(10_000);
        const payload = asciiPayload(64);
        let resolveDone!: () => void;
        const done = new Promise<void>((res) => (resolveDone = res));
        let remaining = subs;
        const counters = new Array<number>(subs).fill(0);
        const subscribers: RedisClient[] = [];
        try {
          for (let s = 0; s < subs; s++) {
            const sc = await connect(server.url);
            subscribers.push(sc);
            await sc.subscribe("bench", () => {
              if (++counters[s]! === MSGS && --remaining === 0) resolveDone();
            });
          }
          const t0 = Bun.nanoseconds();
          await measureThroughput(MSGS, 100, () => pub.publish("bench", payload), 0);
          // a dropped delivery must fail the case, not hang the whole run
          let timer: ReturnType<typeof setTimeout> | undefined;
          const deadline = new Promise<never>((_, rej) => {
            timer = setTimeout(
              () => rej(new Error(`F2 stalled: delivered ${counters.join(",")} of ${MSGS}`)),
              60_000,
            );
          });
          await Promise.race([done, deadline]);
          clearTimeout(timer);
          const elapsedMs = (Bun.nanoseconds() - t0) / 1e6;
          out[`subs${subs}`] = Math.round((MSGS * subs) / (elapsedMs / 1000));
        } finally {
          for (const sc of subscribers) sc.close();
        }
      });
    }
    results.add(
      "F2",
      "PUBLISH fan-out delivered msgs/sec",
      { msgs: n(10_000), note: "subscribers share the bench process; see report caveat" },
      out,
    );
  }

  if (wants("F3")) {
    const RATE = 5000;
    const N = 25_000; // FIXED 5s wall-clock window even in --quick: TTL deadlines below assume it
    const HOT = n(20_000);
    const TTL_KEYS = n(50_000);
    const runLeg = async (
      churn: "none" | "staggered" | "burst",
    ): Promise<Record<string, unknown>> => {
      let legResult: Record<string, unknown> = {};
      await withServer(
        async (c) => {
          const hot = keyArray("hot", HOT);
          const ttlKeys = keyArray("ttl", TTL_KEYS);
          await preload(c, hot, VAL64);
          // warm the GET path BEFORE arming expiry so deadlines land in the window
          await measureThroughput(5000, 50, (i) => c.get(hot[Math.abs(i) % HOT]!), 0);
          if (churn !== "none") {
            await preload(c, ttlKeys, VAL64);
            // Anchor expiry to absolute instants inside the open-loop window via
            // PEXPIREAT — relative TTLs would smear across the arming duration.
            const rand = lcg(3);
            const base = Date.now() + 1500; // ~1.1s into the measured window
            const atOf = () => (churn === "staggered" ? base - 1000 + (rand() % 4000) : base);
            await measureThroughput(
              TTL_KEYS,
              200,
              (i) =>
                c.send("PEXPIREAT", [ttlKeys[Math.abs(i) % TTL_KEYS]!, String(atOf())]),
              0,
            );
          }
          const s = await measureOpenLoop(RATE, N, (i) => c.get(hot[i % HOT]!));
          legResult = { ...s };
        },
        { reaperIntervalMs: 100 },
      );
      return legResult;
    };
    const none = await runLeg("none");
    const staggered = await runLeg("staggered");
    const burst = await runLeg("burst");
    results.add(
      "F3",
      "open-loop GET latency @5k/s during TTL churn",
      { rate: RATE, samples: N, ttlKeys: TTL_KEYS },
      { baseline: none, staggeredExpiry: staggered, burstExpiry: burst },
    );
  }

  if (wants("F5")) {
    await withServer(
      async (c) => {
        const N = n(30_000);
        const ttlKeys = keyArray("lz", N);
        await measureThroughput(
          N,
          200,
          (i) => c.send("PSETEX", [ttlKeys[Math.abs(i) % N]!, "300", VAL64]),
          0,
        );
        await new Promise((r) => setTimeout(r, 800)); // all expired; reaper effectively off
        const s = await measureServiceTime(N - n(5000), n(5000), (i) => c.get(ttlKeys[i]!));
        results.add(
          "F5",
          "GET of expired key (lazy-expiry DELETE on read path)",
          { n: N, note: "compare against A3 plain miss" },
          { ...s },
        );
      },
      { reaperIntervalMs: 600_000 },
    );
  }

  if (wants("F6")) {
    await withServer(async (c) => {
      const TOTAL = n(20_000);
      await c.hset("ht", "f", "v");
      await c.set("str", "notanumber");
      const noop = () => {};
      // identical promise-chain shape on every leg so only the server path differs
      const happyGet = await measureThroughput(TOTAL, 100, () => c.get("str").then(noop, noop));
      const happyIncr = await measureThroughput(TOTAL, 100, () => c.incr("ctr").then(noop, noop));
      const wrongtype = await measureThroughput(TOTAL, 100, () => c.get("ht").then(noop, noop));
      const incrErr = await measureThroughput(TOTAL, 100, () => c.incr("str").then(noop, noop));
      results.add(
        "F6",
        "error-path throughput (client-side throw included)",
        { total: TOTAL, depth: 100 },
        {
          happyGetOpsPerSec: happyGet.opsPerSec,
          happyIncrOpsPerSec: happyIncr.opsPerSec,
          wrongtypeGetOpsPerSec: wrongtype.opsPerSec,
          incrNotIntOpsPerSec: incrErr.opsPerSec,
        },
      );
    });
  }

  if (wants("F7")) {
    await withServer(async (c) => {
      const APPENDS = n(2000);
      const CHUNK = asciiPayload(1024);
      // warm the APPEND path on a scratch key so the first window isn't cold-start
      for (let i = 0; i < 100; i++) await c.append("warm", CHUNK);
      await c.del("warm");
      const samples = new Float64Array(APPENDS);
      for (let i = 0; i < APPENDS; i++) {
        const t0 = Bun.nanoseconds();
        await c.append("big", CHUNK);
        samples[i] = Bun.nanoseconds() - t0;
      }
      const meanUs = (from: number, to: number) => {
        let sum = 0;
        for (let i = from; i < to; i++) sum += samples[i]!;
        return Math.round(sum / (to - from) / 100) / 10;
      };
      const W = Math.min(200, Math.floor(APPENDS / 4)); // head/tail windows must not overlap
      const finalLen = await c.strlen("big");
      results.add(
        "F7",
        "APPEND 1KB x N to one key (value rewrite cost growth)",
        { appends: APPENDS, finalBytes: finalLen, window: W },
        { firstWindowMeanUs: meanUs(0, W), lastWindowMeanUs: meanUs(APPENDS - W, APPENDS) },
      );
    });
  }
}

// ── G. launch modes ──────────────────────────────────────────────────────---

async function caseG(): Promise<void> {
  if (!wants("G1")) return;
  const TOTAL = n(50_000);
  const HOT = n(100_000);
  const out: Record<string, unknown> = {};
  for (const mode of ["spawn", "embed"] as const) {
    await withServer(
      async (c) => {
        const hot = keyArray("hot", HOT);
        await preload(c, hot, VAL64);
        const get = await repeatThroughput(REPS, () =>
          measureThroughput(TOTAL, 100, (i) => c.get(hot[wrapIdx(i, HOT)]!)),
        );
        const set = await repeatThroughput(REPS, () =>
          measureThroughput(TOTAL, 100, (i) => c.set(hot[wrapIdx(i, HOT)]!, VAL64)),
        );
        const svc = await measureServiceTime(n(10_000), n(3000), (i) => c.get(hot[i % HOT]!));
        out[mode] = { getOpsPerSec: get.opsPerSec, setOpsPerSec: set.opsPerSec, getServiceTime: svc };
      },
      { mode },
    );
  }
  results.add(
    "G1",
    "embedServer vs spawnServer",
    { total: TOTAL, note: "embed shares one event loop with the measuring client" },
    out,
  );
}

// ── H. collection cardinality ────────────────────────────────────────────---

async function caseH(): Promise<void> {
  if (!wants("H1")) return;
  await withServer(async (c) => {
    const sizes = [n(1000), n(10_000), n(100_000)];
    const out: Record<string, unknown> = {};
    for (const size of sizes) {
      const fields = Array.from({ length: size }, (_, i) => `f${i}`);
      await measureThroughput(size, 200, (i) => c.hset(`H${size}`, fields[Math.abs(i) % size]!, VAL64), 0);
      await measureThroughput(size, 200, (i) => c.sadd(`S${size}`, fields[Math.abs(i) % size]!), 0);
      const reps = size >= 100_000 ? 10 : size >= 10_000 ? 30 : 100;
      const hgetall = await measureServiceTime(reps, 3, () => c.hgetall(`H${size}`));
      const smembers = await measureServiceTime(reps, 3, () => c.smembers(`S${size}`));
      out[`card${size}`] = {
        hgetallMeanMs: Math.round(hgetall.meanUs / 100) / 10,
        smembersMeanMs: Math.round(smembers.meanUs / 100) / 10,
      };
    }
    // single-member ops on the biggest set — flags O(n) SRANDMEMBER/SPOP
    const big = sizes[sizes.length - 1]!;
    const srand = await measureServiceTime(n(300), 20, () => c.srandmember(`S${big}`));
    const spop = await measureServiceTime(n(300), 20, () => c.spop(`S${big}`));
    out.singleMemberOnBig = {
      setCard: big,
      srandmemberMeanUs: srand.meanUs,
      spopMeanUs: spop.meanUs,
    };
    // whole-collection DEL
    const delMs: Record<string, number> = {};
    for (const size of sizes.slice(1)) {
      for (const kind of ["H", "S"] as const) {
        const t0 = Bun.nanoseconds();
        await c.del(`${kind}${size}`);
        delMs[`${kind}${size}`] = Math.round(((Bun.nanoseconds() - t0) / 1e6) * 10) / 10;
      }
    }
    out.delWholeCollectionMs = delMs;
    results.add("H1", "collection cardinality costs", { sizes }, out);
  });
}

// ── I. hot cache on/off ──────────────────────────────────────────────────---

async function caseI(): Promise<void> {
  if (!wants("I1")) return;
  const TOTAL = n(100_000);
  const HOT = n(100_000);
  const out: Record<string, unknown> = {};
  for (const cacheMb of [64, 0]) {
    const tag = cacheMb > 0 ? "cacheOn" : "cacheOff";
    await withServer(
      async (c, server) => {
        const hot = keyArray("hot", HOT);
        await preload(c, hot, VAL64);
        const cpu = startCpuSampler(server.pid);
        const get = await repeatThroughput(REPS, () =>
          measureThroughput(TOTAL, 100, (i) => c.get(hot[wrapIdx(i, HOT)]!)),
        );
        const cpuStats = await cpu.stop();
        const set = await repeatThroughput(REPS, () =>
          measureThroughput(TOTAL, 100, (i) => c.set(hot[wrapIdx(i, HOT)]!, VAL64)),
        );
        const svc = await measureServiceTime(n(20_000), n(5000), (i) => c.get(hot[i % HOT]!));
        out[tag] = {
          getOpsPerSec: get.opsPerSec,
          getRepeats: get.repeats,
          setOpsPerSec: set.opsPerSec,
          getServiceTime: svc,
          cpu: cpuStats,
        };
      },
      { cacheMb },
    );
  }
  results.add(
    "I1",
    "hot cache on (64MB) vs off — GET/SET, 100k-key working set",
    { total: TOTAL, depth: 100, keyspace: HOT },
    out,
  );
}

// ── main ─────────────────────────────────────────────────────────────────---

const started = Date.now();
console.error(`bundis bench${QUICK ? " (quick)" : ""} starting…`);

// One failing case records an error and the suite keeps going — results.json
// must always be written with whatever was collected.
const caseFns: Array<[string, () => Promise<void>]> = [
  ["A", caseA],
  ["B", caseB],
  ["C", caseC],
  ["D", caseD],
  ["E", caseE],
  ["F", caseF],
  ["G", caseG],
  ["H", caseH],
  ["I", caseI],
];
for (const [group, fn] of caseFns) {
  try {
    await fn();
  } catch (e) {
    results.add(group, "CASE GROUP FAILED", {}, { error: String(e) });
  }
}

const meta = {
  date: new Date().toISOString(),
  quick: QUICK,
  durationSec: Math.round((Date.now() - started) / 1000),
  bun: Bun.version,
  platform: `${process.platform} ${(await Bun.$`uname -r`.text()).trim()}`,
  cpu: (await Bun.$`sysctl -n machdep.cpu.brand_string`.text()).trim(),
  cores: Number((await Bun.$`sysctl -n hw.ncpu`.text()).trim()),
  memGB: Math.round(Number((await Bun.$`sysctl -n hw.memsize`.text()).trim()) / 2 ** 30),
  commit: (await Bun.$`git rev-parse --short HEAD`.text()).trim(),
  config: {
    serverMode: "spawnServer (separate process); embed only in G1",
    db: "file-backed WAL in tmpdir (fresh per case); :memory: only in E1 ceiling leg",
    client: "stock Bun.RedisClient, autoReconnect off",
    reaperIntervalMs: "100 (default) unless a case overrides",
  },
};

await Bun.write(
  new URL("./results.json", import.meta.url),
  JSON.stringify({ meta, cases: results.cases }, null, 2),
);
console.error(`done in ${meta.durationSec}s → bench/results.json`);
process.exit(0); // a lingering client/socket handle otherwise keeps the loop alive
