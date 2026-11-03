<div align="center">

# bundis

**A Redis-compatible server backed by SQLite — built for [Bun](https://bun.sh), and only Bun.**

Point a stock `Bun.RedisClient` at it and it just works. No Redis to install, no
native add-ons, no external dependencies. Your data lives in a SQLite file and
survives restarts.

[![Bun only](https://img.shields.io/badge/runtime-Bun%20only-f9f1e1?logo=bun&logoColor=black)](https://bun.sh)
[![RESP3](https://img.shields.io/badge/protocol-RESP3-d33682)](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md)
[![SQLite WAL](https://img.shields.io/badge/storage-SQLite%20WAL-003b57?logo=sqlite&logoColor=white)](https://www.sqlite.org/wal.html)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

```
┌──────────────────┐   RESP3 over TCP    ┌──────────────────────────┐
│  Bun.RedisClient │ ──────────────────▶ │  bundis                  │
│  (stock, as-is)  │ ◀────────────────── │  Bun.listen + bun:sqlite │
└──────────────────┘    RESP3 replies    └────────────┬─────────────┘
                                                       ▼
                                              SQLite file (WAL)
```

</div>

---

## ⚡ Bun only — please read first

> **bundis runs on the Bun runtime exclusively. It does not work on Node.js,
> Deno, or any other runtime, and there are no plans to support them.**

This is a deliberate design choice, not a limitation that will be "fixed":

- The server is built on **Bun-native APIs** — `Bun.listen` (TCP), `bun:sqlite`
  (storage), `Bun.spawn` (sidecar mode), `bun:test` (tests). These have no
  drop-in equivalent outside Bun.
- The only supported **client** is the stock [`Bun.RedisClient`](https://bun.sh/docs/api/redis),
  which speaks RESP3. That wire contract — *correct bytes in, correct bytes
  out* — is what bundis guarantees. Other Redis clients (ioredis, node-redis,
  redis-py, …) are out of scope.
- It ships as **TypeScript source**, executed directly by Bun. No build step,
  no transpiled `dist/`.

If your project isn't on Bun, bundis is not for you — and that's fine.

---

## Why bundis?

You want Redis-style data structures and pub/sub from a Bun app, but you don't
want to run and operate a separate Redis server. bundis gives you the
`Bun.RedisClient` API over a persistent, single-file backend:

| | Redis | bundis |
|---|---|---|
| Install / ops | separate server to run | none — it's a Bun import or `bunx` |
| Persistence | RDB/AOF | a SQLite file, always durable (WAL) |
| Cold start | replays data (∝ dataset size) | **~13 ms**, independent of dataset |
| Memory | whole dataset in RAM | disk-resident + a bounded hot cache |
| Dependencies | — | **zero** (everything is Bun-native) |
| Client | any | **`Bun.RedisClient` only** |

It is **interface-compatible, not a performance clone** of Redis: the goal is
that your code using `Bun.RedisClient` runs unchanged, persisted to SQLite.

## Requirements

- **[Bun](https://bun.sh) ≥ 1.1** (the `bun:sqlite` + `Bun.RedisClient` APIs).
- That's the whole list.

## Install

```bash
bun add bundis                   # from npm
bun add github:Munsunty/bundis   # or straight from GitHub
```

## Quick start

Three ways to run the server. All hand you a `redis://` URL for a stock
`Bun.RedisClient` — pick by where you want the SQLite writer to live.

### 1. In your process — `embedServer()`

Zero IPC, instant startup. Shares your app's event loop (`bun:sqlite` is
synchronous), so it's ideal for tests, scripts, and small apps.

```ts
import { RedisClient } from "bun";
import { embedServer } from "bundis";

const server = embedServer({ port: 6379, dbPath: "./data.db" });
const client = new RedisClient(server.url); // the stock client, unmodified

await client.set("user:1", "alice");
await client.get("user:1"); // "alice"

client.close();
server.stop();
```

### 2. A separate process — `spawnServer()`

Runs the server as its own Bun process and resolves once it's accepting
connections. Isolates the SQLite writer (and any blocking work) from your app.

```ts
import { RedisClient } from "bun";
import { spawnServer } from "bundis";

const server = await spawnServer({ port: 0, dbPath: "./data.db" }); // 0 = ephemeral
const client = new RedisClient(server.url);

await client.incr("hits");

client.close();
await server.stop(); // kills the child and waits for exit
```

### 3. A standalone daemon — CLI

```bash
bunx bundis --port 6379 --db ./data.db
```

stdout prints one machine-readable ready line
(`{"event":"bundis:ready","host":"…","port":…}`); human logs go to stderr.
Then connect from anywhere:

```ts
import { RedisClient } from "bun";
const client = new RedisClient("redis://127.0.0.1:6379");
await client.set("k", "v");
await client.get("k"); // "v"
```

## Configuration

Every option has a sensible internal default — none are required.

| Option (`embed`/`spawn`) | CLI flag / env | Default | Meaning |
|---|---|---|---|
| `host` | `--host` / `REDIS_HOST` | `127.0.0.1` | Bind address. Use `0.0.0.0` to expose beyond loopback. |
| `port` | `--port` / `REDIS_PORT` | `6379` | TCP port. `0` = ephemeral. |
| `dbPath` | `--db` / `REDIS_DB_PATH` | `./data.db` | SQLite file. `":memory:"` for non-persistent. |
| `password` | `--password` / `REDIS_PASSWORD` | none | Require `AUTH`. |
| `maxMemoryMb` | `--max-memory-mb` / `REDIS_MAX_MEMORY_MB` | `256` | Overall budget: 50% SQLite page cache, 25% hot cache. |
| `cacheMb` | `--cache-mb` / `REDIS_CACHE_MB` | `maxMemory/4` | Hot-cache byte ceiling. `0` disables it. |
| `cacheIdleSec` | `--cache-idle` / `REDIS_CACHE_IDLE_SEC` | `300` | Hot-cache base time-to-idle. |
| `reaperIntervalMs` | `--reaper` / `REDIS_REAPER_MS` | `100` | Active-expiry sweep interval. |
| `maxClients` | `--max-clients` / `REDIS_MAX_CLIENTS` | `10000` | Connection cap. |
| `maintenance` | `--maintenance` / `REDIS_MAINTENANCE` | `off` | `worker` = background data cleanup ([below](#background-maintenance-worker-mode)). |
| `maint` | `--maint-<name> <value>` | — | Initial maintenance parameters (same names as `CONFIG SET`). |

`spawnServer` also accepts `bunPath` and `readyTimeoutMs`. The returned `url`
already embeds the password when one is set.

> 🔒 **Security defaults:** bundis binds to `127.0.0.1` by default. Before
> setting `--host 0.0.0.0`, set a `--password` — an exposed, password-less cache
> is an open door on your network. `spawnServer` passes the password to the
> child via the environment, never via `ps`-visible argv.

## Hot cache

Reads are accelerated by an in-memory, write-through hot cache:

- **Write-through:** every `SET` updates SQLite *and* the cache, so the next
  `GET` is a hit — no cold first read.
- **Adaptive idle eviction:** an entry's idle lifetime grows with how often it's
  read (capped at 8×), and popularity decays each sweep — frequently-read keys
  stay hot, one-off keys fall out.
- **Bounded:** an LRU byte ceiling (`cacheMb`) keeps memory in check, so a
  dataset larger than RAM still works (that's the point of the SQLite backend).
- **Pure accelerator:** SQLite is always the source of truth. Durability and
  restart semantics are unchanged; the cache is empty after a restart and
  refills on use.

Live stats (`cache_hits`, `cache_entries`, evictions, …) show up under
`# Cache` in `INFO`. Set `cacheMb` / `REDIS_CACHE_MB` to `0` to turn it off.

## Performance

Measured on Apple M5 (10 cores) · Bun 1.3.14 · file-backed WAL + hot cache on,
the default production config. Full methodology and the before/after numbers
are in [`PERFORMANCE.md`](./PERFORMANCE.md); reproduce with `bun run bench/run.ts`.

| Metric | Value |
|---|---|
| GET throughput (hot working set, pipelined) | **~322k ops/s** (2.67× with cache vs without) |
| SET throughput (write-through) | **~69k ops/s** |
| GET latency, single op (p50 / p99) | **~30 µs / ~39 µs** |
| 16 KB value GET bandwidth (cache hit) | **~3.3 GB/s** |
| MSET bulk write (100 keys/call) | **~291k keys/s** |
| Pub/Sub fan-out (50 subscribers) | **~462k msgs/s** |
| Cold start (100k-key DB → first reply) | **~13 ms** |

> Numbers are interface-compatibility benchmarks against a real `Bun.RedisClient`
> over loopback TCP, not a claim of Redis-equal throughput. bundis targets the
> "no separate Redis, data on disk, runs on Bun" niche.

## Background maintenance (worker mode)

Deleting data is synchronous in SQLite, and bundis serves every connection from
one thread — so a big delete stalls everyone (a 1M-field hash takes ~2 s to
`DEL`). Worker mode moves that work off the event loop:

```ts
embedServer({ dbPath: "./data.db", maintenance: "worker" });
// or: bunx bundis --db ./data.db --maintenance worker --maint-interval-sec 30
```

- **Lazy delete:** `DEL`/`UNLINK` of a hash/set/list/zset and `FLUSHALL` only
  mark keys expired (O(1) per key). They are gone for clients immediately.
- **Cycles:** a worker thread with its own SQLite connection reclaims expired
  and deleted rows in small chunks, returns free pages to the filesystem
  (`incremental_vacuum`) and checkpoints the WAL.
- **No write stalls:** during a cycle (IDLE), writes are buffered in memory and
  reads merge buffer + SQLite (WAL allows reading while the worker writes).
  Only keys written during IDLE are buffered; cold data stays in SQLite. When
  the worker finishes, the buffer is applied in small time-budgeted slices.
- **Runtime control:** every knob is a `CONFIG SET` parameter:

| Parameter | Default | Meaning |
|---|---|---|
| `maint-enabled` | `yes` | Automatic cycles on/off (`MAINT RUN` still works). |
| `maint-interval-sec` | `60` | Periodic cycle; `0` = off. |
| `maint-expired-threshold` | `10000` | Start a cycle when this many expired/deleted keys await reclaim; `0` = off. |
| `maint-max-run-ms` | `5000` | Time cap per worker run (leftovers resume next cycle). |
| `maint-chunk-rows` | `2000` | Rows deleted per worker transaction. |
| `maint-buffer-max-mb` | `64` | Write-buffer size that makes the worker stop early. |
| `maint-flush-budget-ms` | `5` | Max event-loop time per buffer-apply slice. |
| `maint-vacuum-step-pages` | `256` | Pages per `incremental_vacuum` step; `0` = no disk reclaim. |
| `maint-maxkeys` / `maint-maxdisk-mb` | `0` | Limits that trigger a cycle (and eviction); `0` = off. |
| `maint-eviction-policy` | `noeviction` | `volatile-ttl`: when over a limit, evict soonest-expiring TTL keys first. |
| `maint-evict-batch` | `1000` | Max keys evicted per cycle. |

```ts
await client.send("CONFIG", ["SET", "maint-flush-budget-ms", "2", "maint-chunk-rows", "5000"]);
await client.send("MAINT", ["RUN"]);     // start a cycle now
await client.send("MAINT", ["STATUS"]);  // same fields as INFO's "# Maintenance"
```

Trade-offs, stated plainly:

- **Durability during a cycle:** buffered writes live in memory until applied.
  A clean stop (`stop()`, SIGTERM/SIGINT) applies them first; a hard crash
  during a cycle loses them.
- **Single writer via lock file:** the worker needs a second connection, so
  SQLite's exclusive locking is off and `<db>.lock` (holding the pid) keeps
  other bundis processes out. A stale lock from a dead process is taken over.
- Disk reclaim needs `auto_vacuum = INCREMENTAL`, which worker mode sets on
  **new** database files; existing files need one offline `VACUUM` after
  `PRAGMA auto_vacuum = INCREMENTAL` (check `disk_auto_vacuum` in `INFO`).
- Rewriting a deleted big key before a cycle reclaims it (e.g. `DEL big` then
  `HSET big …`) still purges the old rows inline.
- `FLUSHALL` is O(number of keys) (~0.5 µs/key), no longer O(rows).
- `:memory:` databases can't use worker mode (the worker can't share them).

## Supported commands

The server doesn't distinguish "dedicated method" from `client.send(...)` — every
command arrives as the same RESP array. Coverage grows by adding dispatch cases.

- **Handshake / connection:** HELLO, AUTH, PING, SELECT, INFO, QUIT, CLIENT, ECHO, RESET
- **String / numeric:** SET (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET), GET, GETSET,
  GETDEL, APPEND, STRLEN, DEL/UNLINK, EXISTS, INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT
- **Multi-key:** MGET, MSET, MSETNX, SETEX, PSETEX, SETNX
- **Expiry:** EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT (with NX/XX/GT/LT), TTL, PTTL, PERSIST
- **Hash:** HSET, HMSET, HSETNX, HGET, HMGET, HGETALL, HDEL, HEXISTS, HKEYS,
  HVALS, HLEN, HINCRBY, HINCRBYFLOAT
- **Set:** SADD, SREM, SISMEMBER, SMEMBERS, SCARD, SRANDMEMBER, SPOP
- **List:** LPUSH, RPUSH, LPOP, RPOP (with count), LRANGE, LLEN, LINDEX
- **Sorted set:** ZADD, ZRANGE, ZREVRANGE, ZRANGEBYSCORE (with WITHSCORES/LIMIT),
  ZSCORE, ZRANK, ZCARD, ZREM
- **Pub/Sub:** SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB
- **Transactions:** MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Server / admin:** TYPE, DBSIZE, FLUSHDB, FLUSHALL, CONFIG GET/SET, COMMAND,
  MAINT RUN/STATUS (worker mode)

`getBuffer()` is binary-safe (values are stored as BLOBs), `EXISTS`/`SISMEMBER`
coerce to booleans, zset scores reply as RESP3 doubles, and pub/sub delivers
RESP3 push frames — the response-type contract a stock client expects.

## Not in scope

By design (see [`CLAUDE.md`](./CLAUDE.md), the design SSOT):

- **Any runtime other than Bun**, and **any client other than `Bun.RedisClient`.**
- Redis Cluster / Sentinel (the client doesn't support them either).
- Multi-process sharing of one `.db` file, HA, or automatic failover —
  single-writer is an enforced assumption.
- Lua scripting (`EVAL`/`SCRIPT`).

## Testing

```bash
bun test          # unit (no network) + contract (real TCP, stock client)
bun run typecheck # tsc --noEmit, strict
```

Contract tests boot the server on an ephemeral port and drive it with a genuine
`Bun.RedisClient`, asserting on the **JS values it returns** — the only honest
proof of wire compatibility. Set `REDIS_URL` to additionally run a differential
test that replays the same commands against a real Redis and compares results.

## Project layout

```
src/
  index.ts        public API (embedServer / spawnServer / startServer)
  cli.ts          standalone daemon entry (bunx bundis)
  launch.ts       embed / spawn launchers
  server.ts       L1 transport (Bun.listen)
  resp/           L2 RESP3 parser + serializer
  connection.ts   L3 per-connection state machine
  dispatcher.ts   L4 command routing table
  commands/       L5 command semantics
  storage/        L6 StorageEngine + SqliteStorage (WAL) + HotCacheStorage
                  + OverlayStorage (write buffer for worker maintenance)
  sidecar/        L7 ExpiryReaper, PubSubHub, WatchRegistry, MemoryGuard,
                  Maintenance (+ maint-worker thread, lock file)
tests/
  unit/           modules in isolation (no network)
  contract/       stock Bun.RedisClient over real TCP
bench/            benchmark harness + methodology (PLAN.md)
```

## License

[MIT](./LICENSE) © Munsunty

---

<div align="center">
<sub>Built with 🥟 Bun. Runs on Bun. Only Bun.</sub>
</div>
