# bun-resp-sqlite

A RESP3-compatible server backed by SQLite. The stock `Bun.RedisClient` connects
to it **unmodified** — point the connection URL at this server and it just works.
No Redis install, no external dependencies: everything is Bun-native
(`Bun.listen` for TCP, `bun:sqlite` for storage, `bun:test` for tests).

See [`CLAUDE.md`](./CLAUDE.md) for the full design SSOT.

```
Bun.RedisClient ──RESP3 over TCP──▶ bun-resp-sqlite ──▶ SQLite (.db file)
```

## Run

```bash
bun run src/index.ts --port 6379 --db ./data.db
# flags (or env): --host/REDIS_HOST  --port/REDIS_PORT
#                 --db/REDIS_DB_PATH (":memory:" for in-memory)
#                 --password/REDIS_PASSWORD
```

Then from any app:

```ts
import { RedisClient } from "bun";
const client = new RedisClient("redis://127.0.0.1:6379");
await client.set("k", "v");
await client.get("k"); // "v"
```

## Supported commands

- **Handshake:** HELLO, AUTH, PING, SELECT, INFO, QUIT, CLIENT, ECHO, RESET
- **String / numeric:** SET (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET), GET, GETSET,
  GETDEL, APPEND, STRLEN, DEL/UNLINK, EXISTS, INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT
- **Multi-key:** MGET, MSET, MSETNX, SETEX, PSETEX, SETNX
- **Expiry:** EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST
- **Hash:** HSET, HMSET, HSETNX, HGET, HMGET, HGETALL, HDEL, HEXISTS, HKEYS,
  HVALS, HLEN, HINCRBY, HINCRBYFLOAT
- **Set:** SADD, SREM, SISMEMBER, SMEMBERS, SCARD, SRANDMEMBER, SPOP
- **Pub/Sub:** SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB
- **Transactions:** MULTI, EXEC, DISCARD, WATCH, UNWATCH

## Test

```bash
bun test          # unit (no network) + contract (real TCP, stock client)
bun run typecheck # tsc --noEmit, strict
```

Contract tests boot the server on an ephemeral port and drive it with a genuine
`Bun.RedisClient`, asserting on the JS values it returns — the only honest proof
of wire compatibility.

## Layout

```
src/
  server.ts          L1 transport (Bun.listen)
  resp/              L2 RESP3 parser + serializer
  connection.ts      L3 per-connection state machine
  dispatcher.ts      L4 command routing table
  commands/          L5 command semantics
  storage/           L6 StorageEngine + SqliteStorage (WAL)
  sidecar/           L7 ExpiryReaper, PubSubHub, WatchRegistry
tests/
  unit/              modules in isolation (no network)
  contract/          stock Bun.RedisClient over real TCP
  helpers/ fixtures/
```

Built and tested against Bun 1.3.14.
