/**
 * L1 Transport — Bun.listen TCP server and socket lifecycle.
 *
 * Per connection (§4.1): accumulate bytes in the parser, drain complete commands
 * in arrival order, dispatch each serially (handlers are synchronous over
 * bun:sqlite), and write replies in order. Connection state lives on
 * `socket.data`. Backpressure is handled via the `drain` callback.
 */

import type { Socket, TCPSocketListener } from "bun";
import { Connection } from "./connection";
import { dispatch } from "./dispatcher";
import { ProtocolError } from "./resp/parser";
import { R } from "./resp/types";
import { SqliteStorage } from "./storage/sqlite";
import { HotCacheStorage } from "./storage/cache";
import type { StorageEngine } from "./storage/types";
import { PubSubHub } from "./sidecar/pubsub";
import { releaseSnapshot, WatchRegistry } from "./sidecar/watch";
import { MemoryGuard } from "./sidecar/memory-guard";
import { ExpiryReaper } from "./sidecar/reaper";
import type { ServerConfig } from "./config";
import type { ServerContext } from "./engine/context";

/** Commands that mutate storage (drive the group-commit batching decision). */
const WRITE_COMMANDS = new Set([
  "SET", "GETSET", "GETDEL", "APPEND", "DEL", "UNLINK",
  "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
  "MSET", "MSETNX", "SETEX", "PSETEX", "SETNX",
  "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
  "HSET", "HMSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
  "SADD", "SREM", "SPOP",
  "LPUSH", "RPUSH", "LPOP", "RPOP",
  "ZADD", "ZREM",
  "FLUSHDB", "FLUSHALL",
]);

export interface RunningServer {
  /** Actual bound port (resolves `port: 0` used by tests). */
  readonly port: number;
  readonly hostname: string;
  readonly server: ServerContext;
  /** Stop accepting connections, stop the reaper, and close storage. */
  stop(): void;
}

export function startServer(config: ServerConfig): RunningServer {
  const watch = new WatchRegistry();
  let cache: HotCacheStorage | null = null;
  const sqlite = new SqliteStorage(config.dbPath, {
    onWrite: (key) => {
      watch.bump(key);
      cache?.invalidate(key); // every mutation evicts its stale cache entry
    },
    onFlushAll: () => {
      watch.bumpAll();
      cache?.invalidateAll();
    },
    // 50% of the overall memory budget goes to the SQLite page cache.
    pageCacheKb: Math.floor(config.maxMemoryBytes / 2 / 1024),
  });
  const storage: StorageEngine =
    config.cacheMaxBytes > 0
      ? (cache = new HotCacheStorage(sqlite, {
          maxBytes: config.cacheMaxBytes,
          baseIdleMs: config.cacheIdleMs,
        }))
      : sqlite;
  const hub = new PubSubHub();
  const ctx: ServerContext = { storage, hub, watch, config };

  const reaper = new ExpiryReaper(storage, config.reaperIntervalMs);
  reaper.start();

  // Aggregate buffer ceiling across all connections (inbound parser + outbound
  // backpressure), independent of per-connection caps.
  const guard = new MemoryGuard(config.maxMemoryBytes);
  let liveClients = 0;
  const listener: TCPSocketListener<Connection> = Bun.listen<Connection>({
    hostname: config.host,
    port: config.port,
    socket: {
      open(socket: Socket<Connection>) {
        if (liveClients >= config.maxClients) {
          socket.write("-ERR max number of clients reached\r\n");
          socket.end();
          return; // socket.data stays unset; close() skips it
        }
        liveClients++;
        // Nagle interacts badly with small request/reply round-trips.
        (socket as unknown as { setNoDelay?: (on: boolean) => void }).setNoDelay?.(true);
        socket.data = new Connection(socket, guard);
      },
      data(socket: Socket<Connection>, chunk: Buffer) {
        const conn = socket.data;
        // Cork: coalesce every reply from this batch into one socket.write.
        conn.cork();
        let fatal = false;
        try {
          const before = conn.parser.bufferedBytes;
          conn.parser.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength));
          const commands = conn.parser.drain();
          // Account the net change in the inbound buffer against the global cap.
          guard.add(conn.parser.bufferedBytes - before);
          if (guard.overLimit) throw new ProtocolError("server memory limit reached");
          const now = Date.now();
          let writes = 0;
          for (const c of commands) if (WRITE_COMMANDS.has(c.name)) writes++;
          if (writes > 1) {
            // Group commit: one WAL commit for the whole pipelined batch.
            // dispatch() never throws (errors become RESP replies), so the
            // transaction always commits; replies flush after (uncork below).
            sqlite.withTransaction(() => {
              for (const command of commands) dispatch(conn, command, ctx, now);
            });
          } else {
            for (const command of commands) dispatch(conn, command, ctx, now);
          }
        } catch (err) {
          if (err instanceof ProtocolError) {
            conn.send(R.error("ERR", `Protocol error: ${err.message}`));
          } else {
            // Contain the blast radius to this connection — one bad socket
            // must never take down the whole server.
            console.error(`bundis: connection #${conn.id} error:`, err);
            conn.send(R.error("ERR", "internal error"));
          }
          guard.sub(conn.parser.bufferedBytes); // releasing this connection's inbound bytes
          fatal = true;
        } finally {
          conn.uncork(); // flush replies (incl. any error) in arrival order
        }
        if (fatal) socket.end();
      },
      drain(socket: Socket<Connection>) {
        socket.data?.flush();
      },
      close(socket: Socket<Connection>) {
        const conn = socket.data;
        if (conn) {
          liveClients--;
          guard.sub(conn.parser.bufferedBytes); // release inbound bytes (markClosed releases outbound)
          conn.markClosed();
          hub.drop(conn);
          if (conn.watch) {
            releaseSnapshot(watch, conn.watch); // refcounted registry interest
            conn.watch = null;
          }
        }
      },
      error(socket: Socket<Connection>) {
        socket.data?.markClosed();
      },
    },
  });

  return {
    port: listener.port,
    hostname: listener.hostname,
    server: ctx,
    stop() {
      reaper.stop();
      listener.stop(true);
      storage.close();
    },
  };
}
