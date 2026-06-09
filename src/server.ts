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
import { PubSubHub } from "./sidecar/pubsub";
import { WatchRegistry } from "./sidecar/watch";
import { ExpiryReaper } from "./sidecar/reaper";
import type { ServerConfig } from "./config";
import type { ServerContext } from "./engine/context";

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
  const storage = new SqliteStorage(config.dbPath, { onWrite: watch.bump });
  const hub = new PubSubHub();
  const ctx: ServerContext = { storage, hub, watch, config };

  const reaper = new ExpiryReaper(storage, config.reaperIntervalMs);
  reaper.start();

  const listener: TCPSocketListener<Connection> = Bun.listen<Connection>({
    hostname: config.host,
    port: config.port,
    socket: {
      open(socket: Socket<Connection>) {
        socket.data = new Connection(socket);
      },
      data(socket: Socket<Connection>, chunk: Buffer) {
        const conn = socket.data;
        conn.parser.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength));
        let commands;
        try {
          commands = conn.parser.drain();
        } catch (err) {
          if (err instanceof ProtocolError) {
            conn.send(R.error("ERR", `Protocol error: ${err.message}`));
            socket.end();
            return;
          }
          throw err;
        }
        const now = Date.now();
        for (const command of commands) {
          dispatch(conn, command, ctx, now);
        }
      },
      drain(socket: Socket<Connection>) {
        socket.data?.flush();
      },
      close(socket: Socket<Connection>) {
        const conn = socket.data;
        if (conn) {
          conn.markClosed();
          hub.drop(conn);
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
