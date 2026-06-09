/**
 * L4 Dispatcher — command routing table + execution policy.
 *
 * Compatibility extends by adding cases here (§3.3). `dispatch` applies the
 * connection-level policy (auth gate, SUBSCRIBED-mode restriction, MULTI
 * queueing) then delegates to `executeCore`, which looks up the handler, builds
 * the context, and turns thrown errors into RESP error replies. EXEC reuses
 * `executeCore` directly to run queued commands.
 */

import { R, type Reply } from "./resp/types";
import type { Command } from "./resp/types";
import { CommandContext, type ServerContext } from "./engine/context";
import { Errors, toRespError } from "./engine/errors";
import type { Connection } from "./connection";

import * as handshake from "./commands/handshake";
import * as string from "./commands/string";
import * as expire from "./commands/expire";
import * as multikey from "./commands/multikey";
import * as hash from "./commands/hash";
import * as set from "./commands/set";
import * as pubsub from "./commands/pubsub";
import * as txn from "./commands/transaction";

/** A command handler. Returns the reply, or null if it wrote its own output. */
export type Handler = (ctx: CommandContext) => Reply | null;

const TABLE: Record<string, Handler> = {
  // handshake / connection
  HELLO: handshake.hello,
  AUTH: handshake.auth,
  PING: handshake.ping,
  SELECT: handshake.select,
  ECHO: handshake.echo,
  QUIT: handshake.quit,
  INFO: handshake.info,
  CLIENT: handshake.client,

  // string / kv
  SET: string.set,
  GET: string.get,
  GETSET: string.getset,
  GETDEL: string.getdel,
  APPEND: string.append,
  STRLEN: string.strlen,
  DEL: string.del,
  UNLINK: string.del,
  EXISTS: string.exists,
  INCR: string.incr,
  DECR: string.decr,
  INCRBY: string.incrby,
  DECRBY: string.decrby,
  INCRBYFLOAT: string.incrbyfloat,

  // multi-key
  MGET: multikey.mget,
  MSET: multikey.mset,
  MSETNX: multikey.msetnx,
  SETEX: multikey.setex,
  PSETEX: multikey.psetex,
  SETNX: multikey.setnx,

  // expiry
  EXPIRE: expire.expire,
  PEXPIRE: expire.pexpire,
  EXPIREAT: expire.expireat,
  PEXPIREAT: expire.pexpireat,
  TTL: expire.ttl,
  PTTL: expire.pttl,
  PERSIST: expire.persist,

  // hash
  HSET: hash.hset,
  HMSET: hash.hmset,
  HSETNX: hash.hsetnx,
  HGET: hash.hget,
  HMGET: hash.hmget,
  HGETALL: hash.hgetall,
  HDEL: hash.hdel,
  HEXISTS: hash.hexists,
  HKEYS: hash.hkeys,
  HVALS: hash.hvals,
  HLEN: hash.hlen,
  HINCRBY: hash.hincrby,
  HINCRBYFLOAT: hash.hincrbyfloat,

  // set
  SADD: set.sadd,
  SREM: set.srem,
  SISMEMBER: set.sismember,
  SMEMBERS: set.smembers,
  SCARD: set.scard,
  SRANDMEMBER: set.srandmember,
  SPOP: set.spop,

  // pub/sub
  SUBSCRIBE: pubsub.subscribe,
  UNSUBSCRIBE: pubsub.unsubscribe,
  PSUBSCRIBE: pubsub.psubscribe,
  PUNSUBSCRIBE: pubsub.punsubscribe,
  PUBLISH: pubsub.publish,
  PUBSUB: pubsub.pubsub,

  // transactions
  MULTI: txn.multi,
  EXEC: txn.exec,
  DISCARD: txn.discard,
  WATCH: txn.watch,
  UNWATCH: txn.unwatch,
  RESET: txn.reset,
};

/** Commands permitted while in SUBSCRIBED mode (§6.1). */
const SUBSCRIBE_ALLOWED = new Set([
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "PSUBSCRIBE",
  "PUNSUBSCRIBE",
  "PING",
  "QUIT",
  "RESET",
]);

/** Commands allowed before auth when a password is configured. */
const PREAUTH_ALLOWED = new Set(["HELLO", "AUTH", "QUIT", "RESET"]);

/** Commands that are not queued during MULTI (they drive the transaction). */
const TXN_CONTROL = new Set(["EXEC", "DISCARD", "MULTI", "WATCH", "RESET"]);

/**
 * Top-level entry for one inbound command. Applies connection policy and writes
 * the reply (handlers that manage their own output return null).
 */
export function dispatch(
  conn: Connection,
  command: Command,
  server: ServerContext,
  nowMs: number,
): void {
  if (command.name === "") return; // empty multibulk / blank inline line

  // Auth gate.
  if (
    server.config.password !== null &&
    !conn.authed &&
    !PREAUTH_ALLOWED.has(command.name)
  ) {
    conn.send(errReply(Errors.noAuth()));
    return;
  }

  // SUBSCRIBED-mode restriction.
  if (conn.inSubscribeMode() && !SUBSCRIBE_ALLOWED.has(command.name)) {
    conn.send(errReply(Errors.unsupportedInSubscribe(command.name)));
    return;
  }

  // MULTI queueing.
  if (conn.txn && !TXN_CONTROL.has(command.name)) {
    if (!TABLE[command.name]) {
      conn.txn.error = true;
      conn.send(errReply(Errors.unknownCmd(command.name, argStrings(command))));
      return;
    }
    conn.txn.queued.push(command);
    conn.send(R.simple("QUEUED"));
    return;
  }

  const reply = executeCore(conn, command, server, nowMs);
  if (reply !== null) conn.send(reply);
}

/**
 * Look up and run a single command, returning its reply (or null if the handler
 * wrote its own output). Never throws: thrown errors become RESP error replies.
 */
export function executeCore(
  conn: Connection,
  command: Command,
  server: ServerContext,
  nowMs: number,
): Reply | null {
  const handler = TABLE[command.name];
  if (!handler) {
    return errReply(Errors.unknownCmd(command.name, argStrings(command)));
  }
  const ctx = new CommandContext(conn, server, command.args, nowMs);
  try {
    return handler(ctx);
  } catch (err) {
    return errReply(toRespError(err));
  }
}

function errReply(e: { code: string; message: string }): Reply {
  return R.error(e.code, e.message);
}

function argStrings(command: Command): string[] {
  const dec = new TextDecoder();
  return command.args.slice(1).map((a) => dec.decode(a));
}
