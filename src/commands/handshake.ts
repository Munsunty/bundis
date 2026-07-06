/**
 * Handshake / connection commands (§2.2).
 *
 * These must be answered before (or alongside) data commands or the stock client
 * stalls during connect: HELLO, AUTH, SELECT, PING, INFO, QUIT, CLIENT, ECHO.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";

/** HELLO [protover [AUTH user pass] [SETNAME name]] → RESP3 server-info map. */
export function hello(ctx: CommandContext): Reply {
  let i = 0;
  if (ctx.argOpt(i) !== undefined && /^\d+$/.test(ctx.str(i))) {
    const proto = parseInt(ctx.str(i), 10);
    if (proto !== 2 && proto !== 3) {
      return R.error("NOPROTO", "unsupported protocol version");
    }
    ctx.conn.proto = proto;
    i++;
  }
  // Optional AUTH / SETNAME sub-options.
  while (ctx.argOpt(i) !== undefined) {
    const opt = ctx.upper(i);
    if (opt === "AUTH") {
      const pass = ctx.str(i + 2); // user is ctx.str(i+1), ignored (single user)
      if (!authenticate(ctx, pass)) return R.error(...wrongPass());
      ctx.conn.authed = true;
      i += 3;
    } else if (opt === "SETNAME") {
      ctx.conn.name = ctx.str(i + 1);
      i += 2;
    } else {
      throw Errors.syntax();
    }
  }
  if (requiresAuth(ctx) && !ctx.conn.authed) return R.error(...noAuth());
  ctx.conn.state = "READY";
  return helloMap(ctx);
}

/** AUTH [user] pass */
export function auth(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  const pass = ctx.argc >= 2 ? ctx.str(1) : ctx.str(0);
  if (ctx.server.config.password === null) {
    // No password configured: Redis errors, but we stay lenient and accept.
    ctx.conn.authed = true;
    return R.ok();
  }
  if (!authenticate(ctx, pass)) throw Errors.wrongPass();
  ctx.conn.authed = true;
  return R.ok();
}

export function ping(ctx: CommandContext): Reply {
  if (ctx.argc >= 1) return R.bulk(ctx.arg(0));
  return R.simple("PONG");
}

export function select(ctx: CommandContext): Reply {
  // Single logical DB (CLAUDE.md scope): index 0 is accepted, anything else is
  // an honest error — silently mapping /1 onto /0's data would clobber it.
  ctx.requireExactArgc(1);
  const idx = Number(ctx.int(0));
  if (idx !== 0) return R.error("ERR", "DB index is out of range");
  return R.ok();
}

export function echo(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.bulk(ctx.arg(0));
}

export function quit(ctx: CommandContext): null {
  ctx.conn.send(R.ok());
  ctx.conn.end(); // flushes any corked replies before closing
  return null; // reply already written; socket is closing
}

export function info(ctx: CommandContext): Reply {
  const lines = [
    "# Server",
    "redis_version:7.4.0",
    "redis_mode:standalone",
    "run_id:bundis",
    "tcp_port:" + ctx.server.config.port,
    "",
    "# Clients",
    "connected_clients:1",
    "",
    "# Memory",
    `used_memory:${process.memoryUsage().rss}`,
    `maxmemory:${ctx.server.config.maxMemoryBytes}`,
    "maxmemory_policy:noeviction", // data lives in SQLite; only caches evict
    "",
    "# Keyspace",
    `db0:keys=${ctx.storage.dbsize(ctx.nowMs)},expires=0,avg_ttl=0`,
    "",
  ];
  const stats = (ctx.storage as { stats?: () => Record<string, number> }).stats?.();
  if (stats) {
    lines.push(
      "# Cache",
      `cache_entries:${stats.entries}`,
      `cache_bytes:${stats.bytes}`,
      `cache_max_bytes:${stats.maxBytes}`,
      `cache_hits:${stats.hits}`,
      `cache_misses:${stats.misses}`,
      `cache_evicted_idle:${stats.evictedIdle}`,
      `cache_evicted_lru:${stats.evictedLru}`,
      "",
    );
  }
  return R.verbatim("txt", lines.join("\r\n"));
}

/** CLIENT SETINFO/SETNAME/GETNAME/ID/... — lenient, returns sensible replies. */
export function client(ctx: CommandContext): Reply {
  const sub = ctx.upper(0);
  switch (sub) {
    case "SETNAME":
      ctx.conn.name = ctx.argc >= 2 ? ctx.str(1) : "";
      return R.ok();
    case "GETNAME":
      return R.bulk(ctx.conn.name || "");
    case "ID":
      return R.int(ctx.conn.id);
    case "SETINFO":
      return R.ok();
    case "INFO":
      return R.bulk(`id=${ctx.conn.id} name=${ctx.conn.name}`);
    default:
      return R.ok();
  }
}

// ── helpers ──────────────────────────────────────────────────────────────---

function helloMap(ctx: CommandContext): Reply {
  return R.map([
    [R.bulk("server"), R.bulk("bundis")],
    [R.bulk("version"), R.bulk("0.1.0")],
    [R.bulk("proto"), R.int(ctx.conn.proto)],
    [R.bulk("id"), R.int(ctx.conn.id)],
    [R.bulk("mode"), R.bulk("standalone")],
    [R.bulk("role"), R.bulk("master")],
    [R.bulk("modules"), R.array([])],
  ]);
}

function authenticate(ctx: CommandContext, pass: string): boolean {
  const expected = ctx.server.config.password;
  return expected === null || expected === pass;
}

function requiresAuth(ctx: CommandContext): boolean {
  return ctx.server.config.password !== null;
}

// Small adapters so we can `R.error(...wrongPass())` cleanly.
function wrongPass(): [string, string] {
  const e = Errors.wrongPass();
  return [e.code, e.message];
}
function noAuth(): [string, string] {
  const e = Errors.noAuth();
  return [e.code, e.message];
}
