/**
 * Server/admin commands: TYPE, DBSIZE, FLUSHDB, FLUSHALL, CONFIG, COMMAND.
 *
 * These exist because real tooling sends them before doing anything useful:
 * integration tests flush between cases, redis-cli probes TYPE/COMMAND on
 * connect, and client libraries (BullMQ, connect-redis, health checks) read
 * CONFIG GET at init. CONFIG SET is a lenient stub — it never honors
 * security-sensitive parameters (dir, save, …); values are accepted and
 * dropped so init-time probes succeed.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import { commandCount } from "../dispatcher";
import type { CommandContext } from "../engine/context";

export function type(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.simple(ctx.storage.typeOf(ctx.arg(0), ctx.nowMs) ?? "none");
}

export function dbsize(ctx: CommandContext): Reply {
  ctx.requireExactArgc(0);
  return R.int(ctx.storage.dbsize(ctx.nowMs));
}

/** FLUSHDB/FLUSHALL [ASYNC|SYNC] — single DB, so both clear everything. */
export function flushall(ctx: CommandContext): Reply {
  if (ctx.argc > 1) return R.error("ERR", "syntax error");
  if (ctx.argc === 1) {
    const mode = ctx.upper(0);
    if (mode !== "ASYNC" && mode !== "SYNC") return R.error("ERR", "syntax error");
  }
  ctx.storage.flushAll();
  return R.ok();
}

/** Values reported by CONFIG GET (kept truthful to the actual configuration). */
function configValues(ctx: CommandContext): Record<string, string> {
  return {
    maxmemory: String(ctx.server.config.maxMemoryBytes),
    "maxmemory-policy": "noeviction",
    appendonly: "no",
    save: "",
    databases: "1",
    "proto-max-bulk-len": String(512 * 1024 * 1024),
  };
}

export function config(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  const sub = ctx.upper(0);
  switch (sub) {
    case "GET": {
      ctx.requireArgc(2);
      const values = configValues(ctx);
      // Dedupe across overlapping patterns: each parameter appears at most once.
      const matched = new Map<string, string>();
      for (let i = 1; i < ctx.argc; i++) {
        const pattern = ctx.str(i).toLowerCase();
        for (const [name, value] of Object.entries(values)) {
          if (globMatch(pattern, name)) matched.set(name, value);
        }
      }
      return R.map([...matched].map(([name, value]) => [R.bulk(name), R.bulk(value)]));
    }
    case "SET":
      // Lenient stub: accepted, never applied. Still validate arity like Redis
      // (at least one name/value pair → an even number of trailing tokens).
      if (ctx.argc < 3 || ctx.argc % 2 === 0) throw Errors.wrongArgs("config|set");
      return R.ok();
    case "RESETSTAT":
    case "REWRITE":
      return R.ok();
    default:
      return R.error("ERR", `Unknown CONFIG subcommand or wrong number of arguments for '${ctx.str(0)}'`);
  }
}

export function command(ctx: CommandContext): Reply {
  if (ctx.argc === 0) return R.array([]); // full introspection unsupported; empty is accepted by clients
  switch (ctx.upper(0)) {
    case "COUNT":
      return R.int(commandCount());
    case "DOCS":
      return R.map([]);
    case "INFO":
      // One reply element per requested command name (nil = no details), so
      // positional consumers that map request→reply slots stay aligned.
      return R.array(Array.from({ length: Math.max(0, ctx.argc - 1) }, () => R.nullReply()));
    case "GETKEYS":
    case "GETKEYSANDFLAGS":
      return R.error("ERR", "The command has no key arguments");
    default:
      return R.error(
        "ERR",
        `Unknown subcommand or wrong number of arguments for '${ctx.str(0)}'. Try COMMAND HELP.`,
      );
  }
}

/** Minimal glob: `*`, `?` (enough for CONFIG GET patterns). */
function globMatch(pattern: string, s: string): boolean {
  const re = new RegExp(
    "^" + pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*").replace(/\?/g, ".") + "$",
  );
  return re.test(s);
}
