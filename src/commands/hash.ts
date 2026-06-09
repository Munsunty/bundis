/**
 * Hash commands (Phase 1).
 *
 * Type-conversion notes (§2.4): HGETALL → RESP3 Map (`%`) so the client yields an
 * object; HEXISTS → Integer (client coerces to boolean); HINCRBYFLOAT → bulk.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";

export function hset(ctx: CommandContext): Reply {
  ctx.requireArgc(3);
  if (ctx.argc % 2 !== 1) throw Errors.wrongArgs(ctx.cmd); // key + field/value pairs
  const key = ctx.arg(0);
  const pairs: Array<[Uint8Array, Uint8Array]> = [];
  for (let i = 1; i < ctx.argc; i += 2) pairs.push([ctx.arg(i), ctx.arg(i + 1)]);
  return R.int(ctx.storage.hSet(key, pairs, ctx.nowMs));
}

/** HMSET is like HSET but returns OK. */
export function hmset(ctx: CommandContext): Reply {
  ctx.requireArgc(3);
  if (ctx.argc % 2 !== 1) throw Errors.wrongArgs(ctx.cmd);
  const key = ctx.arg(0);
  const pairs: Array<[Uint8Array, Uint8Array]> = [];
  for (let i = 1; i < ctx.argc; i += 2) pairs.push([ctx.arg(i), ctx.arg(i + 1)]);
  ctx.storage.hSet(key, pairs, ctx.nowMs);
  return R.ok();
}

export function hsetnx(ctx: CommandContext): Reply {
  ctx.requireExactArgc(3);
  const key = ctx.arg(0);
  const field = ctx.arg(1);
  return ctx.storage.withTransaction(() => {
    if (ctx.storage.hExists(key, field, ctx.nowMs)) return R.int(0);
    ctx.storage.hSet(key, [[field, ctx.arg(2)]], ctx.nowMs);
    return R.int(1);
  });
}

export function hget(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.bulk(ctx.storage.hGet(ctx.arg(0), ctx.arg(1), ctx.nowMs));
}

export function hmget(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  const key = ctx.arg(0);
  const out: Reply[] = [];
  for (let i = 1; i < ctx.argc; i++) {
    out.push(R.bulk(ctx.storage.hGet(key, ctx.arg(i), ctx.nowMs)));
  }
  return R.array(out);
}

export function hgetall(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  const entries = ctx.storage.hGetAll(ctx.arg(0), ctx.nowMs);
  return R.map(entries.map(([f, v]) => [R.bulk(f), R.bulk(v)] as const));
}

export function hdel(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return R.int(ctx.storage.hDel(ctx.arg(0), ctx.args.slice(2), ctx.nowMs));
}

export function hexists(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.int(ctx.storage.hExists(ctx.arg(0), ctx.arg(1), ctx.nowMs) ? 1 : 0);
}

export function hkeys(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.array(ctx.storage.hKeys(ctx.arg(0), ctx.nowMs).map((k) => R.bulk(k)));
}

export function hvals(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.array(ctx.storage.hVals(ctx.arg(0), ctx.nowMs).map((v) => R.bulk(v)));
}

export function hlen(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.hLen(ctx.arg(0), ctx.nowMs));
}

export function hincrby(ctx: CommandContext): Reply {
  ctx.requireExactArgc(3);
  return R.int(ctx.storage.hIncrBy(ctx.arg(0), ctx.arg(1), ctx.int(2), ctx.nowMs));
}

export function hincrbyfloat(ctx: CommandContext): Reply {
  ctx.requireExactArgc(3);
  const next = ctx.storage.hIncrByFloat(ctx.arg(0), ctx.arg(1), ctx.float(2), ctx.nowMs);
  return R.bulk(formatFloat(next));
}

function formatFloat(n: number): string {
  if (n === Infinity) return "inf";
  if (n === -Infinity) return "-inf";
  let s = n.toPrecision(17);
  if (s.includes(".") && !/[eE]/.test(s)) s = s.replace(/0+$/, "").replace(/\.$/, "");
  return Number(s).toString();
}
