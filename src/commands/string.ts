/**
 * String / KV + numeric commands (Phase 0).
 *
 * SET (with EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET), GET, GETSET, GETDEL, APPEND,
 * STRLEN, DEL, EXISTS, INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT.
 *
 * Type-conversion notes (§2.4): GET miss → RESP3 null; EXISTS → Integer (the
 * stock client coerces 1/0 to boolean); INCRBYFLOAT → bulk string.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";
import type { SetOptions } from "../storage/types";

export function set(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  const key = ctx.arg(0);
  const value = ctx.arg(1);
  const opts: { -readonly [K in keyof SetOptions]: SetOptions[K] } = {};
  let wantGet = false;

  let i = 2;
  while (ctx.argOpt(i) !== undefined) {
    const o = ctx.upper(i);
    switch (o) {
      case "EX":
        opts.expireAtMs = ctx.nowMs + Number(ctx.int(i + 1)) * 1000;
        i += 2;
        break;
      case "PX":
        opts.expireAtMs = ctx.nowMs + Number(ctx.int(i + 1));
        i += 2;
        break;
      case "EXAT":
        opts.expireAtMs = Number(ctx.int(i + 1)) * 1000;
        i += 2;
        break;
      case "PXAT":
        opts.expireAtMs = Number(ctx.int(i + 1));
        i += 2;
        break;
      case "NX":
        opts.mode = "NX";
        i += 1;
        break;
      case "XX":
        opts.mode = "XX";
        i += 1;
        break;
      case "KEEPTTL":
        opts.keepTtl = true;
        i += 1;
        break;
      case "GET":
        wantGet = true;
        i += 1;
        break;
      default:
        throw Errors.syntax();
    }
  }

  let old: Uint8Array | null = null;
  const result = ctx.storage.withTransaction(() => {
    if (wantGet) old = ctx.storage.kvGet(key, ctx.nowMs);
    return ctx.storage.kvSet(key, value, ctx.nowMs, opts);
  });

  if (wantGet) return R.bulk(old);
  return result === "set" ? R.ok() : R.nullReply();
}

export function get(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.bulk(ctx.storage.kvGet(ctx.arg(0), ctx.nowMs));
}

export function getset(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  const key = ctx.arg(0);
  return ctx.storage.withTransaction(() => {
    const old = ctx.storage.kvGet(key, ctx.nowMs);
    ctx.storage.kvSet(key, ctx.arg(1), ctx.nowMs);
    return R.bulk(old);
  });
}

export function getdel(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  const key = ctx.arg(0);
  return ctx.storage.withTransaction(() => {
    const old = ctx.storage.kvGet(key, ctx.nowMs);
    if (old !== null) ctx.storage.del([key], ctx.nowMs);
    return R.bulk(old);
  });
}

export function append(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.int(ctx.storage.append(ctx.arg(0), ctx.arg(1), ctx.nowMs));
}

export function strlen(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  const v = ctx.storage.kvGet(ctx.arg(0), ctx.nowMs);
  return R.int(v === null ? 0 : v.length);
}

export function del(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  return R.int(ctx.storage.del(ctx.args.slice(1), ctx.nowMs));
}

export function exists(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  let n = 0;
  for (let i = 0; i < ctx.argc; i++) {
    if (ctx.storage.exists(ctx.arg(i), ctx.nowMs)) n++;
  }
  return R.int(n);
}

export function incr(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.incrBy(ctx.arg(0), 1n, ctx.nowMs));
}

export function decr(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.incrBy(ctx.arg(0), -1n, ctx.nowMs));
}

export function incrby(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.int(ctx.storage.incrBy(ctx.arg(0), ctx.int(1), ctx.nowMs));
}

export function decrby(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.int(ctx.storage.incrBy(ctx.arg(0), -ctx.int(1), ctx.nowMs));
}

export function incrbyfloat(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  const next = ctx.storage.incrByFloat(ctx.arg(0), ctx.float(1), ctx.nowMs);
  return R.bulk(formatFloat(next));
}

function formatFloat(n: number): string {
  if (n === Infinity) return "inf";
  if (n === -Infinity) return "-inf";
  let s = n.toPrecision(17);
  if (s.includes(".") && !/[eE]/.test(s)) s = s.replace(/0+$/, "").replace(/\.$/, "");
  return Number(s).toString();
}
