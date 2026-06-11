/**
 * List commands (Phase 3).
 *
 * Type-conversion notes (§2.4): LPOP/RPOP without a count → Bulk/Null; with a
 * count → Array, except a missing key still replies Null (unlike SPOP's empty
 * array — Redis 7 semantics). LRANGE always replies an Array, empty included.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";

export function lpush(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return R.int(ctx.storage.lPush(ctx.arg(0), ctx.args.slice(2), ctx.nowMs));
}

export function rpush(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return R.int(ctx.storage.rPush(ctx.arg(0), ctx.args.slice(2), ctx.nowMs));
}

export function lpop(ctx: CommandContext): Reply {
  return pop(ctx, "L");
}

export function rpop(ctx: CommandContext): Reply {
  return pop(ctx, "R");
}

function pop(ctx: CommandContext, side: "L" | "R"): Reply {
  ctx.requireArgc(1);
  if (ctx.argc > 2) throw Errors.syntax();
  const hasCount = ctx.argOpt(1) !== undefined;
  const count = hasCount ? Number(ctx.int(1)) : null;
  if (count !== null && count < 0) {
    return R.error("ERR", "value is out of range, must be positive");
  }
  const res =
    side === "L"
      ? ctx.storage.lPop(ctx.arg(0), count, ctx.nowMs)
      : ctx.storage.rPop(ctx.arg(0), count, ctx.nowMs);
  if (count === null) {
    return res === null ? R.nullReply() : R.bulk(res as Uint8Array);
  }
  if (res === null) return R.nullReply();
  return R.array((res as Uint8Array[]).map((v) => R.bulk(v)));
}

export function lrange(ctx: CommandContext): Reply {
  ctx.requireExactArgc(3);
  const values = ctx.storage.lRange(
    ctx.arg(0),
    Number(ctx.int(1)),
    Number(ctx.int(2)),
    ctx.nowMs,
  );
  return R.array(values.map((v) => R.bulk(v)));
}

export function llen(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.lLen(ctx.arg(0), ctx.nowMs));
}

export function lindex(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  const value = ctx.storage.lIndex(ctx.arg(0), Number(ctx.int(1)), ctx.nowMs);
  return value === null ? R.nullReply() : R.bulk(value);
}
