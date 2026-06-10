/**
 * Set commands (Phase 1).
 *
 * Type-conversion notes (§2.4): SMEMBERS / SPOP(count) / SRANDMEMBER(count) →
 * RESP3 Set (`~`) so the client yields an array; SISMEMBER → Integer (client
 * coerces to boolean).
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";

export function sadd(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return R.int(ctx.storage.sAdd(ctx.arg(0), ctx.args.slice(2), ctx.nowMs));
}

export function srem(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return R.int(ctx.storage.sRem(ctx.arg(0), ctx.args.slice(2), ctx.nowMs));
}

export function sismember(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.int(ctx.storage.sIsMember(ctx.arg(0), ctx.arg(1), ctx.nowMs) ? 1 : 0);
}

export function smembers(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.set(ctx.storage.sMembers(ctx.arg(0), ctx.nowMs).map((m) => R.bulk(m)));
}

export function scard(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.sCard(ctx.arg(0), ctx.nowMs));
}

export function srandmember(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  if (ctx.argc > 2) throw Errors.syntax();
  const hasCount = ctx.argOpt(1) !== undefined;
  const count = hasCount ? Number(ctx.int(1)) : null;
  const res = ctx.storage.sRandMember(ctx.arg(0), count, ctx.nowMs);
  if (count === null) {
    return res === null ? R.nullReply() : R.bulk(res as Uint8Array);
  }
  // With a count argument, reply is an array (possibly empty), not a set.
  return R.array((res as Uint8Array[]).map((m) => R.bulk(m)));
}

export function spop(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  if (ctx.argc > 2) throw Errors.syntax();
  const hasCount = ctx.argOpt(1) !== undefined;
  const count = hasCount ? Number(ctx.int(1)) : null;
  if (count !== null && count < 0) {
    return R.error("ERR", "value is out of range, must be positive");
  }
  const res = ctx.storage.sPop(ctx.arg(0), count, ctx.nowMs);
  if (count === null) {
    return res === null ? R.nullReply() : R.bulk(res as Uint8Array);
  }
  return R.array((res as Uint8Array[]).map((m) => R.bulk(m)));
}
