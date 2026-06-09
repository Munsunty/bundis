/**
 * Expiry commands (Phase 0): EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL,
 * PERSIST. TTL/PTTL return -2 (missing), -1 (no expiry), or remaining time.
 */

import { R, type Reply } from "../resp/types";
import type { CommandContext } from "../engine/context";

export function expire(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  const atMs = ctx.nowMs + Number(ctx.int(1)) * 1000;
  return R.int(ctx.storage.expireSet(ctx.arg(0), atMs, ctx.nowMs) ? 1 : 0);
}

export function pexpire(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  const atMs = ctx.nowMs + Number(ctx.int(1));
  return R.int(ctx.storage.expireSet(ctx.arg(0), atMs, ctx.nowMs) ? 1 : 0);
}

export function expireat(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  const atMs = Number(ctx.int(1)) * 1000;
  return R.int(ctx.storage.expireSet(ctx.arg(0), atMs, ctx.nowMs) ? 1 : 0);
}

export function pexpireat(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  const atMs = Number(ctx.int(1));
  return R.int(ctx.storage.expireSet(ctx.arg(0), atMs, ctx.nowMs) ? 1 : 0);
}

export function ttl(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  const ms = ctx.storage.pttl(ctx.arg(0), ctx.nowMs);
  if (ms < 0) return R.int(ms);
  return R.int(Math.ceil(ms / 1000));
}

export function pttl(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.pttl(ctx.arg(0), ctx.nowMs));
}

export function persist(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.persist(ctx.arg(0), ctx.nowMs) ? 1 : 0);
}
