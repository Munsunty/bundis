/**
 * Expiry commands (Phase 0): EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL,
 * PERSIST. TTL/PTTL return -2 (missing), -1 (no expiry), or remaining time.
 *
 * EXPIRE-family commands accept one optional NX|XX|GT|LT flag (Redis ≥ 7.0):
 * NX = only when no TTL exists, XX = only when one exists, GT/LT = only when
 * the new expiry is later/earlier than the current one (a key without TTL
 * counts as infinitely late, so GT never applies and LT always does).
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";

export function expire(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return expireGeneric(ctx, ctx.nowMs + Number(ctx.int(1)) * 1000);
}

export function pexpire(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return expireGeneric(ctx, ctx.nowMs + Number(ctx.int(1)));
}

export function expireat(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return expireGeneric(ctx, Number(ctx.int(1)) * 1000);
}

export function pexpireat(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return expireGeneric(ctx, Number(ctx.int(1)));
}

function expireGeneric(ctx: CommandContext, atMs: number): Reply {
  const flags: string[] = [];
  for (let i = 2; i < ctx.argc; i++) {
    const f = ctx.upper(i);
    if (f !== "NX" && f !== "XX" && f !== "GT" && f !== "LT") throw Errors.syntax();
    flags.push(f);
  }
  if (flags.includes("NX") && flags.length > 1) {
    return R.error("ERR", "NX and XX, GT or LT options at the same time are not compatible");
  }
  if (flags.includes("GT") && flags.includes("LT")) {
    return R.error("ERR", "GT and LT options at the same time are not compatible");
  }
  const flag = (flags[0] ?? null) as "NX" | "XX" | "GT" | "LT" | null;
  if (flag !== null) {
    const cur = ctx.storage.pttl(ctx.arg(0), ctx.nowMs);
    if (cur === -2) return R.int(0); // missing key: no flag can apply
    const hasTtl = cur >= 0;
    const curAtMs = hasTtl ? ctx.nowMs + cur : Infinity; // no TTL = infinitely late
    if (flag === "NX" && hasTtl) return R.int(0);
    if (flag === "XX" && !hasTtl) return R.int(0);
    if (flag === "GT" && atMs <= curAtMs) return R.int(0);
    if (flag === "LT" && atMs >= curAtMs) return R.int(0);
  }
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
