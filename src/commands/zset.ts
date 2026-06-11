/**
 * Sorted-set commands (Phase 3).
 *
 * Type-conversion notes (§2.4): scores travel as RESP3 Doubles (`,`) the way
 * Redis 7 replies under HELLO 3 — ZSCORE / ZADD..INCR are Double-or-Null, and
 * WITHSCORES ranges reply an Array of [member, double] pairs (Redis flattens
 * to member,score,... only on RESP2, which bundis does not speak). Rank/count
 * replies are Integers; ranges without scores are plain Arrays, empty included.
 */

import { R, type Reply } from "../resp/types";
import { Errors, RespError } from "../engine/errors";
import type { CommandContext } from "../engine/context";
import type { ScoreBound, ZAddOptions } from "../storage/types";

export function zadd(ctx: CommandContext): Reply {
  ctx.requireArgc(3);
  let i = 1;
  let mode: "NX" | "XX" | undefined;
  let gt = false;
  let lt = false;
  let ch = false;
  let incr = false;
  for (; i < ctx.argc; i++) {
    const tok = ctx.upper(i);
    if (tok === "NX") mode = "NX";
    else if (tok === "XX") mode = "XX";
    else if (tok === "GT") gt = true;
    else if (tok === "LT") lt = true;
    else if (tok === "CH") ch = true;
    else if (tok === "INCR") incr = true;
    else break;
  }
  if ((gt && lt) || (mode === "NX" && (gt || lt))) {
    return R.error("ERR", "GT, LT, and/or NX options at the same time are not compatible");
  }
  const remaining = ctx.argc - i;
  if (remaining === 0 || remaining % 2 !== 0) throw Errors.syntax();
  const entries: Array<readonly [number, Uint8Array]> = [];
  for (; i < ctx.argc; i += 2) {
    entries.push([parseScore(ctx.str(i)), ctx.arg(i + 1)]);
  }
  const opts: ZAddOptions = { mode, gt, lt, ch };
  if (incr) {
    if (entries.length !== 1) {
      return R.error("ERR", "INCR option supports a single increment-element pair");
    }
    const [score, member] = entries[0]!;
    const next = ctx.storage.zIncr(ctx.arg(0), score, member, ctx.nowMs, opts);
    return next === null ? R.nullReply() : R.double(next);
  }
  return R.int(ctx.storage.zAdd(ctx.arg(0), entries, ctx.nowMs, opts));
}

export function zrange(ctx: CommandContext): Reply {
  return rangeByRank(ctx, false);
}

export function zrevrange(ctx: CommandContext): Reply {
  return rangeByRank(ctx, true);
}

function rangeByRank(ctx: CommandContext, rev: boolean): Reply {
  ctx.requireArgc(3);
  let withScores = false;
  if (ctx.argc === 4) {
    if (ctx.upper(3) !== "WITHSCORES") throw Errors.syntax();
    withScores = true;
  } else if (ctx.argc > 4) {
    throw Errors.syntax();
  }
  const pairs = ctx.storage.zRangeByRank(
    ctx.arg(0),
    Number(ctx.int(1)),
    Number(ctx.int(2)),
    rev,
    ctx.nowMs,
  );
  return rangeReply(pairs, withScores);
}

export function zrangebyscore(ctx: CommandContext): Reply {
  ctx.requireArgc(3);
  const min = parseBound(ctx.str(1));
  const max = parseBound(ctx.str(2));
  let withScores = false;
  let limit: { offset: number; count: number } | null = null;
  for (let i = 3; i < ctx.argc; ) {
    const tok = ctx.upper(i);
    if (tok === "WITHSCORES") {
      withScores = true;
      i++;
    } else if (tok === "LIMIT" && i + 2 < ctx.argc) {
      limit = { offset: Number(ctx.int(i + 1)), count: Number(ctx.int(i + 2)) };
      i += 3;
    } else {
      throw Errors.syntax();
    }
  }
  const pairs = ctx.storage.zRangeByScore(ctx.arg(0), min, max, limit, ctx.nowMs);
  return rangeReply(pairs, withScores);
}

export function zscore(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  const score = ctx.storage.zScore(ctx.arg(0), ctx.arg(1), ctx.nowMs);
  return score === null ? R.nullReply() : R.double(score);
}

export function zrank(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  const rank = ctx.storage.zRank(ctx.arg(0), ctx.arg(1), ctx.nowMs);
  return rank === null ? R.nullReply() : R.int(rank);
}

export function zcard(ctx: CommandContext): Reply {
  ctx.requireExactArgc(1);
  return R.int(ctx.storage.zCard(ctx.arg(0), ctx.nowMs));
}

export function zrem(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  return R.int(ctx.storage.zRem(ctx.arg(0), ctx.args.slice(2), ctx.nowMs));
}

function rangeReply(pairs: Array<[Uint8Array, number]>, withScores: boolean): Reply {
  if (!withScores) return R.array(pairs.map(([m]) => R.bulk(m)));
  return R.array(pairs.map(([m, s]) => R.array([R.bulk(m), R.double(s)])));
}

/** Score parser: unlike ctx.float, accepts the Redis inf spellings. */
function parseScore(s: string): number {
  const t = s.trim().toLowerCase();
  if (t === "inf" || t === "+inf" || t === "infinity" || t === "+infinity") return Infinity;
  if (t === "-inf" || t === "-infinity") return -Infinity;
  const n = Number(s);
  if (s.trim().length === 0 || Number.isNaN(n)) throw Errors.notFloat();
  return n;
}

/** ZRANGEBYSCORE endpoint: optional `(` prefix marks an exclusive bound. */
function parseBound(s: string): ScoreBound {
  const exclusive = s.startsWith("(");
  try {
    return { value: parseScore(exclusive ? s.slice(1) : s), exclusive };
  } catch {
    throw new RespError("ERR", "min or max is not a float");
  }
}
