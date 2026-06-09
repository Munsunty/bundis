/**
 * Multi-key string commands (Phase 1): MGET, MSET, MSETNX, SETEX, PSETEX, SETNX.
 * MSET/MSETNX run inside one transaction for atomicity.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import type { CommandContext } from "../engine/context";

export function mget(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  const out: Reply[] = [];
  for (let i = 0; i < ctx.argc; i++) {
    out.push(R.bulk(ctx.storage.kvGet(ctx.arg(i), ctx.nowMs)));
  }
  return R.array(out);
}

export function mset(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  if (ctx.argc % 2 !== 0) throw Errors.wrongArgs(ctx.cmd);
  ctx.storage.withTransaction(() => {
    for (let i = 0; i < ctx.argc; i += 2) {
      ctx.storage.kvSet(ctx.arg(i), ctx.arg(i + 1), ctx.nowMs);
    }
  });
  return R.ok();
}

export function msetnx(ctx: CommandContext): Reply {
  ctx.requireArgc(2);
  if (ctx.argc % 2 !== 0) throw Errors.wrongArgs(ctx.cmd);
  const ok = ctx.storage.withTransaction(() => {
    for (let i = 0; i < ctx.argc; i += 2) {
      if (ctx.storage.exists(ctx.arg(i), ctx.nowMs)) return false;
    }
    for (let i = 0; i < ctx.argc; i += 2) {
      ctx.storage.kvSet(ctx.arg(i), ctx.arg(i + 1), ctx.nowMs);
    }
    return true;
  });
  return R.int(ok ? 1 : 0);
}

export function setex(ctx: CommandContext): Reply {
  ctx.requireExactArgc(3);
  const atMs = ctx.nowMs + Number(ctx.int(1)) * 1000;
  ctx.storage.kvSet(ctx.arg(0), ctx.arg(2), ctx.nowMs, { expireAtMs: atMs });
  return R.ok();
}

export function psetex(ctx: CommandContext): Reply {
  ctx.requireExactArgc(3);
  const atMs = ctx.nowMs + Number(ctx.int(1));
  ctx.storage.kvSet(ctx.arg(0), ctx.arg(2), ctx.nowMs, { expireAtMs: atMs });
  return R.ok();
}

export function setnx(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  const res = ctx.storage.kvSet(ctx.arg(0), ctx.arg(1), ctx.nowMs, { mode: "NX" });
  return R.int(res === "set" ? 1 : 0);
}
