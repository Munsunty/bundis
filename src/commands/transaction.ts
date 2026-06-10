/**
 * Transaction commands (Phase 2): MULTI, EXEC, DISCARD, WATCH, UNWATCH, RESET.
 *
 * MULTI starts queueing on the connection (handled in the dispatcher, which
 * replies +QUEUED). EXEC runs the queued commands inside one SQLite transaction
 * and returns the result array; if any WATCHed key changed since WATCH, EXEC
 * returns nil (optimistic lock, §6.3). Under the single-writer assumption a
 * simple version comparison via {@link WatchRegistry} is sufficient.
 */

import { R, type Reply } from "../resp/types";
import { Errors } from "../engine/errors";
import { executeCore } from "../dispatcher";
import type { CommandContext } from "../engine/context";
import { hashKey, releaseSnapshot, type WatchSnapshot } from "../sidecar/watch";

export function multi(ctx: CommandContext): Reply {
  if (ctx.conn.txn) return R.error("ERR", "MULTI calls can not be nested");
  ctx.conn.txn = { queued: [], error: false };
  return R.ok();
}

export function discard(ctx: CommandContext): Reply {
  if (!ctx.conn.txn) throw Errors.discardWithoutMulti();
  ctx.conn.txn = null;
  clearWatch(ctx);
  return R.ok();
}

export function exec(ctx: CommandContext): Reply {
  const conn = ctx.conn;
  const txn = conn.txn;
  if (!txn) throw Errors.execWithoutMulti();
  conn.txn = null;

  if (txn.error) {
    clearWatch(ctx);
    throw Errors.execAbort();
  }
  if (conn.watch && isWatchDirty(conn.watch, ctx)) {
    clearWatch(ctx);
    return R.nullReply(); // a watched key changed → abort
  }
  clearWatch(ctx);

  const results = ctx.storage.withTransaction(() =>
    txn.queued.map((cmd) => {
      const reply = executeCore(conn, cmd, ctx.server, ctx.nowMs);
      return reply ?? R.nullReply();
    }),
  );
  return R.array(results);
}

export function watch(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  if (ctx.conn.txn) return R.error("ERR", "WATCH inside MULTI is not allowed");
  const snap: WatchSnapshot = ctx.conn.watch ?? new Map();
  for (let i = 0; i < ctx.argc; i++) {
    const key = ctx.arg(i);
    const k = hashKey(key);
    if (snap.has(k)) continue; // already watched: keep the earlier snapshot
    snap.set(k, { key, version: ctx.server.watch.acquire(key) });
  }
  ctx.conn.watch = snap;
  return R.ok();
}

export function unwatch(ctx: CommandContext): Reply {
  clearWatch(ctx);
  return R.ok();
}

export function reset(ctx: CommandContext): Reply {
  ctx.conn.txn = null;
  clearWatch(ctx);
  ctx.server.hub.drop(ctx.conn);
  ctx.conn.state = "READY";
  return R.simple("RESET");
}

/** Drop the connection's WATCH snapshot, releasing registry interest. */
function clearWatch(ctx: CommandContext): void {
  if (ctx.conn.watch) {
    releaseSnapshot(ctx.server.watch, ctx.conn.watch);
    ctx.conn.watch = null;
  }
}

function isWatchDirty(snap: WatchSnapshot, ctx: CommandContext): boolean {
  for (const { key, version } of snap.values()) {
    if (ctx.server.watch.peek(key) !== version) return true;
  }
  return false;
}
