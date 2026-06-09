/**
 * Pub/Sub commands (Phase 2): SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE,
 * PUBLISH, PUBSUB.
 *
 * (Un)subscribe confirmations are sent as RESP3 push frames (`>`), one per
 * channel, matching what the stock client's subscribe()/unsubscribe() expect.
 * These handlers write their own frames and return null (the dispatcher then
 * writes nothing further).
 */

import { R, type Reply } from "../resp/types";
import type { CommandContext } from "../engine/context";

export function subscribe(ctx: CommandContext): null {
  ctx.requireArgc(1);
  for (let i = 0; i < ctx.argc; i++) {
    const channel = ctx.str(i);
    const count = ctx.server.hub.subscribe(ctx.conn, channel);
    ctx.conn.state = "SUBSCRIBED";
    ctx.conn.send(R.push([R.bulk("subscribe"), R.bulk(channel), R.int(count)]));
  }
  return null;
}

export function psubscribe(ctx: CommandContext): null {
  ctx.requireArgc(1);
  for (let i = 0; i < ctx.argc; i++) {
    const pattern = ctx.str(i);
    const count = ctx.server.hub.psubscribe(ctx.conn, pattern);
    ctx.conn.state = "SUBSCRIBED";
    ctx.conn.send(R.push([R.bulk("psubscribe"), R.bulk(pattern), R.int(count)]));
  }
  return null;
}

export function unsubscribe(ctx: CommandContext): null {
  const channels =
    ctx.argc >= 1
      ? rangeArgs(ctx, 0)
      : [...ctx.conn.channels];
  if (channels.length === 0) {
    // No channels: still send a single confirmation with null channel.
    ctx.conn.send(
      R.push([R.bulk("unsubscribe"), R.nullReply(), R.int(ctx.conn.subscriptionCount())]),
    );
    maybeExitSubscribe(ctx);
    return null;
  }
  for (const channel of channels) {
    const count = ctx.server.hub.unsubscribe(ctx.conn, channel);
    ctx.conn.send(R.push([R.bulk("unsubscribe"), R.bulk(channel), R.int(count)]));
  }
  maybeExitSubscribe(ctx);
  return null;
}

export function punsubscribe(ctx: CommandContext): null {
  const patterns = ctx.argc >= 1 ? rangeArgs(ctx, 0) : [...ctx.conn.patterns];
  if (patterns.length === 0) {
    ctx.conn.send(
      R.push([R.bulk("punsubscribe"), R.nullReply(), R.int(ctx.conn.subscriptionCount())]),
    );
    maybeExitSubscribe(ctx);
    return null;
  }
  for (const pattern of patterns) {
    const count = ctx.server.hub.punsubscribe(ctx.conn, pattern);
    ctx.conn.send(R.push([R.bulk("punsubscribe"), R.bulk(pattern), R.int(count)]));
  }
  maybeExitSubscribe(ctx);
  return null;
}

export function publish(ctx: CommandContext): Reply {
  ctx.requireExactArgc(2);
  return R.int(ctx.server.hub.publish(ctx.str(0), ctx.arg(1)));
}

export function pubsub(ctx: CommandContext): Reply {
  ctx.requireArgc(1);
  const sub = ctx.upper(0);
  switch (sub) {
    case "CHANNELS":
      return R.array(ctx.server.hub.channelNames().map((c) => R.bulk(c)));
    case "NUMSUB": {
      const out: Reply[] = [];
      for (let i = 1; i < ctx.argc; i++) {
        const ch = ctx.str(i);
        out.push(R.bulk(ch), R.int(ctx.server.hub.numSub(ch)));
      }
      return R.array(out);
    }
    default:
      return R.array([]);
  }
}

function rangeArgs(ctx: CommandContext, from: number): string[] {
  const out: string[] = [];
  for (let i = from; i < ctx.argc; i++) out.push(ctx.str(i));
  return out;
}

function maybeExitSubscribe(ctx: CommandContext): void {
  if (!ctx.conn.inSubscribeMode()) ctx.conn.state = "READY";
}
