/**
 * PubSubHub — single-process in-memory pub/sub (§6.2).
 *
 * Maps channels and glob patterns to the set of subscribed connections. PUBLISH
 * pushes RESP3 push frames (`>`) directly to each matching connection's socket.
 * Multi-process broadcast is explicitly a non-goal.
 */

import { R, type Reply } from "../resp/types";
import type { Connection } from "../connection";

export class PubSubHub {
  #channels = new Map<string, Set<Connection>>();
  #patterns = new Map<string, Set<Connection>>();

  subscribe(conn: Connection, channel: string): number {
    addTo(this.#channels, channel, conn);
    conn.channels.add(channel);
    return conn.subscriptionCount();
  }

  unsubscribe(conn: Connection, channel: string): number {
    removeFrom(this.#channels, channel, conn);
    conn.channels.delete(channel);
    return conn.subscriptionCount();
  }

  psubscribe(conn: Connection, pattern: string): number {
    addTo(this.#patterns, pattern, conn);
    conn.patterns.add(pattern);
    return conn.subscriptionCount();
  }

  punsubscribe(conn: Connection, pattern: string): number {
    removeFrom(this.#patterns, pattern, conn);
    conn.patterns.delete(pattern);
    return conn.subscriptionCount();
  }

  /** Drop a connection from all channels/patterns (on disconnect). */
  drop(conn: Connection): void {
    for (const ch of conn.channels) removeFrom(this.#channels, ch, conn);
    for (const p of conn.patterns) removeFrom(this.#patterns, p, conn);
    conn.channels.clear();
    conn.patterns.clear();
  }

  /** Deliver a message; returns the number of clients that received it. */
  publish(channel: string, message: Uint8Array): number {
    let n = 0;
    const direct = this.#channels.get(channel);
    if (direct) {
      const frame = R.push([R.bulk("message"), R.bulk(channel), R.bulk(message)]);
      for (const conn of direct) {
        conn.send(frame);
        n++;
      }
    }
    for (const [pattern, conns] of this.#patterns) {
      if (!matchPattern(pattern, channel)) continue;
      const frame = R.push([
        R.bulk("pmessage"),
        R.bulk(pattern),
        R.bulk(channel),
        R.bulk(message),
      ]);
      for (const conn of conns) {
        conn.send(frame);
        n++;
      }
    }
    return n;
  }

  channelNames(): string[] {
    return [...this.#channels.entries()].filter(([, s]) => s.size > 0).map(([c]) => c);
  }

  numSub(channel: string): number {
    return this.#channels.get(channel)?.size ?? 0;
  }
}

function addTo(map: Map<string, Set<Connection>>, key: string, conn: Connection): void {
  let s = map.get(key);
  if (!s) {
    s = new Set();
    map.set(key, s);
  }
  s.add(conn);
}

function removeFrom(
  map: Map<string, Set<Connection>>,
  key: string,
  conn: Connection,
): void {
  const s = map.get(key);
  if (!s) return;
  s.delete(conn);
  if (s.size === 0) map.delete(key);
}

/** Redis-style glob match: `*` `?` `[...]` with `\` escapes. */
export function matchPattern(pattern: string, str: string): boolean {
  return globMatch(pattern, 0, str, 0);
}

function globMatch(p: string, pi: number, s: string, si: number): boolean {
  while (pi < p.length) {
    const pc = p[pi]!;
    if (pc === "*") {
      while (pi + 1 < p.length && p[pi + 1] === "*") pi++;
      if (pi + 1 === p.length) return true;
      for (let k = si; k <= s.length; k++) {
        if (globMatch(p, pi + 1, s, k)) return true;
      }
      return false;
    }
    if (si >= s.length) return false;
    if (pc === "?") {
      pi++;
      si++;
      continue;
    }
    if (pc === "[") {
      const close = p.indexOf("]", pi + 1);
      if (close === -1) {
        if (p[pi] !== s[si]) return false; // literal '['
        pi++;
        si++;
        continue;
      }
      let negate = false;
      let start = pi + 1;
      if (p[start] === "^") {
        negate = true;
        start++;
      }
      let matched = false;
      for (let k = start; k < close; k++) {
        if (p[k + 1] === "-" && k + 2 < close) {
          if (s[si]! >= p[k]! && s[si]! <= p[k + 2]!) matched = true;
          k += 2;
        } else if (p[k] === s[si]) {
          matched = true;
        }
      }
      if (matched === negate) return false;
      pi = close + 1;
      si++;
      continue;
    }
    if (pc === "\\" && pi + 1 < p.length) pi++;
    if (p[pi] !== s[si]) return false;
    pi++;
    si++;
  }
  return si === s.length;
}

export type { Reply };
