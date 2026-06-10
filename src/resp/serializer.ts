/**
 * L2 outbound RESP3 serializer.
 *
 * Walks a {@link Reply} tree and produces wire bytes. Bulk/verbatim lengths are
 * computed in **bytes** (not characters) so binary-safe values and multi-byte
 * UTF-8 are framed correctly. This is the only place RESP3 type bytes are chosen.
 *
 * RESP3 only by design: the sole supported client is the stock `Bun.RedisClient`,
 * which always negotiates `HELLO 3` (§2.1). No RESP2 downgrade path exists.
 */

import type { Reply } from "./types";

const encoder = new TextEncoder();

/** Serialize one reply to a single Uint8Array. */
export function serialize(reply: Reply): Uint8Array {
  const parts: Uint8Array[] = [];
  write(reply, parts);
  return concat(parts);
}

/** Serialize many replies back-to-back (one pipelined write). */
export function serializeAll(replies: readonly Reply[]): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const r of replies) write(r, parts);
  return concat(parts);
}

function write(reply: Reply, out: Uint8Array[]): void {
  switch (reply.t) {
    case "simple":
      out.push(encoder.encode(`+${reply.v}\r\n`));
      return;
    case "error":
      out.push(encoder.encode(`-${reply.code} ${reply.msg}\r\n`));
      return;
    case "int":
      out.push(encoder.encode(`:${reply.v}\r\n`));
      return;
    case "bignum":
      out.push(encoder.encode(`(${reply.v}\r\n`));
      return;
    case "bool":
      out.push(encoder.encode(reply.v ? "#t\r\n" : "#f\r\n"));
      return;
    case "double":
      out.push(encoder.encode(`,${formatDouble(reply.v)}\r\n`));
      return;
    case "null":
      out.push(encoder.encode("_\r\n"));
      return;
    case "bulk": {
      if (reply.v === null) {
        out.push(encoder.encode("_\r\n")); // RESP3 null
        return;
      }
      const body = typeof reply.v === "string" ? encoder.encode(reply.v) : reply.v;
      out.push(encoder.encode(`$${body.length}\r\n`));
      out.push(body);
      out.push(CRLF);
      return;
    }
    case "verbatim": {
      const body = encoder.encode(`${reply.fmt}:${reply.v}`);
      out.push(encoder.encode(`=${body.length}\r\n`));
      out.push(body);
      out.push(CRLF);
      return;
    }
    case "array": {
      if (reply.v === null) {
        out.push(encoder.encode("*-1\r\n"));
        return;
      }
      out.push(encoder.encode(`*${reply.v.length}\r\n`));
      for (const el of reply.v) write(el, out);
      return;
    }
    case "set":
      out.push(encoder.encode(`~${reply.v.length}\r\n`));
      for (const el of reply.v) write(el, out);
      return;
    case "push":
      out.push(encoder.encode(`>${reply.v.length}\r\n`));
      for (const el of reply.v) write(el, out);
      return;
    case "map":
      out.push(encoder.encode(`%${reply.v.length}\r\n`));
      for (const [k, val] of reply.v) {
        write(k, out);
        write(val, out);
      }
      return;
  }
}

const CRLF = encoder.encode("\r\n");

function formatDouble(v: number): string {
  if (Number.isNaN(v)) return "nan";
  if (v === Infinity) return "inf";
  if (v === -Infinity) return "-inf";
  return String(v);
}

function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}
