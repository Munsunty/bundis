/**
 * L2 outbound RESP3 serializer.
 *
 * {@link RespWriter} appends replies into one growable byte buffer instead of
 * allocating a Uint8Array per token and concatenating at the end — the write
 * path of every command reply, so allocation churn here is felt everywhere.
 * Bulk/verbatim lengths are computed in **bytes** (not characters) so
 * binary-safe values and multi-byte UTF-8 are framed correctly. This is the
 * only place RESP3 type bytes are chosen.
 *
 * RESP3 only by design: the sole supported client is the stock `Bun.RedisClient`,
 * which always negotiates `HELLO 3` (§2.1). No RESP2 downgrade path exists.
 */

import type { Reply } from "./types";

const encoder = new TextEncoder();

const CR = 13;
const LF = 10;

/** Initial buffer capacity; typical pipelined batches fit without growing. */
const INITIAL_CAP = 4 * 1024;
/** Capacity kept across {@link RespWriter.take} calls; bigger buffers are released. */
const RETAIN_CAP = 256 * 1024;

/**
 * Append-only RESP3 byte writer with a reusable buffer. One instance lives on
 * each connection: replies serialize straight into it, and the whole batch
 * flushes as a single contiguous write.
 */
export class RespWriter {
  #buf = new Uint8Array(INITIAL_CAP);
  #len = 0;

  /** Bytes written and not yet taken. */
  get length(): number {
    return this.#len;
  }

  writeReply(reply: Reply): void {
    this.#write(reply);
  }

  /**
   * Return everything written as a view and reset the writer. The view aliases
   * the internal buffer, which later writes reuse — callers must copy anything
   * they retain past the next write (see Connection's backpressure queue).
   * Buffers grown past {@link RETAIN_CAP} are released rather than reused, so a
   * one-off huge reply doesn't pin its capacity for the connection's lifetime
   * (the returned view keeps the old buffer alive only while the caller holds it).
   */
  take(): Uint8Array {
    const view = this.#buf.subarray(0, this.#len);
    this.#len = 0;
    if (this.#buf.length > RETAIN_CAP) this.#buf = new Uint8Array(INITIAL_CAP);
    return view;
  }

  /** Discard everything written (connection closed mid-batch). */
  reset(): void {
    this.#len = 0;
    if (this.#buf.length > RETAIN_CAP) this.#buf = new Uint8Array(INITIAL_CAP);
  }

  #write(reply: Reply): void {
    switch (reply.t) {
      case "simple":
        this.#byte(43); // '+'
        this.#utf8(reply.v);
        this.#crlf();
        return;
      case "error":
        this.#byte(45); // '-'
        this.#utf8(reply.code);
        this.#byte(32); // ' '
        this.#utf8(reply.msg);
        this.#crlf();
        return;
      case "int":
        this.#byte(58); // ':'
        this.#ascii(String(reply.v));
        this.#crlf();
        return;
      case "bignum":
        this.#byte(40); // '('
        this.#ascii(String(reply.v));
        this.#crlf();
        return;
      case "bool":
        this.#ascii(reply.v ? "#t\r\n" : "#f\r\n");
        return;
      case "double":
        this.#byte(44); // ','
        this.#ascii(formatDouble(reply.v));
        this.#crlf();
        return;
      case "null":
        this.#ascii("_\r\n");
        return;
      case "bulk": {
        const v = reply.v;
        if (v === null) {
          this.#ascii("_\r\n"); // RESP3 null
          return;
        }
        if (typeof v === "string") {
          const byteLen = Buffer.byteLength(v);
          this.#header(36, byteLen); // '$'
          this.#ensure(byteLen);
          encoder.encodeInto(v, this.#buf.subarray(this.#len));
          this.#len += byteLen;
        } else {
          this.#header(36, v.length); // '$'
          this.#bytes(v);
        }
        this.#crlf();
        return;
      }
      case "verbatim": {
        const body = `${reply.fmt}:${reply.v}`;
        this.#header(61, Buffer.byteLength(body)); // '='
        this.#utf8(body);
        this.#crlf();
        return;
      }
      case "array": {
        if (reply.v === null) {
          this.#ascii("*-1\r\n");
          return;
        }
        this.#header(42, reply.v.length); // '*'
        for (const el of reply.v) this.#write(el);
        return;
      }
      case "set":
        this.#header(126, reply.v.length); // '~'
        for (const el of reply.v) this.#write(el);
        return;
      case "push":
        this.#header(62, reply.v.length); // '>'
        for (const el of reply.v) this.#write(el);
        return;
      case "map":
        this.#header(37, reply.v.length); // '%'
        for (const [k, val] of reply.v) {
          this.#write(k);
          this.#write(val);
        }
        return;
    }
  }

  /** Type tag + decimal length + CRLF, e.g. `$5\r\n`. */
  #header(tag: number, n: number): void {
    this.#byte(tag);
    this.#ascii(String(n));
    this.#crlf();
  }

  #ensure(extra: number): void {
    const need = this.#len + extra;
    if (need <= this.#buf.length) return;
    let cap = this.#buf.length * 2;
    while (cap < need) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.#buf.subarray(0, this.#len));
    this.#buf = next;
  }

  #byte(b: number): void {
    this.#ensure(1);
    this.#buf[this.#len++] = b;
  }

  #crlf(): void {
    this.#ensure(2);
    this.#buf[this.#len++] = CR;
    this.#buf[this.#len++] = LF;
  }

  /** ASCII-only text (type tags, numbers, fixed frames). */
  #ascii(s: string): void {
    this.#ensure(s.length);
    const buf = this.#buf;
    let len = this.#len;
    for (let i = 0; i < s.length; i++) buf[len++] = s.charCodeAt(i);
    this.#len = len;
  }

  /** UTF-8 text of a priori unknown byte length (simple/error payloads). */
  #utf8(s: string): void {
    this.#ensure(s.length * 3); // worst case for any UTF-16 string
    const { written } = encoder.encodeInto(s, this.#buf.subarray(this.#len));
    this.#len += written;
  }

  #bytes(b: Uint8Array): void {
    this.#ensure(b.length);
    this.#buf.set(b, this.#len);
    this.#len += b.length;
  }
}

/** Serialize one reply to a single Uint8Array. */
export function serialize(reply: Reply): Uint8Array {
  const w = new RespWriter();
  w.writeReply(reply);
  return w.take();
}

/** Serialize many replies back-to-back (one pipelined write). */
export function serializeAll(replies: readonly Reply[]): Uint8Array {
  const w = new RespWriter();
  for (const r of replies) w.writeReply(r);
  return w.take();
}

function formatDouble(v: number): string {
  if (Number.isNaN(v)) return "nan";
  if (v === Infinity) return "inf";
  if (v === -Infinity) return "-inf";
  return String(v);
}
