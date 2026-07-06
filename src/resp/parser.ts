/**
 * L2 inbound RESP parser (streaming).
 *
 * TCP has no message boundaries: {@link ../server.ts}'s `data` callback delivers
 * arbitrary chunks. This parser keeps a per-connection accumulation buffer and
 * yields complete commands only, preserving arrival order so pipelined commands
 * stay in order (§4.1.1, §4.1.2).
 *
 * The stock `Bun.RedisClient` always sends commands as a RESP Array of Bulk
 * Strings (`*N\r\n($len\r\n<bytes>\r\n)*`). We parse that shape, plus inline
 * commands as a tolerant fallback. Argument bytes are preserved verbatim.
 *
 * Input caps (defense against pre-auth memory exhaustion — ProtocolError closes
 * the connection): declared bulk length ≤ 512MB (Redis proto-max-bulk-len),
 * multibulk count ≤ 1M, inline line ≤ 64KB, total pending buffer ≤ 768MB.
 */

import type { Command } from "./types";

const CR = 13;
const LF = 10;
const STAR = 42; // '*'
const DOLLAR = 36; // '$'

const MAX_BULK_LEN = 512 * 1024 * 1024;
const MAX_MULTIBULK = 1024 * 1024;
const MAX_INLINE_LEN = 64 * 1024;
const MAX_PENDING = 768 * 1024 * 1024;

const EMPTY = new Uint8Array(0);

/** Floor capacity for the owned accumulation buffer. */
const MIN_CAP = 8 * 1024;

export class RespParser {
  /** Pending bytes: either an aliased external chunk or an owned buffer. */
  #buf: Uint8Array = EMPTY;
  /** Valid bytes in #buf (owned buffers over-allocate capacity). */
  #len = 0;
  /** True while #buf aliases an externally-owned chunk (no copy taken yet). */
  #aliased = false;

  /** Bytes currently held pending a complete command. */
  get bufferedBytes(): number {
    return this.#len;
  }

  /** Append a freshly received chunk to the internal buffer. */
  push(chunk: Uint8Array): void {
    if (this.#len + chunk.length > MAX_PENDING) {
      throw new ProtocolError("query buffer exceeds limit");
    }
    if (this.#len === 0) {
      this.#buf = chunk;
      this.#len = chunk.length;
      this.#aliased = true; // zero-copy fast path; drain() copies any leftover
      return;
    }
    if (this.#aliased || this.#len + chunk.length > this.#buf.length) {
      // Grow geometrically so a value arriving in many chunks costs O(n)
      // total copying, not O(n²) (one full-buffer copy per chunk).
      const cap = Math.max(this.#buf.length * 2, this.#len + chunk.length, MIN_CAP);
      const next = new Uint8Array(cap);
      next.set(this.#buf.subarray(0, this.#len));
      this.#buf = next;
      this.#aliased = false;
    }
    this.#buf.set(chunk, this.#len);
    this.#len += chunk.length;
  }

  /**
   * Drain all complete commands currently buffered, in order.
   * Incomplete trailing bytes are retained for the next {@link push}.
   * Throws on malformed protocol (the caller should reply with an error and
   * close the connection).
   */
  drain(): Command[] {
    const out: Command[] = [];
    let offset = 0;
    for (;;) {
      const res = this.#parseOne(offset);
      if (res === null) break; // need more bytes
      out.push(res.command);
      offset = res.next;
    }
    if (offset >= this.#len) {
      // Fully consumed: release the buffer so the next push re-enters the
      // zero-copy alias fast path (and a grown buffer doesn't stay pinned).
      this.#buf = EMPTY;
      this.#len = 0;
      this.#aliased = false;
    } else if (this.#aliased) {
      // Copy the remainder: the caller's chunk buffer may be reused by the
      // runtime after this callback returns, so we must never retain a view
      // of it across data() events.
      this.#buf = this.#buf.slice(offset, this.#len);
      this.#len = this.#buf.length;
      this.#aliased = false;
    } else if (offset > 0) {
      // Owned buffer: slide the remainder to the front in place.
      this.#buf.copyWithin(0, offset, this.#len);
      this.#len -= offset;
    }
    return out;
  }

  /** Parse a single command starting at `start`, or null if incomplete. */
  #parseOne(start: number): { command: Command; next: number } | null {
    const buf = this.#buf;
    if (start >= this.#len) return null;
    const first = buf[start]!;
    if (first === STAR) return this.#parseArray(start);
    // Inline command fallback: a CRLF- or LF-terminated whitespace-split line.
    return this.#parseInline(start);
  }

  #parseArray(start: number): { command: Command; next: number } | null {
    const buf = this.#buf;
    const header = this.#readLine(start);
    if (header === null) return null;
    const count = parseInt(decodeAscii(buf.subarray(start + 1, header.end)), 10);
    if (Number.isNaN(count) || count > MAX_MULTIBULK) {
      throw new ProtocolError("invalid multibulk length");
    }
    if (count <= 0) {
      // Empty/negative multibulk: skip, no command produced. Represent as empty.
      return { command: { name: "", args: [] }, next: header.next };
    }
    const args: Uint8Array[] = [];
    let offset = header.next;
    for (let i = 0; i < count; i++) {
      if (offset >= this.#len) return null;
      if (buf[offset] !== DOLLAR) throw new ProtocolError("expected bulk string");
      const lenLine = this.#readLine(offset);
      if (lenLine === null) return null;
      const len = parseInt(decodeAscii(buf.subarray(offset + 1, lenLine.end)), 10);
      if (Number.isNaN(len) || len < 0 || len > MAX_BULK_LEN) {
        throw new ProtocolError("invalid bulk length");
      }
      const dataStart = lenLine.next;
      const dataEnd = dataStart + len;
      if (dataEnd + 2 > this.#len) return null; // data + trailing CRLF not all here yet
      args.push(buf.slice(dataStart, dataEnd)); // copy: detach from the shared buffer
      offset = dataEnd + 2; // skip trailing CRLF
    }
    const name = decodeAscii(args[0]!).toUpperCase();
    return { command: { name, args }, next: offset };
  }

  #parseInline(start: number): { command: Command; next: number } | null {
    const line = this.#readLine(start);
    if (line === null) {
      if (this.#len - start > MAX_INLINE_LEN) {
        throw new ProtocolError("too big inline request");
      }
      return null;
    }
    const raw = this.#buf.subarray(start, line.end);
    if (raw.length > MAX_INLINE_LEN) throw new ProtocolError("too big inline request");
    const text = decodeAscii(raw).trim();
    if (text.length === 0) {
      return { command: { name: "", args: [] }, next: line.next };
    }
    const tokens = text.split(/\s+/);
    const args = tokens.map((t) => ENC.encode(t));
    return { command: { name: tokens[0]!.toUpperCase(), args }, next: line.next };
  }

  /**
   * Locate the next CRLF (or bare LF) at/after `from`.
   * Returns `end` (index of CR/LF, exclusive of the terminator) and `next`
   * (index just past the terminator), or null if no line terminator yet.
   */
  #readLine(from: number): { end: number; next: number } | null {
    const buf = this.#buf;
    const len = this.#len;
    for (let i = from; i < len; i++) {
      if (buf[i] === LF) {
        const end = i > from && buf[i - 1] === CR ? i - 1 : i;
        return { end, next: i + 1 };
      }
    }
    return null;
  }
}

export class ProtocolError extends Error {}

const ENC = new TextEncoder();

function decodeAscii(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]!);
  return s;
}
