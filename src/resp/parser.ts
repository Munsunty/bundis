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
 */

import type { Command } from "./types";

const CR = 13;
const LF = 10;
const STAR = 42; // '*'
const DOLLAR = 36; // '$'

export class RespParser {
  /** Pending bytes not yet consumed into a full command. */
  #buf: Uint8Array = new Uint8Array(0);

  /** Append a freshly received chunk to the internal buffer. */
  push(chunk: Uint8Array): void {
    if (this.#buf.length === 0) {
      this.#buf = chunk;
      return;
    }
    const merged = new Uint8Array(this.#buf.length + chunk.length);
    merged.set(this.#buf, 0);
    merged.set(chunk, this.#buf.length);
    this.#buf = merged;
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
    if (offset > 0) {
      this.#buf = this.#buf.subarray(offset);
    }
    return out;
  }

  /** Parse a single command starting at `start`, or null if incomplete. */
  #parseOne(start: number): { command: Command; next: number } | null {
    const buf = this.#buf;
    if (start >= buf.length) return null;
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
    if (Number.isNaN(count)) throw new ProtocolError("invalid multibulk length");
    if (count <= 0) {
      // Empty/negative multibulk: skip, no command produced. Represent as empty.
      return { command: { name: "", args: [] }, next: header.next };
    }
    const args: Uint8Array[] = [];
    let offset = header.next;
    for (let i = 0; i < count; i++) {
      if (offset >= buf.length) return null;
      if (buf[offset] !== DOLLAR) throw new ProtocolError("expected bulk string");
      const lenLine = this.#readLine(offset);
      if (lenLine === null) return null;
      const len = parseInt(decodeAscii(buf.subarray(offset + 1, lenLine.end)), 10);
      if (Number.isNaN(len) || len < 0) throw new ProtocolError("invalid bulk length");
      const dataStart = lenLine.next;
      const dataEnd = dataStart + len;
      if (dataEnd + 2 > buf.length) return null; // data + trailing CRLF not all here yet
      args.push(buf.slice(dataStart, dataEnd)); // copy: detach from the shared buffer
      offset = dataEnd + 2; // skip trailing CRLF
    }
    const name = decodeAscii(args[0]!).toUpperCase();
    return { command: { name, args }, next: offset };
  }

  #parseInline(start: number): { command: Command; next: number } | null {
    const line = this.#readLine(start);
    if (line === null) return null;
    const raw = this.#buf.subarray(start, line.end);
    const text = decodeAscii(raw).trim();
    if (text.length === 0) {
      return { command: { name: "", args: [] }, next: line.next };
    }
    const tokens = text.split(/\s+/);
    const args = tokens.map((t) => new TextEncoder().encode(t));
    return { command: { name: tokens[0]!.toUpperCase(), args }, next: line.next };
  }

  /**
   * Locate the next CRLF (or bare LF) at/after `from`.
   * Returns `end` (index of CR/LF, exclusive of the terminator) and `next`
   * (index just past the terminator), or null if no line terminator yet.
   */
  #readLine(from: number): { end: number; next: number } | null {
    const buf = this.#buf;
    for (let i = from; i < buf.length; i++) {
      if (buf[i] === LF) {
        const end = i > from && buf[i - 1] === CR ? i - 1 : i;
        return { end, next: i + 1 };
      }
    }
    return null;
  }
}

export class ProtocolError extends Error {}

function decodeAscii(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]!);
  return s;
}
