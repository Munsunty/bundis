/**
 * L3 Connection — per-socket state machine and write path.
 *
 * Owns everything isolated to one client connection (§4.1.3): handshake/auth
 * state, selected DB, the streaming parser buffer, pub/sub subscriptions, the
 * MULTI transaction queue, and the WATCH snapshot. Also implements the
 * backpressure-aware {@link send} used by both the dispatcher and the PubSubHub.
 */

import type { Socket } from "bun";
import { serialize } from "./resp/serializer";
import { RespParser } from "./resp/parser";
import type { Command, Reply } from "./resp/types";
import type { WatchSnapshot } from "./sidecar/watch";
import type { MemoryGuard } from "./sidecar/memory-guard";

export type ConnState = "HANDSHAKE" | "READY" | "SUBSCRIBED";

/** Hard ceiling for bytes queued toward one slow-reading client. */
const MAX_OUTBOX_BYTES = 32 * 1024 * 1024;

/** A buffered MULTI transaction. */
export interface TxnState {
  queued: Command[];
  /** Set if a command failed to queue (bad arity / unknown) → EXEC aborts. */
  error: boolean;
}

let nextId = 1;

export class Connection {
  readonly id = nextId++;
  state: ConnState = "HANDSHAKE";
  db = 0;
  authed = false;
  proto = 2; // upgraded to 3 on HELLO 3
  name = "";

  readonly parser = new RespParser();

  // pub/sub
  readonly channels = new Set<string>();
  readonly patterns = new Set<string>();

  // transactions
  txn: TxnState | null = null;
  watch: WatchSnapshot | null = null;

  /** Bytes awaiting a `drain` event when the socket buffer was full. */
  #outbox: Uint8Array[] = [];
  #outboxBytes = 0;
  #closed = false;
  readonly #guard: MemoryGuard;

  /** When corked, replies accrue here and flush as one write on uncork. */
  #corked = false;
  #corkBuf: Uint8Array[] = [];

  constructor(
    readonly socket: Socket<Connection>,
    guard: MemoryGuard,
  ) {
    this.#guard = guard;
  }

  /** Total active (P)SUBSCRIBE count, used in reply frames. */
  subscriptionCount(): number {
    return this.channels.size + this.patterns.size;
  }

  /** True once the client has at least one active subscription. */
  inSubscribeMode(): boolean {
    return this.subscriptionCount() > 0;
  }

  /** Replies buffered while corked (one data() event → one socket.write). */
  #cork: Uint8Array[] | null = null;

  /** Start buffering writes; {@link uncork} flushes them as one write. */
  cork(): void {
    if (this.#cork === null) this.#cork = [];
  }

  /** Flush all corked bytes with a single socket write. */
  uncork(): void {
    const parts = this.#cork;
    this.#cork = null;
    if (parts === null || parts.length === 0 || this.#closed) return;
    if (parts.length === 1) {
      this.#writeNow(parts[0]!);
      return;
    }
    let total = 0;
    for (const p of parts) total += p.length;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const p of parts) {
      out.set(p, offset);
      offset += p.length;
    }
    this.#writeNow(out);
  }

  /** Flush any corked replies, then close the socket (QUIT path). */
  end(): void {
    this.uncork();
    this.socket.end();
  }

  /** Serialize and write a reply, honoring backpressure. */
  send(reply: Reply): void {
    const bytes = serialize(reply);
    if (this.#corked) {
      this.#corkBuf.push(bytes);
      return;
    }
    this.write(bytes);
  }

  /**
   * Begin batching replies. A whole pipeline of commands from one `data` event
   * is processed under a cork so its replies coalesce into a single
   * `socket.write` instead of one syscall per command (§4.1.2 ordering is
   * preserved — bytes append in dispatch order).
   */
  cork(): void {
    this.#corked = true;
  }

  /** Flush all batched replies as one ordered write and stop batching. */
  uncork(): void {
    this.#corked = false;
    if (this.#corkBuf.length === 0) return;
    const merged = this.#corkBuf.length === 1 ? this.#corkBuf[0]! : concat(this.#corkBuf);
    this.#corkBuf.length = 0;
    this.write(merged);
  }

  /** Write raw bytes, buffering any unflushed remainder for `drain`. */
  write(bytes: Uint8Array): void {
    if (this.#closed) return;
    if (this.#cork !== null) {
      this.#cork.push(bytes);
      return;
    }
    this.#writeNow(bytes);
  }

  #writeNow(bytes: Uint8Array): void {
    if (this.#closed) return;
    if (this.#outbox.length > 0) {
      // Preserve ordering: once we're behind, everything queues.
      this.#queue(bytes);
      return;
    }
    const written = this.socket.write(bytes);
    if (written < bytes.length) {
      this.#queue(bytes.subarray(written));
    }
  }

  /**
   * Queue backpressured bytes; a consumer that stops reading (e.g. a stalled
   * subscriber under PUBLISH load) is disconnected at the cap rather than
   * letting its outbox grow until the process OOMs.
   */
  #queue(bytes: Uint8Array): void {
    this.#outboxBytes += bytes.length;
    this.#guard.add(bytes.length);
    if (this.#outboxBytes > MAX_OUTBOX_BYTES || this.#guard.overLimit) {
      this.markClosed();
      this.socket.end();
      return;
    }
    this.#outbox.push(bytes);
  }

  /** Called from the socket `drain` handler: flush what we can, in order. */
  flush(): void {
    while (this.#outbox.length > 0 && !this.#closed) {
      const chunk = this.#outbox[0]!;
      const written = this.socket.write(chunk);
      this.#outboxBytes -= written;
      this.#guard.sub(written);
      if (written < chunk.length) {
        this.#outbox[0] = chunk.subarray(written);
        return; // still backpressured
      }
      this.#outbox.shift();
    }
  }

  markClosed(): void {
    this.#closed = true;
    this.#cork = null;
    this.#guard.sub(this.#outboxBytes);
    this.#outbox.length = 0;
    this.#outboxBytes = 0;
    this.#corked = false;
    this.#corkBuf.length = 0;
  }
}

function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}
