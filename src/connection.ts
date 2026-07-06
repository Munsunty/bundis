/**
 * L3 Connection — per-socket state machine and write path.
 *
 * Owns everything isolated to one client connection (§4.1.3): handshake/auth
 * state, selected DB, the streaming parser buffer, pub/sub subscriptions, the
 * MULTI transaction queue, and the WATCH snapshot. Also implements the
 * backpressure-aware {@link send} used by both the dispatcher and the PubSubHub.
 *
 * Replies serialize straight into a per-connection {@link RespWriter}. While
 * corked (one `data` event's whole pipeline), bytes accrue there and flush as a
 * single `socket.write` on uncork — §4.1.2 ordering is preserved because bytes
 * append in dispatch order.
 */

import type { Socket } from "bun";
import { RespWriter } from "./resp/serializer";
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

  /** Replies serialize here; flushed as one contiguous write per batch. */
  readonly #writer = new RespWriter();
  /** While corked, sends accrue in the writer instead of flushing per reply. */
  #corked = false;

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

  /**
   * Begin batching replies. A whole pipeline of commands from one `data` event
   * is processed under a cork so its replies coalesce into a single
   * `socket.write` instead of one syscall per command.
   */
  cork(): void {
    this.#corked = true;
  }

  /** Flush all batched replies as one ordered write and stop batching. */
  uncork(): void {
    this.#corked = false;
    this.#flush();
  }

  /** Flush any corked replies, then close the socket (QUIT path). */
  end(): void {
    this.uncork();
    this.socket.end();
  }

  /** Serialize a reply into the shared writer, honoring cork + backpressure. */
  send(reply: Reply): void {
    if (this.#closed) return;
    this.#writer.writeReply(reply);
    if (!this.#corked) this.#flush();
  }

  #flush(): void {
    if (this.#closed || this.#writer.length === 0) return;
    this.#writeNow(this.#writer.take());
  }

  /**
   * Write bytes now if possible. `bytes` may alias the shared writer buffer,
   * so anything not consumed by `socket.write` is copied before queueing.
   */
  #writeNow(bytes: Uint8Array): void {
    if (this.#outbox.length > 0) {
      // Preserve ordering: once we're behind, everything queues.
      this.#queue(bytes.slice());
      return;
    }
    const written = this.socket.write(bytes);
    if (written < bytes.length) {
      this.#queue(bytes.slice(written));
    }
  }

  /**
   * Queue backpressured bytes (an owned copy); a consumer that stops reading
   * (e.g. a stalled subscriber under PUBLISH load) is disconnected at the cap
   * rather than letting its outbox grow until the process OOMs.
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
    this.#corked = false;
    this.#writer.reset();
    this.#guard.sub(this.#outboxBytes);
    this.#outbox.length = 0;
    this.#outboxBytes = 0;
  }
}
