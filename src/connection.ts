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

export type ConnState = "HANDSHAKE" | "READY" | "SUBSCRIBED";

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
  #closed = false;

  constructor(readonly socket: Socket<Connection>) {}

  /** Total active (P)SUBSCRIBE count, used in reply frames. */
  subscriptionCount(): number {
    return this.channels.size + this.patterns.size;
  }

  /** True once the client has at least one active subscription. */
  inSubscribeMode(): boolean {
    return this.subscriptionCount() > 0;
  }

  /** Serialize and write a reply, honoring backpressure. */
  send(reply: Reply): void {
    this.write(serialize(reply));
  }

  /** Write raw bytes, buffering any unflushed remainder for `drain`. */
  write(bytes: Uint8Array): void {
    if (this.#closed) return;
    if (this.#outbox.length > 0) {
      // Preserve ordering: once we're behind, everything queues.
      this.#outbox.push(bytes);
      return;
    }
    const written = this.socket.write(bytes);
    if (written < bytes.length) {
      this.#outbox.push(bytes.subarray(written));
    }
  }

  /** Called from the socket `drain` handler: flush what we can, in order. */
  flush(): void {
    while (this.#outbox.length > 0 && !this.#closed) {
      const chunk = this.#outbox[0]!;
      const written = this.socket.write(chunk);
      if (written < chunk.length) {
        this.#outbox[0] = chunk.subarray(written);
        return; // still backpressured
      }
      this.#outbox.shift();
    }
  }

  markClosed(): void {
    this.#closed = true;
    this.#outbox.length = 0;
  }
}
