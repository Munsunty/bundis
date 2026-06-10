/**
 * WatchRegistry — optimistic-lock version tracking for MULTI/WATCH/EXEC.
 *
 * Under the single-writer assumption (§5.3), correctness only requires detecting
 * whether any watched key was modified between WATCH and EXEC. The registry
 * tracks versions ONLY for currently-watched keys (refcounted): bump() is a
 * no-op when nobody watches, and entries die with their last watcher — so the
 * per-write tax is one Map miss and the registry cannot grow without bound.
 */

export class WatchRegistry {
  #entries = new Map<string, { version: number; refs: number }>();
  /** Whole-keyspace generation: bumped by FLUSHDB/FLUSHALL. */
  #epoch = 0;

  /** Bump a key's version. Called by storage on every mutation. */
  bump = (key: Uint8Array): void => {
    if (this.#entries.size === 0) return; // nobody watches anything
    const e = this.#entries.get(hashKey(key));
    if (e) e.version++;
  };

  /** WATCH: register interest in a key and snapshot its current version. */
  acquire(key: Uint8Array): number {
    const k = hashKey(key);
    let e = this.#entries.get(k);
    if (!e) {
      e = { version: 0, refs: 0 };
      this.#entries.set(k, e);
    }
    e.refs++;
    return e.version + this.#epoch;
  }

  /** Current version without registering interest (EXEC dirty check). */
  peek(key: Uint8Array): number {
    return (this.#entries.get(hashKey(key))?.version ?? 0) + this.#epoch;
  }

  /** Release interest registered by {@link acquire}. */
  release(key: Uint8Array): void {
    const k = hashKey(key);
    const e = this.#entries.get(k);
    if (!e) return;
    if (--e.refs <= 0) this.#entries.delete(k);
  }

  /**
   * FLUSHDB/FLUSHALL: every key changed. The epoch also covers keys whose
   * writes predate this process (persisted DB) and so have no entry.
   */
  bumpAll(): void {
    this.#epoch++;
  }

  /** Tracked-entry count (observability/tests). */
  size(): number {
    return this.#entries.size;
  }
}

/** A connection's WATCH snapshot: key (as hash) → version observed at WATCH. */
export type WatchSnapshot = Map<string, { key: Uint8Array; version: number }>;

/** Release every key in a snapshot (UNWATCH/EXEC/DISCARD/RESET/disconnect). */
export function releaseSnapshot(registry: WatchRegistry, snap: WatchSnapshot): void {
  for (const { key } of snap.values()) registry.release(key);
}

/** Stable map-key for a byte array (latin1 round-trips every byte). */
export function hashKey(key: Uint8Array): string {
  return Buffer.from(key.buffer, key.byteOffset, key.byteLength).toString("latin1");
}
