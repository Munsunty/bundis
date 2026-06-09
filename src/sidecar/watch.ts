/**
 * WatchRegistry — optimistic-lock version tracking for MULTI/WATCH/EXEC.
 *
 * Under the single-writer assumption (§5.3), correctness only requires detecting
 * whether any watched key was modified between WATCH and EXEC. We keep an
 * in-process monotonic version per key, bumped on every write. WATCH snapshots
 * the current versions; EXEC compares. A missing key has version 0.
 */

export class WatchRegistry {
  #versions = new Map<string, number>();

  /** Bump a key's version. Called by storage on every mutation. */
  bump = (key: Uint8Array): void => {
    const k = hashKey(key);
    this.#versions.set(k, (this.#versions.get(k) ?? 0) + 1);
  };

  /** Current version of a key (0 if never written). */
  version(key: Uint8Array): number {
    return this.#versions.get(hashKey(key)) ?? 0;
  }
}

/** A connection's WATCH snapshot: key (as hash) → version observed at WATCH. */
export type WatchSnapshot = Map<string, { key: Uint8Array; version: number }>;

/** Stable map-key for a byte array (latin1 round-trips every byte). */
export function hashKey(key: Uint8Array): string {
  let s = "";
  for (let i = 0; i < key.length; i++) s += String.fromCharCode(key[i]!);
  return s;
}
