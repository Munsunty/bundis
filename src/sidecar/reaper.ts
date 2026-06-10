/**
 * ExpiryReaper — active TTL sweep (§5.3.2).
 *
 * Lazy expiry (on read) keeps expired keys from leaking into replies, but does
 * not reclaim keys that are never touched. This periodic sweep deletes expired
 * rows in bulk so DBSIZE and on-disk size stay consistent.
 */

import type { StorageEngine } from "../storage/types";

export class ExpiryReaper {
  #timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly storage: StorageEngine,
    private readonly intervalMs: number,
  ) {}

  start(): void {
    if (this.#timer !== null) return;
    this.#timer = setInterval(() => {
      try {
        this.storage.sweepExpired(Date.now());
      } catch (err) {
        // A transient storage error (SQLITE_BUSY, disk) must not kill the
        // process; the next tick simply retries.
        console.error("bundis: expiry sweep failed:", err);
      }
    }, this.intervalMs);
    // Don't keep the process alive solely for the reaper.
    this.#timer.unref?.();
  }

  stop(): void {
    if (this.#timer !== null) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
  }
}
