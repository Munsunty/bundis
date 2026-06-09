/**
 * Temp SQLite file helpers for persistence tests. In-memory tests use ":memory:"
 * directly and need none of this.
 */

import { tmpdir } from "node:os";
import { join } from "node:path";
import { existsSync, rmSync } from "node:fs";

export function mkTempDbPath(): string {
  const name = `bun-resp-sqlite-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  return join(tmpdir(), name);
}

/** Remove a temp db file plus its WAL/SHM sidecars. */
export function cleanupDb(path: string): void {
  for (const p of [path, `${path}-wal`, `${path}-shm`]) {
    if (existsSync(p)) rmSync(p, { force: true });
  }
}
