/**
 * Single-writer guard for worker maintenance mode.
 *
 * Normally `PRAGMA locking_mode = EXCLUSIVE` keeps a second bundis process
 * off the same .db file. Worker mode needs a second *connection* in this
 * process (the maintenance worker), so it opens the file in NORMAL locking
 * mode and enforces the contract with `<db>.lock` instead: created with
 * O_EXCL, holding the owner's pid. A lock whose pid is no longer alive is
 * stale (crash) and is taken over.
 */

import { closeSync, openSync, readFileSync, unlinkSync, writeSync } from "node:fs";

export function lockPath(dbPath: string): string {
  return `${dbPath}.lock`;
}

/** Acquire the lock or throw; returns the release function. */
export function acquireDbLock(dbPath: string): () => void {
  const path = lockPath(dbPath);
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = openSync(path, "wx");
      writeSync(fd, String(process.pid));
      closeSync(fd);
      return () => {
        try {
          if (readFileSync(path, "utf8").trim() === String(process.pid)) unlinkSync(path);
        } catch {
          // already gone
        }
      };
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
      const owner = lockOwner(dbPath);
      if (owner !== null) throw inUse(dbPath, owner);
      try {
        unlinkSync(path); // stale lock from a crashed process
      } catch {
        // raced with another remover; retry
      }
    }
  }
  throw new Error(`cannot acquire lock file ${path}`);
}

/** Throw if a live process holds the lock (used by exclusive-mode servers). */
export function assertDbUnlocked(dbPath: string): void {
  const owner = lockOwner(dbPath);
  if (owner !== null) throw inUse(dbPath, owner);
}

/** Pid of a live lock owner, or null when unlocked / stale. */
function lockOwner(dbPath: string): number | null {
  let pid: number;
  try {
    pid = parseInt(readFileSync(lockPath(dbPath), "utf8").trim(), 10);
  } catch {
    return null;
  }
  if (!Number.isFinite(pid) || pid <= 0) return null;
  if (pid === process.pid) return pid; // another server in this process
  try {
    process.kill(pid, 0);
    return pid;
  } catch (err) {
    // EPERM: alive but owned by another user — still a live owner.
    return (err as NodeJS.ErrnoException).code === "EPERM" ? pid : null;
  }
}

function inUse(dbPath: string, pid: number): Error {
  return new Error(
    `database "${dbPath}" is already in use by another bundis process (pid ${pid}; ` +
      "the single-writer contract forbids sharing one .db file)",
  );
}
