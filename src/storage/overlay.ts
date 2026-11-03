/**
 * OverlayStorage — in-memory write buffer over the SQLite engine, used while
 * the maintenance worker owns the database write lock (IDLE mode).
 *
 * Why: SQLite allows one writer per file. While the worker deletes rows on its
 * own connection, any main-thread write would block the event loop on
 * busy_timeout. So during maintenance:
 *
 *   - writes never touch SQLite; they land in a per-key buffer entry
 *   - reads merge "buffer first, else SQLite" (WAL lets the main connection
 *     read while the worker writes)
 *   - when the worker is done, {@link flushStep} applies the buffer to SQLite
 *     in time-budgeted slices; once empty, the overlay goes back to a pure
 *     pass-through
 *
 * Only keys written during IDLE are buffered — untouched (cold) keys stay in
 * SQLite and are read straight through. Hot-cache contents never need to be
 * written back: the cache is write-through, so SQLite already holds them.
 *
 * Buffer model (per key, see {@link Entry}): a tombstone, or a typed entry
 * that is either a full replacement (`reset`: base content ignored) or a
 * delta over the base key (hash fields / set members / zset scores / list end
 * ops, plus an optional TTL override). Strings are always full replacements.
 *
 * Safety invariants (relied on by the maintenance controller):
 *   - While active, the base engine receives no writes except from flushStep.
 *   - The worker only deletes keys whose expiry is <= the cycle cutoff, which
 *     is taken before activation; such keys already read as absent here, so a
 *     buffered write on them is a `reset` entry and never depends on base rows
 *     the worker may be removing.
 *   - A delta entry's base rows are stable until the entry is flushed: the
 *     next cycle cannot start before the buffer is empty.
 *
 * Durability: buffered writes live only in memory until flushed. A process
 * crash during a maintenance cycle loses them (accepted trade-off); a clean
 * stop() flushes first.
 */

import { RespError, TypeMismatchError } from "../engine/errors";
import { concatBytes, formatFloat, parseFloatStrict, parseIntStrict } from "./sqlite";
import type {
  OverlayBase,
  RedisType,
  ScoreBound,
  SetOptions,
  StorageEngine,
  ZAddOptions,
} from "./types";

const ENC = new TextEncoder();

type Exp = number | null;

interface FieldDelta {
  /** New value, or null when the field was deleted. */
  v: Uint8Array | null;
  /** Whether the base hash had this field when it was first touched. */
  baseHad: boolean;
}
interface MemberDelta {
  present: boolean;
  baseHad: boolean;
}
interface ScoreDelta {
  /** New score, or null when the member was removed. */
  score: number | null;
  baseHad: boolean;
}
interface ListDelta {
  /** Base length when the entry was created (0 for reset entries). */
  baseLen: number;
  /** Base elements consumed from the head / tail. */
  popL: number;
  popR: number;
  /** LPUSHed values in push order (the head of the list is the last one). */
  left: Uint8Array[];
  /** RPUSHed values in push order (the tail of the list is the last one). */
  right: Uint8Array[];
}

interface Entry {
  /** null = tombstone: the key is absent, base content ignored. */
  type: RedisType | null;
  /** true = full replacement: base content ignored. */
  reset: boolean;
  /** TTL override; undefined = inherit the base key's expiry (delta entries). */
  expire: Exp | undefined;
  /** Base key's expiry when this delta entry was created. */
  baseExpire: Exp;
  str: Uint8Array | null;
  hash: Map<string, FieldDelta> | null;
  set: Map<string, MemberDelta> | null;
  zset: Map<string, ScoreDelta> | null;
  list: ListDelta | null;
  /** Approximate memory footprint (buffer-pressure accounting). */
  bytes: number;
}

/** A live key as seen through the overlay. */
interface View {
  entry: Entry | null;
  type: RedisType;
  expire: Exp;
  /**
   * `now` to use for base reads of this key. A delta entry whose TTL was
   * overridden (PERSIST/EXPIRE during IDLE) must keep reading its base rows
   * even after the base row's own expiry passed.
   */
  baseNow: number;
}

export interface OverlayHooks {
  /** Fired for every buffered mutation (WATCH versioning + cache eviction). */
  readonly onWrite?: (key: Uint8Array) => void;
  readonly onFlushAll?: () => void;
  /** Fired when the buffer grows past `maxBytes` (controller stops the worker). */
  readonly onPressure?: () => void;
}

export interface OverlayStats {
  active: boolean;
  entries: number;
  bytes: number;
  peakBytes: number;
  flushedEntries: number;
}

const ENTRY_OVERHEAD = 128;
const ELEM_OVERHEAD = 48;

export class OverlayStorage implements StorageEngine {
  #entries = new Map<string, Entry>();
  #active = false;
  /** FLUSHALL during IDLE: every base key reads as absent until flushed. */
  #baseCleared = false;
  #bytes = 0;
  #peakBytes = 0;
  #flushed = 0;
  #applying = false;
  /** Whether the reaper's main-thread sweep may run (off in worker mode). */
  mainThreadSweep: boolean;
  /** Buffer size that triggers onPressure. */
  maxBytes: number;

  readonly #onWrite: (key: Uint8Array) => void;
  readonly #onFlushAll: () => void;
  readonly #onPressure: () => void;

  constructor(
    private readonly base: OverlayBase,
    hooks: OverlayHooks = {},
    opts: { mainThreadSweep?: boolean; maxBytes?: number } = {},
  ) {
    this.#onWrite = hooks.onWrite ?? (() => {});
    this.#onFlushAll = hooks.onFlushAll ?? (() => {});
    this.#onPressure = hooks.onPressure ?? (() => {});
    this.mainThreadSweep = opts.mainThreadSweep ?? true;
    this.maxBytes = opts.maxBytes ?? 64 * 1024 * 1024;
  }

  // ── lifecycle (driven by the maintenance controller) ───────────────────--

  get active(): boolean {
    return this.#active;
  }

  /** True while flushStep is writing buffered entries into the base engine. */
  get applying(): boolean {
    return this.#applying;
  }

  get bufferedBytes(): number {
    return this.#bytes;
  }

  get bufferedEntries(): number {
    return this.#entries.size;
  }

  /** Start buffering writes (IDLE). */
  activate(): void {
    this.#active = true;
  }

  overlayStats(): OverlayStats {
    return {
      active: this.#active,
      entries: this.#entries.size,
      bytes: this.#bytes,
      peakBytes: this.#peakBytes,
      flushedEntries: this.#flushed,
    };
  }

  /**
   * Apply buffered entries to the base engine for at most `budgetMs`, inside
   * one base transaction. Returns the number of entries still buffered; when
   * it reaches 0 the overlay deactivates (pure pass-through again).
   *
   * Writes arriving between slices keep going to the buffer, so per-key order
   * is preserved: an entry is removed from the buffer in the same synchronous
   * step that writes it to SQLite.
   */
  flushStep(budgetMs: number, now: number): number {
    if (!this.#active) return 0;
    const start = performance.now();
    const applied: string[] = [];
    let clearedBase = false;
    this.#applying = true;
    try {
      this.base.withTransaction(() => {
        if (this.#baseCleared) {
          this.base.tombstoneAll();
          clearedBase = true;
        }
        for (const [k, e] of this.#entries) {
          this.#apply(k, e, now);
          applied.push(k);
          if (performance.now() - start >= budgetMs) break;
        }
      });
    } finally {
      this.#applying = false;
    }
    // Committed: only now drop the applied entries (a rolled-back slice keeps
    // its entries buffered and is retried by the next step).
    if (clearedBase) this.#baseCleared = false;
    for (const k of applied) {
      const e = this.#entries.get(k)!;
      this.#entries.delete(k);
      this.#bytes -= e.bytes;
      this.#flushed++;
    }
    if (this.#entries.size === 0 && !this.#baseCleared) {
      this.#active = false;
      this.#bytes = 0;
    }
    return this.#entries.size;
  }

  // ── meta / ttl ─────────────────────────────────────────────────────────--

  typeOf(key: Uint8Array, now: number): RedisType | null {
    if (!this.#active) return this.base.typeOf(key, now);
    return this.#live(key, now)?.type ?? null;
  }

  exists(key: Uint8Array, now: number): boolean {
    if (!this.#active) return this.base.exists(key, now);
    return this.#live(key, now) !== null;
  }

  del(keys: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.del(keys, now);
    let n = 0;
    for (const key of keys) {
      if (this.#live(key, now) !== null) {
        this.#tomb(mk(key));
        this.#onWrite(key);
        n++;
      }
    }
    return n;
  }

  expireSet(key: Uint8Array, atMs: number, now: number): boolean {
    if (!this.#active) return this.base.expireSet(key, atMs, now);
    const v = this.#live(key, now);
    if (!v) return false;
    const e = this.#ensureAny(key, v, now);
    e.expire = atMs;
    this.#touched(key, e, 0);
    return true;
  }

  pttl(key: Uint8Array, now: number): number {
    if (!this.#active) return this.base.pttl(key, now);
    const v = this.#live(key, now);
    if (!v) return -2;
    return v.expire === null ? -1 : Math.max(0, v.expire - now);
  }

  persist(key: Uint8Array, now: number): boolean {
    if (!this.#active) return this.base.persist(key, now);
    const v = this.#live(key, now);
    if (!v || v.expire === null) return false;
    const e = this.#ensureAny(key, v, now);
    e.expire = null;
    this.#touched(key, e, 0);
    return true;
  }

  sweepExpired(now: number): number {
    if (this.#active || !this.mainThreadSweep) return 0;
    return this.base.sweepExpired(now);
  }

  dbsize(now: number): number {
    if (!this.#active) return this.base.dbsize(now);
    let n = this.#baseCleared ? 0 : this.base.dbsize(now);
    for (const k of this.#entries.keys()) {
      const key = unmk(k);
      if (this.#live(key, now) !== null) n++;
      if (!this.#baseCleared && this.base.metaOf(key, now) !== null) n--;
    }
    return n;
  }

  flushAll(): void {
    if (!this.#active) return this.base.flushAll();
    this.#entries.clear();
    this.#bytes = 0;
    this.#baseCleared = true;
    this.#onFlushAll();
  }

  // ── string ─────────────────────────────────────────────────────────────--

  kvGet(key: Uint8Array, now: number): Uint8Array | null {
    return this.kvGetEx(key, now)?.value ?? null;
  }

  kvGetEx(key: Uint8Array, now: number): { value: Uint8Array; expireAtMs: number | null } | null {
    if (!this.#active) {
      return this.base.kvGetEx
        ? this.base.kvGetEx(key, now)
        : this.#baseKvGetEx(key, now);
    }
    const v = this.#live(key, now);
    if (!v) return null;
    if (v.type !== "string") throw new TypeMismatchError(v.type, "string");
    if (v.entry) return { value: v.entry.str!, expireAtMs: v.expire };
    return this.base.kvGetEx ? this.base.kvGetEx(key, now) : this.#baseKvGetEx(key, now);
  }

  kvSet(key: Uint8Array, value: Uint8Array, now: number, opts: SetOptions = {}): "set" | "noop" {
    if (!this.#active) return this.base.kvSet(key, value, now, opts);
    const v = this.#live(key, now);
    if (opts.mode === "NX" && v !== null) return "noop";
    if (opts.mode === "XX" && v === null) return "noop";
    const expire =
      opts.expireAtMs !== undefined && opts.expireAtMs !== null
        ? opts.expireAtMs
        : opts.keepTtl && v?.type === "string"
          ? v.expire
          : null;
    this.#putString(key, value, expire);
    return "set";
  }

  incrBy(key: Uint8Array, delta: bigint, now: number): bigint {
    if (!this.#active) return this.base.incrBy(key, delta, now);
    const v = this.#live(key, now);
    const cur = this.kvGet(key, now); // WRONGTYPE check
    const next = (cur === null ? 0n : parseIntStrict(cur)) + delta;
    this.#putString(key, ENC.encode(next.toString()), v?.expire ?? null);
    return next;
  }

  incrByFloat(key: Uint8Array, delta: number, now: number): number {
    if (!this.#active) return this.base.incrByFloat(key, delta, now);
    const v = this.#live(key, now);
    const cur = this.kvGet(key, now);
    const next = (cur === null ? 0 : parseFloatStrict(cur)) + delta;
    if (!Number.isFinite(next)) {
      throw new RespError("ERR", "increment would produce NaN or Infinity");
    }
    this.#putString(key, ENC.encode(formatFloat(next)), v?.expire ?? null);
    return next;
  }

  append(key: Uint8Array, value: Uint8Array, now: number): number {
    if (!this.#active) return this.base.append(key, value, now);
    const cur = this.kvGet(key, now);
    const next = cur === null ? value : concatBytes(cur, value);
    this.kvSet(key, next, now, { keepTtl: true });
    return next.length;
  }

  // ── hash ───────────────────────────────────────────────────────────────--

  hSet(
    key: Uint8Array,
    pairs: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
    now: number,
  ): number {
    if (!this.#active) return this.base.hSet(key, pairs, now);
    const [e, baseNow] = this.#ensure(key, "hash", now);
    let added = 0;
    let grew = 0;
    for (const [field, value] of pairs) {
      const fk = mk(field);
      const d = e.hash!.get(fk);
      let baseHad: boolean;
      let existed: boolean;
      if (d) {
        baseHad = d.baseHad;
        existed = d.v !== null;
      } else {
        baseHad = !e.reset && this.base.hGet(key, field, baseNow) !== null;
        existed = baseHad;
      }
      if (!existed) added++;
      e.hash!.set(fk, { v: value, baseHad });
      grew += field.length + value.length + ELEM_OVERHEAD;
    }
    this.#touched(key, e, grew);
    return added;
  }

  hGet(key: Uint8Array, field: Uint8Array, now: number): Uint8Array | null {
    if (!this.#active) return this.base.hGet(key, field, now);
    const v = this.#liveTyped(key, "hash", now);
    if (!v) return null;
    if (!v.entry) return this.base.hGet(key, field, now);
    const d = v.entry.hash!.get(mk(field));
    if (d) return d.v;
    return v.entry.reset ? null : this.base.hGet(key, field, v.baseNow);
  }

  hDel(key: Uint8Array, fields: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.hDel(key, fields, now);
    const v = this.#liveTyped(key, "hash", now);
    if (!v) return 0;
    const [e, baseNow] = this.#ensure(key, "hash", now);
    let n = 0;
    for (const field of fields) {
      const fk = mk(field);
      const d = e.hash!.get(fk);
      const baseHad = d ? d.baseHad : !e.reset && this.base.hGet(key, field, baseNow) !== null;
      const existed = d ? d.v !== null : baseHad;
      if (existed) {
        n++;
        e.hash!.set(fk, { v: null, baseHad });
      }
    }
    if (this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, fields.length * ELEM_OVERHEAD);
    this.#onWrite(key);
    return n;
  }

  hGetAll(key: Uint8Array, now: number): Array<[Uint8Array, Uint8Array]> {
    if (!this.#active) return this.base.hGetAll(key, now);
    const v = this.#liveTyped(key, "hash", now);
    if (!v) return [];
    if (!v.entry) return this.base.hGetAll(key, now);
    const e = v.entry;
    const out: Array<[Uint8Array, Uint8Array]> = [];
    if (!e.reset) {
      for (const pair of this.base.hGetAll(key, v.baseNow)) {
        if (!e.hash!.has(mk(pair[0]))) out.push(pair);
      }
    }
    for (const [fk, d] of e.hash!) if (d.v !== null) out.push([unmk(fk), d.v]);
    return out;
  }

  hKeys(key: Uint8Array, now: number): Uint8Array[] {
    if (!this.#active) return this.base.hKeys(key, now);
    return this.hGetAll(key, now).map((p) => p[0]);
  }

  hVals(key: Uint8Array, now: number): Uint8Array[] {
    if (!this.#active) return this.base.hVals(key, now);
    return this.hGetAll(key, now).map((p) => p[1]);
  }

  hLen(key: Uint8Array, now: number): number {
    if (!this.#active) return this.base.hLen(key, now);
    const v = this.#liveTyped(key, "hash", now);
    if (!v) return 0;
    if (!v.entry) return this.base.hLen(key, now);
    const e = v.entry;
    let n = e.reset ? 0 : this.base.hLen(key, v.baseNow);
    for (const d of e.hash!.values()) {
      if (d.v !== null && !d.baseHad) n++;
      else if (d.v === null && d.baseHad) n--;
    }
    return n;
  }

  hExists(key: Uint8Array, field: Uint8Array, now: number): boolean {
    if (!this.#active) return this.base.hExists(key, field, now);
    return this.hGet(key, field, now) !== null;
  }

  hIncrBy(key: Uint8Array, field: Uint8Array, delta: bigint, now: number): bigint {
    if (!this.#active) return this.base.hIncrBy(key, field, delta, now);
    const cur = this.hGet(key, field, now);
    const next = (cur === null ? 0n : parseIntStrict(cur)) + delta;
    this.hSet(key, [[field, ENC.encode(next.toString())]], now);
    return next;
  }

  hIncrByFloat(key: Uint8Array, field: Uint8Array, delta: number, now: number): number {
    if (!this.#active) return this.base.hIncrByFloat(key, field, delta, now);
    const cur = this.hGet(key, field, now);
    const next = (cur === null ? 0 : parseFloatStrict(cur)) + delta;
    this.hSet(key, [[field, ENC.encode(formatFloat(next))]], now);
    return next;
  }

  // ── set ────────────────────────────────────────────────────────────────--

  sAdd(key: Uint8Array, members: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.sAdd(key, members, now);
    const [e, baseNow] = this.#ensure(key, "set", now);
    let added = 0;
    let grew = 0;
    for (const m of members) {
      const mkey = mk(m);
      const d = e.set!.get(mkey);
      const baseHad = d ? d.baseHad : !e.reset && this.base.sIsMember(key, m, baseNow);
      const existed = d ? d.present : baseHad;
      if (!existed) {
        added++;
        e.set!.set(mkey, { present: true, baseHad });
        grew += m.length + ELEM_OVERHEAD;
      }
    }
    if (e.reset && this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, grew);
    return added;
  }

  sRem(key: Uint8Array, members: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.sRem(key, members, now);
    const v = this.#liveTyped(key, "set", now);
    if (!v) return 0;
    const [e, baseNow] = this.#ensure(key, "set", now);
    let n = 0;
    for (const m of members) n += this.#setRemove(key, e, m, baseNow) ? 1 : 0;
    if (this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, members.length * ELEM_OVERHEAD);
    this.#onWrite(key);
    return n;
  }

  sIsMember(key: Uint8Array, member: Uint8Array, now: number): boolean {
    if (!this.#active) return this.base.sIsMember(key, member, now);
    const v = this.#liveTyped(key, "set", now);
    if (!v) return false;
    if (!v.entry) return this.base.sIsMember(key, member, now);
    const d = v.entry.set!.get(mk(member));
    if (d) return d.present;
    return !v.entry.reset && this.base.sIsMember(key, member, v.baseNow);
  }

  sMembers(key: Uint8Array, now: number): Uint8Array[] {
    if (!this.#active) return this.base.sMembers(key, now);
    const v = this.#liveTyped(key, "set", now);
    if (!v) return [];
    if (!v.entry) return this.base.sMembers(key, now);
    return this.#setMerged(key, v.entry, v.baseNow);
  }

  sCard(key: Uint8Array, now: number): number {
    if (!this.#active) return this.base.sCard(key, now);
    const v = this.#liveTyped(key, "set", now);
    if (!v) return 0;
    if (!v.entry) return this.base.sCard(key, now);
    const e = v.entry;
    let n = e.reset ? 0 : this.base.sCard(key, v.baseNow);
    for (const d of e.set!.values()) {
      if (d.present && !d.baseHad) n++;
      else if (!d.present && d.baseHad) n--;
    }
    return n;
  }

  sRandMember(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    if (!this.#active) return this.base.sRandMember(key, count, now);
    const v = this.#liveTyped(key, "set", now);
    if (!v) return count === null ? null : [];
    if (!v.entry) return this.base.sRandMember(key, count, now);
    return pickRandom(this.#setMerged(key, v.entry, v.baseNow), count);
  }

  sPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    if (!this.#active) return this.base.sPop(key, count, now);
    const v = this.#liveTyped(key, "set", now);
    if (!v) return count === null ? null : [];
    // Pick without writing to the base: SQL-side sampling when the key has no
    // buffered delta yet, otherwise sample the merged member list.
    const picked = v.entry
      ? pickRandom(this.#setMerged(key, v.entry, v.baseNow), count === null ? 1 : Math.max(0, count))
      : this.base.sRandMember(key, count === null ? 1 : Math.max(0, count), now);
    const list = picked as Uint8Array[];
    if (list.length === 0) return count === null ? null : [];
    const [e, baseNow] = this.#ensure(key, "set", now);
    for (const m of list) this.#setRemove(key, e, m, baseNow);
    if (this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, list.length * ELEM_OVERHEAD);
    this.#onWrite(key);
    return count === null ? list[0]! : list;
  }

  // ── list ───────────────────────────────────────────────────────────────--

  lPush(key: Uint8Array, values: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.lPush(key, values, now);
    const [e] = this.#ensure(key, "list", now);
    let grew = 0;
    for (const v of values) {
      e.list!.left.push(v);
      grew += v.length + ELEM_OVERHEAD;
    }
    this.#touched(key, e, grew);
    return listLen(e);
  }

  rPush(key: Uint8Array, values: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.rPush(key, values, now);
    const [e] = this.#ensure(key, "list", now);
    let grew = 0;
    for (const v of values) {
      e.list!.right.push(v);
      grew += v.length + ELEM_OVERHEAD;
    }
    this.#touched(key, e, grew);
    return listLen(e);
  }

  lPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    if (!this.#active) return this.base.lPop(key, count, now);
    return this.#pop(key, count, "L", now);
  }

  rPop(key: Uint8Array, count: number | null, now: number): Uint8Array[] | Uint8Array | null {
    if (!this.#active) return this.base.rPop(key, count, now);
    return this.#pop(key, count, "R", now);
  }

  lRange(key: Uint8Array, start: number, stop: number, now: number): Uint8Array[] {
    if (!this.#active) return this.base.lRange(key, start, stop, now);
    const v = this.#liveTyped(key, "list", now);
    if (!v) return [];
    if (!v.entry) return this.base.lRange(key, start, stop, now);
    const l = v.entry.list!;
    const len = listLen(v.entry);
    const s = Math.max(0, start < 0 ? len + start : start);
    const e = Math.min(len - 1, stop < 0 ? len + stop : stop);
    if (s > e) return [];
    const out: Uint8Array[] = [];
    const L = l.left.length;
    const avail = l.baseLen - l.popL - l.popR;
    // Left-pushed section: rank i is left[L - 1 - i].
    for (let i = s; i <= Math.min(e, L - 1); i++) out.push(l.left[L - 1 - i]!);
    // Base section.
    const bs = Math.max(s, L) - L;
    const be = Math.min(e, L + avail - 1) - L;
    if (bs <= be) {
      for (const x of this.base.lRange(key, l.popL + bs, l.popL + be, v.baseNow)) out.push(x);
    }
    // Right-pushed section.
    for (let i = Math.max(s, L + avail); i <= e; i++) out.push(l.right[i - L - avail]!);
    return out;
  }

  lLen(key: Uint8Array, now: number): number {
    if (!this.#active) return this.base.lLen(key, now);
    const v = this.#liveTyped(key, "list", now);
    if (!v) return 0;
    if (!v.entry) return this.base.lLen(key, now);
    return listLen(v.entry);
  }

  lIndex(key: Uint8Array, index: number, now: number): Uint8Array | null {
    if (!this.#active) return this.base.lIndex(key, index, now);
    const v = this.#liveTyped(key, "list", now);
    if (!v) return null;
    if (!v.entry) return this.base.lIndex(key, index, now);
    const len = listLen(v.entry);
    const i = index < 0 ? len + index : index;
    if (i < 0 || i >= len) return null;
    return this.lRange(key, i, i, now)[0] ?? null;
  }

  // ── zset ───────────────────────────────────────────────────────────────--

  zAdd(
    key: Uint8Array,
    entries: ReadonlyArray<readonly [number, Uint8Array]>,
    now: number,
    opts: ZAddOptions = {},
  ): number {
    if (!this.#active) return this.base.zAdd(key, entries, now, opts);
    const v = this.#liveTyped(key, "zset", now);
    // XX against a missing key must not create it.
    if (!v && opts.mode === "XX") return 0;
    const [e, baseNow] = this.#ensure(key, "zset", now);
    let added = 0;
    let changed = 0;
    let grew = 0;
    for (const [score, member] of entries) {
      const [cur, baseHad] = this.#zCurrent(key, e, member, baseNow);
      if (cur === null) {
        if (opts.mode === "XX") continue;
        e.zset!.set(mk(member), { score, baseHad });
        grew += member.length + ELEM_OVERHEAD;
        added++;
      } else {
        if (opts.mode === "NX") continue;
        if (opts.gt && !(score > cur)) continue;
        if (opts.lt && !(score < cur)) continue;
        if (score !== cur) {
          e.zset!.set(mk(member), { score, baseHad });
          grew += member.length + ELEM_OVERHEAD;
          changed++;
        }
      }
    }
    if (e.reset && this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, grew);
    this.#onWrite(key);
    return added + (opts.ch ? changed : 0);
  }

  zIncr(
    key: Uint8Array,
    delta: number,
    member: Uint8Array,
    now: number,
    opts: ZAddOptions = {},
  ): number | null {
    if (!this.#active) return this.base.zIncr(key, delta, member, now, opts);
    const v = this.#liveTyped(key, "zset", now);
    if (!v && opts.mode === "XX") return null;
    const [e, baseNow] = this.#ensure(key, "zset", now);
    const [cur, baseHad] = this.#zCurrent(key, e, member, baseNow);
    let result: number | null;
    if (cur === null) {
      if (opts.mode === "XX") result = null;
      else {
        e.zset!.set(mk(member), { score: delta, baseHad });
        result = delta;
      }
    } else if (opts.mode === "NX") {
      result = null;
    } else {
      const next = cur + delta;
      if (Number.isNaN(next)) {
        if (e.reset && this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
        throw new RespError("ERR", "resulting score is not a number (NaN)");
      }
      if ((opts.gt && !(next > cur)) || (opts.lt && !(next < cur))) result = null;
      else {
        e.zset!.set(mk(member), { score: next, baseHad });
        result = next;
      }
    }
    if (e.reset && this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, member.length + ELEM_OVERHEAD);
    if (result !== null) this.#onWrite(key);
    return result;
  }

  zScore(key: Uint8Array, member: Uint8Array, now: number): number | null {
    if (!this.#active) return this.base.zScore(key, member, now);
    const v = this.#liveTyped(key, "zset", now);
    if (!v) return null;
    if (!v.entry) return this.base.zScore(key, member, now);
    return this.#zCurrent(key, v.entry, member, v.baseNow)[0];
  }

  zCard(key: Uint8Array, now: number): number {
    if (!this.#active) return this.base.zCard(key, now);
    const v = this.#liveTyped(key, "zset", now);
    if (!v) return 0;
    if (!v.entry) return this.base.zCard(key, now);
    const e = v.entry;
    let n = e.reset ? 0 : this.base.zCard(key, v.baseNow);
    for (const d of e.zset!.values()) {
      if (d.score !== null && !d.baseHad) n++;
      else if (d.score === null && d.baseHad) n--;
    }
    return n;
  }

  zRem(key: Uint8Array, members: Uint8Array[], now: number): number {
    if (!this.#active) return this.base.zRem(key, members, now);
    const v = this.#liveTyped(key, "zset", now);
    if (!v) return 0;
    const [e, baseNow] = this.#ensure(key, "zset", now);
    let n = 0;
    for (const m of members) {
      const [cur, baseHad] = this.#zCurrent(key, e, m, baseNow);
      if (cur !== null) {
        n++;
        e.zset!.set(mk(m), { score: null, baseHad });
      }
    }
    if (this.#isEmpty(key, e, baseNow)) this.#tomb(mk(key));
    else this.#touched(key, e, members.length * ELEM_OVERHEAD);
    this.#onWrite(key);
    return n;
  }

  zRank(key: Uint8Array, member: Uint8Array, now: number): number | null {
    if (!this.#active) return this.base.zRank(key, member, now);
    const v = this.#liveTyped(key, "zset", now);
    if (!v) return null;
    if (!v.entry) return this.base.zRank(key, member, now);
    const all = this.#zMerged(key, v.entry, v.baseNow);
    const i = all.findIndex(([m]) => Buffer.compare(m, member) === 0);
    return i === -1 ? null : i;
  }

  zRangeByRank(
    key: Uint8Array,
    start: number,
    stop: number,
    rev: boolean,
    now: number,
  ): Array<[Uint8Array, number]> {
    if (!this.#active) return this.base.zRangeByRank(key, start, stop, rev, now);
    const v = this.#liveTyped(key, "zset", now);
    if (!v) return [];
    if (!v.entry) return this.base.zRangeByRank(key, start, stop, rev, now);
    const all = this.#zMerged(key, v.entry, v.baseNow);
    if (rev) all.reverse();
    const len = all.length;
    const s = Math.max(0, start < 0 ? len + start : start);
    const e = Math.min(len - 1, stop < 0 ? len + stop : stop);
    if (s > e) return [];
    return all.slice(s, e + 1);
  }

  zRangeByScore(
    key: Uint8Array,
    min: ScoreBound,
    max: ScoreBound,
    limit: { offset: number; count: number } | null,
    now: number,
  ): Array<[Uint8Array, number]> {
    if (!this.#active) return this.base.zRangeByScore(key, min, max, limit, now);
    const v = this.#liveTyped(key, "zset", now);
    if (!v) return [];
    if (!v.entry) return this.base.zRangeByScore(key, min, max, limit, now);
    const inRange = this.#zMerged(key, v.entry, v.baseNow).filter(([, s]) => {
      const okMin = min.exclusive ? s > min.value : s >= min.value;
      const okMax = max.exclusive ? s < max.value : s <= max.value;
      return okMin && okMax;
    });
    const offset = limit === null ? 0 : limit.offset;
    const count = limit === null || limit.count < 0 ? Infinity : limit.count;
    return inRange.slice(offset, count === Infinity ? undefined : offset + count);
  }

  // ── atomicity / lifecycle ──────────────────────────────────────────────--

  withTransaction<T>(fn: () => T): T {
    // While buffering, nothing is written to SQLite, so there is nothing to
    // wrap (and opening a base transaction would contend with the worker).
    if (this.#active) return fn();
    return this.base.withTransaction(fn);
  }

  close(): void {
    this.base.close();
  }

  // ── internals: views ───────────────────────────────────────────────────--

  #live(key: Uint8Array, now: number): View | null {
    const e = this.#entries.get(mk(key));
    if (!e) {
      if (this.#baseCleared) return null;
      const meta = this.base.metaOf(key, now);
      if (!meta) return null;
      return { entry: null, type: meta.type, expire: meta.expireAtMs, baseNow: now };
    }
    if (e.type === null) return null;
    if (e.reset) {
      const exp = e.expire ?? null;
      if (exp !== null && exp <= now) return null;
      return { entry: e, type: e.type, expire: exp, baseNow: now };
    }
    const exp = e.expire === undefined ? e.baseExpire : e.expire;
    if (exp !== null && exp <= now) return null;
    const baseNow = e.baseExpire === null ? now : Math.min(now, e.baseExpire - 1);
    return { entry: e, type: e.type, expire: exp, baseNow };
  }

  #liveTyped(key: Uint8Array, want: RedisType, now: number): View | null {
    const v = this.#live(key, now);
    if (v && v.type !== want) throw new TypeMismatchError(v.type, want);
    return v;
  }

  /**
   * Entry to mutate for a type-specific write. Live base key without an entry
   * → delta entry; absent key → reset entry. Throws WRONGTYPE on a mismatch.
   */
  #ensure(key: Uint8Array, want: RedisType, now: number): [Entry, number] {
    const v = this.#liveTyped(key, want, now);
    if (v?.entry) return [v.entry, v.baseNow];
    const k = mk(key);
    const old = this.#entries.get(k);
    if (old) this.#bytes -= old.bytes; // tombstone / expired entry being replaced
    let e: Entry;
    if (v) {
      e = newEntry(want, false, undefined, v.expire);
      if (want === "list") e.list!.baseLen = this.base.lLen(key, now);
    } else {
      e = newEntry(want, true, null, null);
    }
    e.bytes = ENTRY_OVERHEAD + key.length;
    this.#bytes += e.bytes;
    this.#entries.set(k, e);
    return [e, v ? v.baseNow : now];
  }

  /** Entry for a TTL change on a live key of any type. */
  #ensureAny(key: Uint8Array, v: View, now: number): Entry {
    if (v.entry) return v.entry;
    if (v.type === "string") {
      // Strings are single values: materialize as a full replacement.
      const value = this.kvGet(key, now)!;
      this.#putString(key, value, v.expire);
      return this.#entries.get(mk(key))!;
    }
    return this.#ensure(key, v.type, now)[0];
  }

  #putString(key: Uint8Array, value: Uint8Array, expire: Exp): void {
    const k = mk(key);
    const old = this.#entries.get(k);
    if (old) this.#bytes -= old.bytes;
    const e = newEntry("string", true, expire, null);
    e.str = value;
    e.bytes = ENTRY_OVERHEAD + key.length + value.length;
    this.#entries.set(k, e);
    this.#bytes += e.bytes;
    this.#afterWrite(key);
  }

  #tomb(k: string): void {
    const old = this.#entries.get(k);
    if (old) this.#bytes -= old.bytes;
    const e = newEntry(null, true, null, null);
    e.bytes = ENTRY_OVERHEAD + k.length;
    this.#entries.set(k, e);
    this.#bytes += e.bytes;
    this.#checkPressure();
  }

  #touched(key: Uint8Array, e: Entry, grew: number): void {
    e.bytes += grew;
    this.#bytes += grew;
    this.#afterWrite(key);
  }

  #afterWrite(key: Uint8Array): void {
    this.#onWrite(key);
    this.#checkPressure();
  }

  #checkPressure(): void {
    if (this.#bytes > this.#peakBytes) this.#peakBytes = this.#bytes;
    if (this.#bytes > this.maxBytes) this.#onPressure();
  }

  /** Whether a (live) entry has no members left, merged with its base. */
  #isEmpty(key: Uint8Array, e: Entry, baseNow: number): boolean {
    void baseNow;
    switch (e.type) {
      case "list":
        return listLen(e) === 0;
      case "string":
      case null:
        return false;
    }
    let removedFromBase = 0;
    const deltas: Iterable<{ gone: boolean; baseHad: boolean }> =
      e.type === "hash"
        ? [...e.hash!.values()].map((d) => ({ gone: d.v === null, baseHad: d.baseHad }))
        : e.type === "set"
          ? [...e.set!.values()].map((d) => ({ gone: !d.present, baseHad: d.baseHad }))
          : [...e.zset!.values()].map((d) => ({ gone: d.score === null, baseHad: d.baseHad }));
    for (const d of deltas) {
      if (!d.gone) return false;
      if (d.baseHad) removedFromBase++;
    }
    if (e.reset) return true;
    // Every removed member was in the base; the base outlives the deltas only
    // if it holds more rows than were removed (bounded count, not COUNT(*)).
    return this.base.countUpTo(key, e.type!, removedFromBase + 1) <= removedFromBase;
  }

  #setRemove(key: Uint8Array, e: Entry, m: Uint8Array, baseNow: number): boolean {
    const mkey = mk(m);
    const d = e.set!.get(mkey);
    const baseHad = d ? d.baseHad : !e.reset && this.base.sIsMember(key, m, baseNow);
    const existed = d ? d.present : baseHad;
    if (existed) e.set!.set(mkey, { present: false, baseHad });
    return existed;
  }

  #setMerged(key: Uint8Array, e: Entry, baseNow: number): Uint8Array[] {
    const out: Uint8Array[] = [];
    if (!e.reset) {
      for (const m of this.base.sMembers(key, baseNow)) if (!e.set!.has(mk(m))) out.push(m);
    }
    for (const [mkey, d] of e.set!) if (d.present) out.push(unmk(mkey));
    return out;
  }

  /** [current merged score or null, whether the base had the member]. */
  #zCurrent(key: Uint8Array, e: Entry, member: Uint8Array, baseNow: number): [number | null, boolean] {
    const d = e.zset!.get(mk(member));
    if (d) return [d.score, d.baseHad];
    if (e.reset) return [null, false];
    const s = this.base.zScore(key, member, baseNow);
    return [s, s !== null];
  }

  /** All members of a buffered zset, ascending by (score, member bytes). */
  #zMerged(key: Uint8Array, e: Entry, baseNow: number): Array<[Uint8Array, number]> {
    const out: Array<[Uint8Array, number]> = [];
    if (!e.reset) {
      for (const pair of this.base.zRangeByRank(key, 0, -1, false, baseNow)) {
        if (!e.zset!.has(mk(pair[0]))) out.push(pair);
      }
    }
    for (const [mkey, d] of e.zset!) if (d.score !== null) out.push([unmk(mkey), d.score]);
    out.sort((a, b) => (a[1] !== b[1] ? a[1] - b[1] : Buffer.compare(a[0], b[0])));
    return out;
  }

  #pop(key: Uint8Array, count: number | null, side: "L" | "R", now: number): Uint8Array[] | Uint8Array | null {
    const v = this.#liveTyped(key, "list", now);
    if (!v) return null;
    const [e, baseNow] = this.#ensure(key, "list", now);
    const l = e.list!;
    const len = listLen(e);
    const k = count === null ? 1 : Math.min(count, len);
    if (k === 0) return [];
    const out: Uint8Array[] = [];
    let need = k;
    const avail = l.baseLen - l.popL - l.popR;
    if (side === "L") {
      while (need > 0 && l.left.length > 0) {
        out.push(l.left.pop()!);
        need--;
      }
      const fromBase = Math.min(need, avail);
      if (fromBase > 0) {
        for (const x of this.base.lRange(key, l.popL, l.popL + fromBase - 1, baseNow)) out.push(x);
        l.popL += fromBase;
        need -= fromBase;
      }
      if (need > 0) out.push(...l.right.splice(0, need));
    } else {
      while (need > 0 && l.right.length > 0) {
        out.push(l.right.pop()!);
        need--;
      }
      const fromBase = Math.min(need, avail);
      if (fromBase > 0) {
        const hi = l.popL + avail - 1;
        const got = this.base.lRange(key, hi - fromBase + 1, hi, baseNow);
        for (let i = got.length - 1; i >= 0; i--) out.push(got[i]!);
        l.popR += fromBase;
        need -= fromBase;
      }
      if (need > 0) out.push(...l.left.splice(0, need));
    }
    if (listLen(e) === 0) this.#tomb(mk(key));
    else this.#touched(key, e, 0);
    this.#onWrite(key);
    return count === null ? out[0]! : out;
  }

  #baseKvGetEx(key: Uint8Array, now: number): { value: Uint8Array; expireAtMs: number | null } | null {
    const value = this.base.kvGet(key, now);
    if (value === null) return null;
    const ttl = this.base.pttl(key, now);
    return { value, expireAtMs: ttl < 0 ? null : now + ttl };
  }

  // ── internals: flush ───────────────────────────────────────────────────--

  /** Write one buffered entry into the base engine (inside flushStep's txn). */
  #apply(k: string, e: Entry, now: number): void {
    const key = unmk(k);
    const base = this.base;
    const view = e.type === null ? null : this.#live(key, now);
    if (view === null) {
      // Tombstone (or a buffered key whose TTL has passed): delete the live
      // base key. In lazy-delete mode this is an O(1) tombstone; the next
      // maintenance cycle reclaims the rows.
      if (base.metaOf(key, now) !== null) base.del([key], now);
      return;
    }
    if (e.reset) {
      if (e.type !== "string" && base.metaOf(key, now) !== null) base.del([key], now);
      switch (e.type) {
        case "string":
          base.kvSet(key, e.str!, now, { expireAtMs: view.expire });
          return;
        case "hash":
          base.hSet(key, hashSets(e), now);
          break;
        case "set":
          base.sAdd(key, setAdds(e), now);
          break;
        case "zset":
          base.zAdd(key, zsetUpserts(e), now);
          break;
        case "list":
          base.rPush(key, [...e.list!.left].reverse().concat(e.list!.right), now);
          break;
      }
    } else {
      // Delta entry: re-apply a TTL override first so the base rows read as
      // live (their own expiry may have passed while the override was buffered).
      if (e.expire !== undefined) base.setExpireRaw(key, e.expire);
      switch (e.type) {
        case "hash": {
          const sets = hashSets(e);
          const dels = [...e.hash!].filter(([, d]) => d.v === null && d.baseHad).map(([f]) => unmk(f));
          if (sets.length > 0) base.hSet(key, sets, now);
          if (dels.length > 0) base.hDel(key, dels, now);
          break;
        }
        case "set": {
          const adds = setAdds(e);
          const rems = [...e.set!].filter(([, d]) => !d.present && d.baseHad).map(([m]) => unmk(m));
          if (adds.length > 0) base.sAdd(key, adds, now);
          if (rems.length > 0) base.sRem(key, rems, now);
          break;
        }
        case "zset": {
          const ups = zsetUpserts(e);
          const rems = [...e.zset!].filter(([, d]) => d.score === null && d.baseHad).map(([m]) => unmk(m));
          if (ups.length > 0) base.zAdd(key, ups, now);
          if (rems.length > 0) base.zRem(key, rems, now);
          break;
        }
        case "list": {
          const l = e.list!;
          if (l.popL > 0) base.lPop(key, l.popL, now);
          if (l.popR > 0) base.rPop(key, l.popR, now);
          if (l.left.length > 0) base.lPush(key, l.left, now);
          if (l.right.length > 0) base.rPush(key, l.right, now);
          break;
        }
      }
    }
    // The merged view's expiry is the truth. Re-assert it last: a list whose
    // base rows were all popped (then refilled) was recreated without a TTL.
    if (view.expire !== null) base.setExpireRaw(key, view.expire);
  }
}

// ── helpers ─────────────────────────────────────────────────────────────────

function newEntry(type: RedisType | null, reset: boolean, expire: Exp | undefined, baseExpire: Exp): Entry {
  return {
    type,
    reset,
    expire,
    baseExpire,
    str: null,
    hash: type === "hash" ? new Map() : null,
    set: type === "set" ? new Map() : null,
    zset: type === "zset" ? new Map() : null,
    list: type === "list" ? { baseLen: 0, popL: 0, popR: 0, left: [], right: [] } : null,
    bytes: 0,
  };
}

function listLen(e: Entry): number {
  const l = e.list!;
  return l.baseLen - l.popL - l.popR + l.left.length + l.right.length;
}

function hashSets(e: Entry): Array<[Uint8Array, Uint8Array]> {
  const out: Array<[Uint8Array, Uint8Array]> = [];
  for (const [f, d] of e.hash!) if (d.v !== null) out.push([unmk(f), d.v]);
  return out;
}

function setAdds(e: Entry): Uint8Array[] {
  const out: Uint8Array[] = [];
  for (const [m, d] of e.set!) if (d.present && !d.baseHad) out.push(unmk(m));
  return out;
}

function zsetUpserts(e: Entry): Array<[number, Uint8Array]> {
  const out: Array<[number, Uint8Array]> = [];
  for (const [m, d] of e.zset!) if (d.score !== null) out.push([d.score, unmk(m)]);
  return out;
}

/** SRANDMEMBER semantics over an in-memory member list. */
function pickRandom(members: Uint8Array[], count: number | null): Uint8Array[] | Uint8Array | null {
  if (count === null) {
    return members.length === 0 ? null : members[Math.floor(Math.random() * members.length)]!;
  }
  if (count >= 0) {
    const a = members.slice();
    const k = Math.min(count, a.length);
    for (let i = 0; i < k; i++) {
      const j = i + Math.floor(Math.random() * (a.length - i));
      [a[i], a[j]] = [a[j]!, a[i]!];
    }
    return a.slice(0, k);
  }
  const out: Uint8Array[] = [];
  if (members.length === 0) return out;
  for (let i = 0; i < -count; i++) out.push(members[Math.floor(Math.random() * members.length)]!);
  return out;
}

/** Binary-safe Map key for raw bytes. */
function mk(b: Uint8Array): string {
  return Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString("latin1");
}

function unmk(s: string): Uint8Array {
  return new Uint8Array(Buffer.from(s, "latin1"));
}
