/**
 * CommandContext — everything a command handler needs, plus arg helpers.
 *
 * Handlers receive a single context: the connection, the storage engine, the
 * pub/sub hub, the server registry, and the parsed argument bytes. The helpers
 * centralize the (binary-safe) byte↔string/number conversions and arity checks
 * so each handler stays focused on semantics.
 */

import type { Connection } from "../connection";
import type { StorageEngine } from "../storage/types";
import type { PubSubHub } from "../sidecar/pubsub";
import type { WatchRegistry } from "../sidecar/watch";
import type { ServerConfig } from "../config";
import { Errors, NotIntegerError } from "./errors";

export interface ServerContext {
  readonly storage: StorageEngine;
  readonly hub: PubSubHub;
  readonly watch: WatchRegistry;
  readonly config: ServerConfig;
}

const decoder = new TextDecoder();

export class CommandContext {
  constructor(
    readonly conn: Connection,
    readonly server: ServerContext,
    /** All tokens including the command name (args[0]). */
    readonly args: Uint8Array[],
    /** Captured once per command so all sub-operations agree on "now". */
    readonly nowMs: number,
  ) {}

  get storage(): StorageEngine {
    return this.server.storage;
  }

  get cmd(): string {
    return decoder.decode(this.args[0]).toUpperCase();
  }

  /** Number of arguments excluding the command name. */
  get argc(): number {
    return this.args.length - 1;
  }

  /** Raw bytes of argument `i` (0-based, excluding command name). */
  arg(i: number): Uint8Array {
    const v = this.args[i + 1];
    if (v === undefined) throw Errors.wrongArgs(this.cmd);
    return v;
  }

  /** Optional raw bytes of argument `i`, or undefined if absent. */
  argOpt(i: number): Uint8Array | undefined {
    return this.args[i + 1];
  }

  /** UTF-8 string of argument `i`. */
  str(i: number): string {
    return decoder.decode(this.arg(i));
  }

  /** Upper-cased ASCII of argument `i` (for option keywords). */
  upper(i: number): string {
    return this.str(i).toUpperCase();
  }

  /** Base-10 bigint of argument `i`; throws ERR not-integer on bad input. */
  int(i: number): bigint {
    const s = this.str(i).trim();
    if (!/^[+-]?\d+$/.test(s)) throw new NotIntegerError();
    return BigInt(s);
  }

  /** Finite float of argument `i`; throws ERR not-float on bad input. */
  float(i: number): number {
    const s = this.str(i).trim();
    const n = Number(s);
    if (s.length === 0 || Number.isNaN(n)) throw Errors.notFloat();
    return n;
  }

  /** Require at least `n` arguments (excluding command name). */
  requireArgc(n: number): void {
    if (this.argc < n) throw Errors.wrongArgs(this.cmd);
  }

  /** Require exactly `n` arguments (excluding command name). */
  requireExactArgc(n: number): void {
    if (this.argc !== n) throw Errors.wrongArgs(this.cmd);
  }
}
