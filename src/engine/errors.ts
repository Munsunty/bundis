/**
 * Domain + RESP error model.
 *
 * Command handlers throw {@link RespError}; the dispatcher catches it and emits a
 * RESP error reply (`-CODE msg`). The storage layer must stay free of Redis
 * concepts (§3.2), so it throws the generic {@link TypeMismatchError}, which the
 * command layer translates into a `WRONGTYPE` RespError at the boundary.
 */

import type { RedisType } from "../storage/types";

/** An error that maps directly to a RESP error reply. `code` is the prefix. */
export class RespError extends Error {
  constructor(
    readonly code: string,
    override readonly message: string,
  ) {
    super(message);
    this.name = "RespError";
  }
}

/**
 * Thrown by the storage layer when an operation targets a key whose stored type
 * differs from what the operation expects. Carries no RESP vocabulary.
 */
export class TypeMismatchError extends Error {
  constructor(
    readonly actual: RedisType,
    readonly expected: RedisType,
  ) {
    super(`type mismatch: have ${actual}, want ${expected}`);
    this.name = "TypeMismatchError";
  }
}

/** Stored value is not a base-10 integer (INCR/DECR/HINCRBY on bad data). */
export class NotIntegerError extends Error {
  constructor() {
    super("value is not an integer or out of range");
    this.name = "NotIntegerError";
  }
}

/** Stored value is not a valid float (INCRBYFLOAT/HINCRBYFLOAT on bad data). */
export class NotFloatError extends Error {
  constructor() {
    super("value is not a valid float");
    this.name = "NotFloatError";
  }
}

const WRONGTYPE_MSG =
  "Operation against a key holding the wrong kind of value";

export const Errors = {
  wrongType: () => new RespError("WRONGTYPE", WRONGTYPE_MSG),
  wrongPass: () =>
    new RespError("WRONGPASS", "invalid username-password pair or user is disabled."),
  noAuth: () => new RespError("NOAUTH", "Authentication required."),
  syntax: () => new RespError("ERR", "syntax error"),
  notInt: () => new RespError("ERR", "value is not an integer or out of range"),
  notFloat: () => new RespError("ERR", "value is not a valid float"),
  wrongArgs: (cmd: string) =>
    new RespError("ERR", `wrong number of arguments for '${cmd.toLowerCase()}' command`),
  unknownCmd: (cmd: string, args: string[]) =>
    new RespError(
      "ERR",
      `unknown command '${cmd}', with args beginning with: ${args
        .slice(0, 1)
        .map((a) => `'${a}'`)
        .join(", ")}`,
    ),
  unsupportedInSubscribe: (cmd: string) =>
    new RespError(
      "ERR",
      `Can't execute '${cmd.toLowerCase()}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`,
    ),
  execWithoutMulti: () => new RespError("ERR", "EXEC without MULTI"),
  discardWithoutMulti: () => new RespError("ERR", "DISCARD without MULTI"),
  execAbort: () =>
    new RespError("EXECABORT", "Transaction discarded because of previous errors."),
} as const;

/** Translate a thrown error into a RESP error, mapping storage errors. */
export function toRespError(err: unknown): RespError {
  if (err instanceof RespError) return err;
  if (err instanceof TypeMismatchError) return Errors.wrongType();
  if (err instanceof NotIntegerError) return Errors.notInt();
  if (err instanceof NotFloatError) return Errors.notFloat();
  const msg = err instanceof Error ? err.message : String(err);
  return new RespError("ERR", msg);
}
