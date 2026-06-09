/**
 * L2 RESP3 reply model.
 *
 * A {@link Reply} is a tagged union describing exactly which RESP3 type the
 * server must emit. The serializer ({@link ./serializer.ts}) walks this tree and
 * produces bytes. Keeping the wire type explicit (rather than inferring it from a
 * JS value) is what lets us honor the §2.4 type-conversion contract precisely.
 */

export type Reply =
  | { readonly t: "simple"; readonly v: string } // +OK\r\n
  | { readonly t: "error"; readonly code: string; readonly msg: string } // -CODE msg\r\n
  | { readonly t: "int"; readonly v: number | bigint } // :1\r\n
  | { readonly t: "bulk"; readonly v: Uint8Array | string | null } // $len\r\n..\r\n  (null => RESP3 null)
  | { readonly t: "null" } // _\r\n
  | { readonly t: "array"; readonly v: readonly Reply[] | null } // *N\r\n..  (null => *-1)
  | { readonly t: "map"; readonly v: ReadonlyArray<readonly [Reply, Reply]> } // %N\r\n..
  | { readonly t: "set"; readonly v: readonly Reply[] } // ~N\r\n..
  | { readonly t: "bool"; readonly v: boolean } // #t\r\n / #f\r\n
  | { readonly t: "double"; readonly v: number } // ,3.14\r\n
  | { readonly t: "bignum"; readonly v: bigint } // (123\r\n
  | { readonly t: "verbatim"; readonly fmt: string; readonly v: string } // =len\r\ntxt:..\r\n
  | { readonly t: "push"; readonly v: readonly Reply[] }; // >N\r\n..

/** Reply constructors. Use these instead of building object literals by hand. */
export const R = {
  simple: (v: string): Reply => ({ t: "simple", v }),
  ok: (): Reply => ({ t: "simple", v: "OK" }),
  error: (code: string, msg: string): Reply => ({ t: "error", code, msg }),
  int: (v: number | bigint): Reply => ({ t: "int", v }),
  bulk: (v: Uint8Array | string | null): Reply => ({ t: "bulk", v }),
  nullReply: (): Reply => ({ t: "null" }),
  array: (v: readonly Reply[] | null): Reply => ({ t: "array", v }),
  map: (v: ReadonlyArray<readonly [Reply, Reply]>): Reply => ({ t: "map", v }),
  set: (v: readonly Reply[]): Reply => ({ t: "set", v }),
  bool: (v: boolean): Reply => ({ t: "bool", v }),
  double: (v: number): Reply => ({ t: "double", v }),
  bignum: (v: bigint): Reply => ({ t: "bignum", v }),
  verbatim: (fmt: string, v: string): Reply => ({ t: "verbatim", fmt, v }),
  push: (v: readonly Reply[]): Reply => ({ t: "push", v }),
} as const;

/** A fully parsed inbound command: name is upper-cased ASCII, args keep raw bytes. */
export interface Command {
  /** Upper-cased command name, e.g. "SET". */
  readonly name: string;
  /** All tokens including the command name, as raw bytes (binary-safe). */
  readonly args: Uint8Array[];
}
