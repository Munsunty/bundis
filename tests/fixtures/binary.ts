/** Binary payloads for binary-safety tests (getBuffer round-trips). */

/** All 256 byte values, 0..255. */
export const ALL_BYTES = new Uint8Array(256).map((_, i) => i);

/** Embedded CRLF and NUL — the bytes most likely to break a naive parser. */
export const TRICKY = new Uint8Array([
  0x00, 0x0d, 0x0a, 0x24, 0x2a, 0x00, 0xff, 0x0d, 0x0a, 0x0d, 0x0a,
]);

/** A value larger than a typical TCP segment to exercise chunk splitting. */
export const LARGE = (() => {
  const buf = new Uint8Array(64 * 1024);
  for (let i = 0; i < buf.length; i++) buf[i] = i % 256;
  return buf;
})();

export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}
