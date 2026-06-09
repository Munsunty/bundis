import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { startHarness, closeHarness, type Harness } from "../helpers/server-harness";
import { ALL_BYTES, TRICKY, LARGE, bytesEqual } from "../fixtures/binary";

/**
 * Binary safety: arbitrary bytes set via the stock client must round-trip
 * through getBuffer() unchanged — including NUL and the CRLF bytes that frame
 * the protocol, and payloads larger than a TCP segment.
 */

let h: Harness;
beforeEach(async () => {
  h = await startHarness();
});
afterEach(() => closeHarness(h));

describe("binary safety", () => {
  test("all 256 byte values survive set → getBuffer", async () => {
    await h.client.set("all", Buffer.from(ALL_BYTES));
    const got = await h.client.getBuffer("all");
    expect(got).toBeInstanceOf(Uint8Array);
    expect(bytesEqual(got!, ALL_BYTES)).toBe(true);
  });

  test("embedded CRLF and NUL are preserved", async () => {
    await h.client.set("tricky", Buffer.from(TRICKY));
    const got = await h.client.getBuffer("tricky");
    expect(bytesEqual(got!, TRICKY)).toBe(true);
  });

  test("a payload larger than one TCP segment round-trips", async () => {
    await h.client.set("large", Buffer.from(LARGE));
    const got = await h.client.getBuffer("large");
    expect(got!.length).toBe(LARGE.length);
    expect(bytesEqual(got!, LARGE)).toBe(true);
  });
});
