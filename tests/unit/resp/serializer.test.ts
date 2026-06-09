import { describe, expect, test } from "bun:test";
import { serialize, serializeAll } from "../../../src/resp/serializer";
import { R } from "../../../src/resp/types";

const out = (r: Parameters<typeof serialize>[0]) =>
  new TextDecoder().decode(serialize(r));

describe("RESP3 serializer", () => {
  test("simple string", () => {
    expect(out(R.simple("OK"))).toBe("+OK\r\n");
  });

  test("error with code", () => {
    expect(out(R.error("WRONGTYPE", "nope"))).toBe("-WRONGTYPE nope\r\n");
  });

  test("integer", () => {
    expect(out(R.int(42))).toBe(":42\r\n");
    expect(out(R.int(-1))).toBe(":-1\r\n");
  });

  test("bulk string length is byte length, not char length", () => {
    // "é" is 2 bytes in UTF-8.
    expect(out(R.bulk("é"))).toBe("$2\r\né\r\n");
  });

  test("binary bulk preserves arbitrary bytes", () => {
    const bytes = new Uint8Array([0x00, 0x0d, 0x0a, 0xff]);
    const buf = serialize(R.bulk(bytes));
    // header "$4\r\n" then the 4 bytes then "\r\n"
    expect(buf[0]).toBe(0x24); // '$'
    expect([...buf.subarray(4, 8)]).toEqual([0x00, 0x0d, 0x0a, 0xff]);
    expect([...buf.subarray(8)]).toEqual([0x0d, 0x0a]);
  });

  test("null bulk and RESP3 null both serialize to _", () => {
    expect(out(R.bulk(null))).toBe("_\r\n");
    expect(out(R.nullReply())).toBe("_\r\n");
  });

  test("boolean", () => {
    expect(out(R.bool(true))).toBe("#t\r\n");
    expect(out(R.bool(false))).toBe("#f\r\n");
  });

  test("double", () => {
    expect(out(R.double(3.14))).toBe(",3.14\r\n");
    expect(out(R.double(Infinity))).toBe(",inf\r\n");
  });

  test("array", () => {
    expect(out(R.array([R.int(1), R.bulk("a")]))).toBe("*2\r\n:1\r\n$1\r\na\r\n");
    expect(out(R.array(null))).toBe("*-1\r\n");
  });

  test("map", () => {
    expect(out(R.map([[R.bulk("f"), R.bulk("v")]]))).toBe("%1\r\n$1\r\nf\r\n$1\r\nv\r\n");
  });

  test("set", () => {
    expect(out(R.set([R.bulk("a"), R.bulk("b")]))).toBe("~2\r\n$1\r\na\r\n$1\r\nb\r\n");
  });

  test("push", () => {
    expect(out(R.push([R.bulk("message"), R.bulk("ch"), R.bulk("hi")]))).toBe(
      ">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n",
    );
  });

  test("verbatim", () => {
    expect(out(R.verbatim("txt", "hi"))).toBe("=6\r\ntxt:hi\r\n");
  });

  test("serializeAll concatenates replies", () => {
    expect(new TextDecoder().decode(serializeAll([R.ok(), R.int(1)]))).toBe(
      "+OK\r\n:1\r\n",
    );
  });
});
