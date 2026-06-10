import { describe, expect, test } from "bun:test";
import { ProtocolError, RespParser } from "../../../src/resp/parser";

const enc = new TextEncoder();

describe("parser input caps", () => {
  test("rejects a bulk length over 512MB without buffering the body", () => {
    const p = new RespParser();
    p.push(enc.encode(`*2\r\n$3\r\nSET\r\n$${600 * 1024 * 1024}\r\n`));
    expect(() => p.drain()).toThrow(ProtocolError);
  });

  test("rejects an absurd multibulk count", () => {
    const p = new RespParser();
    p.push(enc.encode(`*${10_000_000}\r\n`));
    expect(() => p.drain()).toThrow(ProtocolError);
  });

  test("rejects an unterminated inline line over 64KB", () => {
    const p = new RespParser();
    p.push(new Uint8Array(70 * 1024).fill(65)); // 'A' x 70KB, no newline
    expect(() => p.drain()).toThrow(ProtocolError);
  });

  test("leftover partial commands never alias the pushed chunk", () => {
    const p = new RespParser();
    const chunk = enc.encode("*2\r\n$3\r\nSET\r\n$3\r\nab"); // incomplete
    p.push(chunk);
    expect(p.drain()).toEqual([]);
    chunk.fill(0); // runtime reuses the read buffer — parser must not care
    p.push(enc.encode("c\r\n"));
    const cmds = p.drain();
    expect(cmds.length).toBe(1);
    expect(new TextDecoder().decode(cmds[0]!.args[1]!)).toBe("abc");
  });
});
