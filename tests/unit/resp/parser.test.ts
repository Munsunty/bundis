import { describe, expect, test } from "bun:test";
import { RespParser, ProtocolError } from "../../../src/resp/parser";

const enc = (s: string) => new TextEncoder().encode(s);
const name = (cmd: { name: string }) => cmd.name;
const args = (cmd: { args: Uint8Array[] }) =>
  cmd.args.map((a) => new TextDecoder().decode(a));

describe("RespParser", () => {
  test("parses a single multibulk command", () => {
    const p = new RespParser();
    p.push(enc("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"));
    const cmds = p.drain();
    expect(cmds).toHaveLength(1);
    expect(name(cmds[0]!)).toBe("SET");
    expect(args(cmds[0]!)).toEqual(["SET", "k", "v"]);
  });

  test("upper-cases the command name but preserves arg bytes", () => {
    const p = new RespParser();
    p.push(enc("*2\r\n$3\r\nget\r\n$5\r\nMixed\r\n"));
    const cmd = p.drain()[0]!;
    expect(cmd.name).toBe("GET");
    expect(args(cmd)[1]).toBe("Mixed");
  });

  test("yields multiple pipelined commands in order", () => {
    const p = new RespParser();
    p.push(enc("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n"));
    const cmds = p.drain();
    expect(cmds.map(name)).toEqual(["PING", "GET"]);
  });

  test("holds incomplete input until the rest arrives", () => {
    const p = new RespParser();
    p.push(enc("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r")); // value byte + CRLF missing
    expect(p.drain()).toHaveLength(0);
    p.push(enc("\nv\r\n"));
    const cmds = p.drain();
    expect(cmds).toHaveLength(1);
    expect(args(cmds[0]!)).toEqual(["SET", "k", "v"]);
  });

  test("splits across the length header boundary", () => {
    const p = new RespParser();
    p.push(enc("*2\r\n$3\r\nGE")); // header split mid-token
    expect(p.drain()).toHaveLength(0);
    p.push(enc("T\r\n$1\r\nx\r\n"));
    expect(args(p.drain()[0]!)).toEqual(["GET", "x"]);
  });

  test("preserves binary argument bytes including CRLF and NUL", () => {
    const p = new RespParser();
    const payload = new Uint8Array([0x00, 0x0d, 0x0a, 0xff]);
    const header = enc(`*2\r\n$3\r\nSET\r\n$${payload.length}\r\n`);
    const tail = enc("\r\n");
    const buf = new Uint8Array(header.length + payload.length + tail.length);
    buf.set(header, 0);
    buf.set(payload, header.length);
    buf.set(tail, header.length + payload.length);
    p.push(buf);
    const cmd = p.drain()[0]!;
    expect([...cmd.args[1]!]).toEqual([0x00, 0x0d, 0x0a, 0xff]);
  });

  test("parses inline commands as a fallback", () => {
    const p = new RespParser();
    p.push(enc("PING\r\n"));
    expect(name(p.drain()[0]!)).toBe("PING");
  });

  test("throws ProtocolError on a malformed bulk header", () => {
    const p = new RespParser();
    p.push(enc("*1\r\n$x\r\n"));
    expect(() => p.drain()).toThrow(ProtocolError);
  });
});
