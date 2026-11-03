/**
 * bundis inside a bundled application (`bun build`, minified).
 *
 * Applications ship bundis's TS sources through their bundler, so nothing of
 * bundis exists on disk next to the bundle. Worker maintenance must still
 * run (its worker starts from a Blob URL, not a file path), and spawnServer
 * must fail with an actionable error unless given `cliPath`.
 */

import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const work = mkdtempSync(join(tmpdir(), "bundis-bundle-"));
afterAll(() => rmSync(work, { recursive: true, force: true }));

const APP = `
import { RedisClient } from "bun";
import { embedServer, spawnServer } from ${JSON.stringify(Bun.fileURLToPath(new URL("../../src/index.ts", import.meta.url)))};
const dir = process.argv[2];
const out = {};
const srv = embedServer({ port: 0, dbPath: dir + "/app.db" }); // default: auto → worker
const c = new RedisClient(srv.url);
await c.set("k", "v");
await c.send("MAINT", ["RUN"]);
let st = {};
for (let i = 0; i < 500; i++) {
  st = await c.send("MAINT", ["STATUS"]);
  if (st.maint_state === "normal" && Number(st.maint_cycles) >= 1) break;
  await Bun.sleep(10);
}
out.embed = { worker: st.maint_worker, state: st.maint_state, cycles: st.maint_cycles, error: st.maint_last_error, get: await c.get("k") };
c.close();
srv.stop();
try { await spawnServer({ port: 0, dbPath: dir + "/s.db" }); out.spawn = "started"; }
catch (err) { out.spawn = String(err); }
console.log(JSON.stringify(out));
process.exit(0);
`;

describe("bundled application", () => {
  test("worker maintenance runs from a minified bundle with no bundis files on disk", async () => {
    const src = join(work, "app.ts");
    writeFileSync(src, APP);
    const built = await Bun.build({ entrypoints: [src], target: "bun", minify: true, outdir: join(work, "dist") });
    expect(built.success).toBe(true);
    rmSync(src); // only the bundle remains
    const proc = Bun.spawn([process.execPath, join(work, "dist", "app.js"), work], {
      cwd: work,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
    await proc.exited;
    const line = stdout.trim().split("\n").pop() ?? "";
    let out: { embed: Record<string, unknown>; spawn: string };
    try {
      out = JSON.parse(line);
    } catch {
      throw new Error(`bundle produced no result.\nstdout: ${stdout}\nstderr: ${stderr}`);
    }
    expect(out.embed).toEqual({ worker: "ready", state: "normal", cycles: 1, error: "", get: "v" });
    expect(stderr).not.toContain("ModuleNotFound");
    expect(out.spawn).toContain("bundis CLI not found");
    expect(out.spawn).toContain("cliPath");
  }, 30_000);
});
