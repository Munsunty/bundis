/**
 * MemoryGuard — process-global ceiling on bytes buffered across ALL connections
 * (inbound parser buffers + outbound backpressure queues).
 *
 * Per-connection caps alone don't bound aggregate memory: maxClients × the
 * per-connection limit can dwarf the configured budget. This single counter is
 * the backstop — once the aggregate crosses the limit, the connection that
 * pushed it over is closed by its caller.
 */
export class MemoryGuard {
  #used = 0;

  constructor(private readonly limit: number) {}

  add(n: number): void {
    this.#used += n;
  }

  sub(n: number): void {
    this.#used -= n;
    if (this.#used < 0) this.#used = 0;
  }

  get used(): number {
    return this.#used;
  }

  get overLimit(): boolean {
    return this.#used > this.limit;
  }
}
