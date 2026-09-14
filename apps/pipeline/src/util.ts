export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Capped exponential backoff with full jitter: base*2^attempt, capped at max. */
export function backoffMs(attempt: number, baseMs: number, maxMs: number): number {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.min(attempt, 20));
  return Math.floor(exp / 2 + Math.random() * (exp / 2));
}

/** Sliding-window rate: records per second over the last `windowMs`. */
export class RateTracker {
  private samples: Array<{ t: number; n: number }> = [];
  constructor(private readonly windowMs = 10_000) {}
  add(n: number, now = Date.now()) {
    this.samples.push({ t: now, n });
    this.prune(now);
  }
  perSecond(now = Date.now()): number {
    this.prune(now);
    if (this.samples.length === 0) return 0;
    const total = this.samples.reduce((a, s) => a + s.n, 0);
    const span = Math.max(1000, now - this.samples[0].t);
    return Math.round((total / span) * 1000);
  }
  private prune(now: number) {
    const cutoff = now - this.windowMs;
    while (this.samples.length && this.samples[0].t < cutoff) this.samples.shift();
  }
}
