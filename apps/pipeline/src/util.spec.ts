import { backoffMs, RateTracker } from './util';

describe('backoffMs', () => {
  it('grows exponentially and is capped', () => {
    const seen = [0, 1, 2, 3, 4, 5, 6, 7, 10].map((a) => backoffMs(a, 500, 30000));
    // each value lies within [exp/2, exp] where exp = min(30000, 500*2^a)
    const caps = [500, 1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000];
    seen.forEach((v, i) => {
      expect(v).toBeLessThanOrEqual(caps[i]);
      expect(v).toBeGreaterThanOrEqual(caps[i] / 2);
    });
  });
  it('never exceeds the cap even for huge attempts', () => {
    expect(backoffMs(1000, 500, 30000)).toBeLessThanOrEqual(30000);
  });
});

describe('RateTracker', () => {
  it('computes records per second over the window', () => {
    const r = new RateTracker(10_000);
    const t0 = 1_000_000;
    r.add(500, t0);
    r.add(500, t0 + 1000);
    r.add(500, t0 + 2000);
    expect(r.perSecond(t0 + 2000)).toBe(750); // 1500 over 2s
  });
  it('drops samples outside the window', () => {
    const r = new RateTracker(10_000);
    r.add(1000, 0);
    r.add(100, 20_000);
    expect(r.perSecond(20_000)).toBe(100);
  });
});
