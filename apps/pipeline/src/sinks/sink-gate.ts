import { Injectable } from '@nestjs/common';
import { MetricsService } from '../metrics/metrics.service';
import { SinkUnavailableError } from './sink-errors';
import { backoffMs, sleep } from '../util';
import { config } from '../config';
import { logger } from '../logger';

export type SinkName = 'es' | 'rabbitmq';

/**
 * The one place that decides how to wait for a sink that is down (G3). No circuit-breaker
 * library: a capped exponential backoff per attempt, `sink_up{sink}` flipped to 0 while
 * waiting, `pipeline_state` = degraded while any sink is down. Every loop parks here, so the
 * process is asleep between attempts — no busy loop.
 */
@Injectable()
export class SinkGate {
  private down = new Set<SinkName>();
  constructor(private readonly metrics: MetricsService) {}

  isUp(sink: SinkName) { return !this.down.has(sink); }
  anyDown() { return this.down.size > 0; }

  async retryUntilOk<T>(sink: SinkName, op: () => Promise<T>, signal?: { stopped?: boolean }): Promise<T> {
    let attempt = 0;
    for (;;) {
      try {
        const out = await op();
        this.markUp(sink);
        return out;
      } catch (err) {
        if (!(err instanceof SinkUnavailableError)) throw err;
        this.markDown(sink, err);
        this.metrics.sinkRetry.inc({ sink });
        const wait = backoffMs(attempt++, config.retryBaseMs, config.retryMaxMs);
        logger.warn({ event: 'sink_retry', sink, attempt, wait_ms: wait, error: err.message });
        await sleep(wait);
        if (signal?.stopped) throw err;
      }
    }
  }

  private markDown(sink: SinkName, err: Error) {
    if (!this.down.has(sink)) {
      this.down.add(sink);
      this.metrics.sinkUp.set({ sink }, 0);
      this.metrics.pipelineState.set(1);
      logger.error({ event: 'sink_down', sink, error: err.message });
    }
  }
  private markUp(sink: SinkName) {
    if (this.down.has(sink)) {
      this.down.delete(sink);
      this.metrics.sinkUp.set({ sink }, 1);
      if (this.down.size === 0) this.metrics.pipelineState.set(0);
      logger.info({ event: 'sink_up', sink });
    }
  }
}
