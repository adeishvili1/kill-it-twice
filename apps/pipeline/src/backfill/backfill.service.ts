import { Injectable, OnModuleInit } from '@nestjs/common';
import { CheckpointService } from '../checkpoint/checkpoint.service';
import { SourceService } from '../source/source.service';
import { BatchWriter } from '../batch/batch-writer';
import { MetricsService } from '../metrics/metrics.service';
import { RateTracker, sleep } from '../util';
import { logger } from '../logger';
import { Op } from '../source/product';

/**
 * Backfill loop (SPEC §6.1). One keyset page at a time, both sinks, then checkpoint.
 * Resumes on boot from the committed checkpoint when the previous process died mid-run.
 */
@Injectable()
export class BackfillService implements OnModuleInit {
  readonly rate = new RateTracker(10_000);
  private busy = false;            // true while a batch is in flight
  private stopSignal = { stopped: false };
  lastBatchAt: string | null = null;
  startedAt: string | null = null;

  constructor(
    private readonly ckpt: CheckpointService,
    private readonly source: SourceService,
    private readonly writer: BatchWriter,
    private readonly metrics: MetricsService,
  ) {}

  onModuleInit() {
    const b = this.ckpt.backfill;
    if (b.status === 'running') {
      this.metrics.backfillResumeFromId.set(b.lastId);
      logger.info({ event: 'backfill_resumed', from_id: b.lastId, target_max_id: b.targetMaxId });
    }
    void this.loop();
  }

  async start() {
    if (this.ckpt.backfill.status === 'running') return this.ckpt.backfill;
    const targetMaxId = await this.source.maxId();
    await this.ckpt.setBackfill({ status: 'running', lastId: 0, targetMaxId });
    this.startedAt = new Date().toISOString();
    logger.info({ event: 'backfill_started', target_max_id: targetMaxId });
    return this.ckpt.backfill;
  }
  async pause() {
    if (this.ckpt.backfill.status === 'running') { await this.ckpt.setBackfill({ status: 'paused' }); logger.info({ event: 'backfill_paused', at_id: this.ckpt.backfill.lastId }); }
    return this.ckpt.backfill;
  }
  async resume() {
    if (this.ckpt.backfill.status === 'paused') { await this.ckpt.setBackfill({ status: 'running' }); logger.info({ event: 'backfill_resumed', from_id: this.ckpt.backfill.lastId, manual: true }); }
    return this.ckpt.backfill;
  }
  async reset() {
    await this.ckpt.setBackfill({ status: 'idle', lastId: 0, targetMaxId: 0 });
    logger.info({ event: 'backfill_reset' });
    return this.ckpt.backfill;
  }
  isBusy() { return this.busy; }

  private async loop() {
    for (;;) {
      try {
        if (this.ckpt.backfill.status !== 'running') {
          this.rate.perSecond() === 0 || this.metrics.recordsPerSecond.set({ mode: 'backfill' }, this.rate.perSecond());
          await sleep(250);
          continue;
        }
        const { lastId, targetMaxId } = this.ckpt.backfill;
        const batch = await this.source.readBackfillBatch(lastId, targetMaxId, this.ckpt.params.batchSize);
        if (batch.length === 0) {
          await this.ckpt.setBackfill({ status: 'done' });
          logger.info({ event: 'backfill_done', last_id: lastId, target_max_id: targetMaxId });
          continue;
        }
        this.busy = true;
        const ops = new Map<number, Op>(batch.map((p) => [p.id, 'insert' as Op]));
        const t0 = Date.now();
        const out = await this.writer.write(batch, ops, 'backfill');
        await this.ckpt.commitBackfill(batch[batch.length - 1].id);      // <-- the checkpoint
        this.busy = false;
        this.rate.add(batch.length);
        this.metrics.recordsPerSecond.set({ mode: 'backfill' }, this.rate.perSecond());
        this.lastBatchAt = new Date().toISOString();
        logger.debug({ event: 'batch_committed', mode: 'backfill', batch_id: out.batchId, size: batch.length, last_id: batch[batch.length - 1].id, ms: Date.now() - t0, dupes: out.duplicates, dlq: out.dlq });
      } catch (err: any) {
        this.busy = false;
        logger.error({ event: 'backfill_error', error: err?.message, stack: err?.stack });
        await sleep(1000);
      }
    }
  }
}
