import { Injectable, OnModuleInit } from '@nestjs/common';
import { CheckpointService } from '../checkpoint/checkpoint.service';
import { SourceService } from '../source/source.service';
import { BatchWriter } from '../batch/batch-writer';
import { MetricsService } from '../metrics/metrics.service';
import { RateTracker, sleep } from '../util';
import { logger } from '../logger';
import { Op } from '../source/product';

/**
 * Incremental loop (SPEC §6.2). Polls change_log after the cursor, minus the safety window,
 * joins the CURRENT product state (so several changes to one row collapse into one write of
 * the latest version), writes both sinks, commits cursor = max(seq) of the batch.
 */
@Injectable()
export class IncrementalService implements OnModuleInit {
  readonly rate = new RateTracker(10_000);
  lag = { lagSeq: 0, lagSeconds: 0, maxSeq: 0 };
  lastBatchAt: string | null = null;
  private busy = false;

  constructor(
    private readonly ckpt: CheckpointService,
    private readonly source: SourceService,
    private readonly writer: BatchWriter,
    private readonly metrics: MetricsService,
  ) {}

  onModuleInit() { void this.loop(); }

  async pause() { await this.ckpt.setIncremental({ status: 'paused' }); logger.info({ event: 'incremental_paused' }); return this.ckpt.incremental; }
  async resume() { await this.ckpt.setIncremental({ status: 'running' }); logger.info({ event: 'incremental_resumed' }); return this.ckpt.incremental; }
  isBusy() { return this.busy; }

  private async refreshLag() {
    this.lag = await this.source.lag(this.ckpt.incremental.cursorSeq);
    this.metrics.incrementalLagSeq.set(this.lag.lagSeq);
    this.metrics.incrementalLagSeconds.set(this.lag.lagSeconds);
  }

  private async loop() {
    for (;;) {
      const interval = this.ckpt.params.incrementalIntervalMs;
      try {
        await this.refreshLag();
        if (this.ckpt.incremental.status !== 'running') { await sleep(interval); continue; }
        const { cursorSeq } = this.ckpt.incremental;
        const changes = await this.source.readChanges(cursorSeq, this.ckpt.params.safetyWindowMs, this.ckpt.params.batchSize);
        if (changes.length === 0) {
          this.metrics.recordsPerSecond.set({ mode: 'incremental' }, this.rate.perSecond());
          await sleep(interval);
          continue;
        }
        // latest op per product wins; the product row itself is read fresh
        const ops = new Map<number, Op>();
        for (const c of changes) ops.set(c.product_id, c.op);
        const products = await this.source.readByIds([...ops.keys()]);
        this.busy = true;
        const t0 = Date.now();
        const out = products.length ? await this.writer.write(products, ops, 'incremental') : { batchId: '-', written: 0, duplicates: 0, dlq: 0, esAttempts: 0 };
        const maxSeq = changes[changes.length - 1].seq;
        await this.ckpt.commitIncremental(maxSeq);                        // <-- the checkpoint
        this.busy = false;
        this.rate.add(products.length);
        this.metrics.recordsPerSecond.set({ mode: 'incremental' }, this.rate.perSecond());
        this.lastBatchAt = new Date().toISOString();
        logger.debug({ event: 'batch_committed', mode: 'incremental', batch_id: out.batchId, changes: changes.length, products: products.length, cursor_seq: maxSeq, ms: Date.now() - t0, dupes: out.duplicates, dlq: out.dlq });
        // full page → there is probably more; do not wait the whole interval
        if (changes.length < this.ckpt.params.batchSize) await sleep(interval);
      } catch (err: any) {
        this.busy = false;
        logger.error({ event: 'incremental_error', error: err?.message, stack: err?.stack });
        await sleep(Math.max(1000, interval));
      }
    }
  }
}
