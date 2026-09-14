import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { EsSink } from '../sinks/es/es.sink';
import { RabbitSink } from '../sinks/rabbit/rabbit.sink';
import { SinkGate } from '../sinks/sink-gate';
import { DlqService } from '../dlq/dlq.service';
import { MetricsService } from '../metrics/metrics.service';
import { Op, Product, toEvent } from '../source/product';
import { backoffMs, sleep } from '../util';
import { config } from '../config';
import { logger } from '../logger';

export type Mode = 'backfill' | 'incremental' | 'replay';
export interface WriteOutcome { batchId: string; written: number; duplicates: number; dlq: number; esAttempts: number }

/**
 * Writes one batch to BOTH sinks and returns only when both have acknowledged it.
 * Callers commit their checkpoint after this returns — never before (SPEC §5).
 *
 * - Sinks are written concurrently; a sink that is unavailable is retried on its own with
 *   capped backoff (SinkGate) while the other sink's result is kept, so a replay on crash is
 *   the only source of duplicate writes.
 * - Per-item ES rejections go to the DLQ; the batch is never rolled back (SPEC §6.4).
 * - `crashAfterAck` is the G2 chaos hook: exit after both acks, before the checkpoint.
 */
@Injectable()
export class BatchWriter {
  crashAfterAck = false;

  constructor(
    private readonly es: EsSink,
    private readonly rabbit: RabbitSink,
    private readonly gate: SinkGate,
    private readonly dlq: DlqService,
    private readonly metrics: MetricsService,
  ) {}

  async write(products: Product[], ops: Map<number, Op>, mode: Mode): Promise<WriteOutcome> {
    const batchId = randomUUID();
    const events = products.map((p) => toEvent(p, ops.get(p.id) ?? 'update'));

    const [esRes] = await Promise.all([
      this.writeEs(products, batchId, mode),
      this.gate.retryUntilOk('rabbitmq', () => this.rabbit.publishBatch(events)).then(() => {
        this.metrics.recordsWritten.inc({ sink: 'rabbitmq', mode }, events.length);
      }),
    ]);

    if (this.crashAfterAck) {
      // Both sinks acknowledged, checkpoint NOT written. This is the window G1/G2 must survive.
      logger.error({ event: 'crash_requested', batch_id: batchId, mode, size: products.length, first_id: products[0]?.id, last_id: products.at(-1)?.id });
      await sleep(50); // let the log line flush
      process.exit(1);
    }
    return { batchId, ...esRes };
  }

  private async writeEs(products: Product[], batchId: string, mode: Mode): Promise<Omit<WriteOutcome, 'batchId'>> {
    let pending = products;
    let written = 0, duplicates = 0, dlqCount = 0, esAttempts = 0, itemRetry = 0;
    const byId = new Map(products.map((p) => [p.id, p]));
    for (;;) {
      esAttempts++;
      const c = await this.gate.retryUntilOk('es', () => this.es.writeBatch(pending));
      written += c.ok.length;
      duplicates += c.duplicates.length;
      await this.dlq.autoResolve('es', [...c.ok, ...c.duplicates]);
      if (c.dlq.length) {
        await this.dlq.add('es', c.dlq.map((d) => ({ product: byId.get(d.id)!, error: d.error })), batchId, mode);
        dlqCount += c.dlq.length;
      }
      if (c.retry.length === 0) break;
      // ES accepted the request but rejected some items with 429/5xx: retry just those.
      this.metrics.sinkRetry.inc({ sink: 'es' });
      const wait = backoffMs(itemRetry++, config.retryBaseMs, config.retryMaxMs);
      logger.warn({ event: 'es_item_retry', batch_id: batchId, count: c.retry.length, wait_ms: wait });
      await sleep(wait);
      pending = c.retry.map((id) => byId.get(id)!);
    }
    this.metrics.recordsWritten.inc({ sink: 'es', mode }, written);
    if (duplicates) this.metrics.duplicatesAbsorbed.inc({ sink: 'es' }, duplicates);
    return { written, duplicates, dlq: dlqCount, esAttempts };
  }
}
