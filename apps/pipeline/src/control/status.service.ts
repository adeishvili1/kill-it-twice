import { Injectable } from '@nestjs/common';
import { BackfillService } from '../backfill/backfill.service';
import { IncrementalService } from '../incremental/incremental.service';
import { CheckpointService } from '../checkpoint/checkpoint.service';
import { SourceService } from '../source/source.service';
import { EsSink } from '../sinks/es/es.sink';
import { RabbitSink } from '../sinks/rabbit/rabbit.sink';
import { SinkGate } from '../sinks/sink-gate';
import { DlqService } from '../dlq/dlq.service';
import { DbService } from '../db/db.service';
import { MetricsService } from '../metrics/metrics.service';

/** Aggregated view for the UI (G5's visual side). Counts are cached for 2 s. */
@Injectable()
export class StatusService {
  private cache: { at: number; counts: any } | null = null;

  constructor(
    private readonly backfill: BackfillService, private readonly incremental: IncrementalService,
    private readonly ckpt: CheckpointService, private readonly source: SourceService,
    private readonly es: EsSink, private readonly rabbit: RabbitSink, private readonly gate: SinkGate,
    private readonly dlq: DlqService, private readonly db: DbService, private readonly metrics: MetricsService,
  ) {}

  async health() {
    const [dbOk, esOk] = await Promise.all([this.db.ping(), this.es.ping()]);
    const rabbitOk = this.rabbit.isConnected();
    const sinks = { es: esOk && this.gate.isUp('es'), rabbitmq: rabbitOk && this.gate.isUp('rabbitmq') };
    const status = dbOk && sinks.es && sinks.rabbitmq ? 'ok' : 'degraded';
    return { status, db: dbOk, sinks, backfill: this.ckpt.backfill.status, incremental: this.ckpt.incremental.status, uptime_s: Math.round(process.uptime()) };
  }

  private async counts() {
    if (this.cache && Date.now() - this.cache.at < 2000) return this.cache.counts;
    const [source, esCount, consumer, dlq, queueDepth] = await Promise.all([
      this.source.count(),
      this.es.count().catch(() => null),
      this.db.one<{ c: string; d: string }>('select count(*) as c, count(distinct product_id) as d from consumed_events').then((r) => ({ events: Number(r.c), products: Number(r.d) })),
      this.dlq.refreshGauge(),
      this.rabbit.queueDepth(),
    ]);
    const counts = { source, es: esCount, consumer, dlq, queue_depth: queueDepth };
    this.cache = { at: Date.now(), counts };
    return counts;
  }

  async status() {
    const [health, counts] = await Promise.all([this.health(), this.counts()]);
    const b = this.ckpt.backfill;
    const pct = b.targetMaxId > 0 ? Math.min(100, Math.round((b.lastId / b.targetMaxId) * 1000) / 10) : 0;
    return {
      health,
      backfill: { ...b, pct, rate: this.backfill.rate.perSecond(), last_batch_at: this.backfill.lastBatchAt, resumed_from: await this.metrics.value('backfill_resume_from_id') },
      incremental: { ...this.ckpt.incremental, ...this.incremental.lag, rate: this.incremental.rate.perSecond(), last_batch_at: this.incremental.lastBatchAt },
      params: this.ckpt.params,
      counts,
      sinks: { es: { up: this.gate.isUp('es') }, rabbitmq: { up: this.gate.isUp('rabbitmq'), connected: this.rabbit.isConnected() } },
      totals: {
        written: { es: await this.metrics.value('records_written_total', { sink: 'es', mode: 'backfill' }) + await this.metrics.value('records_written_total', { sink: 'es', mode: 'incremental' }) + await this.metrics.value('records_written_total', { sink: 'es', mode: 'replay' }),
                   rabbitmq: await this.metrics.value('records_written_total', { sink: 'rabbitmq', mode: 'backfill' }) + await this.metrics.value('records_written_total', { sink: 'rabbitmq', mode: 'incremental' }) + await this.metrics.value('records_written_total', { sink: 'rabbitmq', mode: 'replay' }) },
        duplicates_absorbed: { es: await this.metrics.value('duplicates_absorbed_total', { sink: 'es' }) },
        retries: { es: await this.metrics.value('sink_retry_total', { sink: 'es' }), rabbitmq: await this.metrics.value('sink_retry_total', { sink: 'rabbitmq' }) },
      },
      now: new Date().toISOString(),
    };
  }
}
