import { Injectable } from '@nestjs/common';
import { Counter, Gauge, Registry, collectDefaultMetrics } from 'prom-client';

/**
 * All metric names are part of the contract with verify.sh and the README (SPEC §6.5).
 * Add freely, never rename.
 */
@Injectable()
export class MetricsService {
  readonly registry = new Registry();

  readonly backfillPosition = new Gauge({ name: 'backfill_position', help: 'last committed product id of the backfill', registers: [this.registry] });
  readonly backfillTarget = new Gauge({ name: 'backfill_target', help: 'max product id snapshot taken when the backfill started', registers: [this.registry] });
  readonly backfillStatus = new Gauge({ name: 'backfill_status', help: '0 idle, 1 running, 2 paused, 3 done', registers: [this.registry] });
  readonly backfillResumeFromId = new Gauge({ name: 'backfill_resume_from_id', help: 'position the backfill resumed from at process start (0 if it did not resume)', registers: [this.registry] });
  readonly recordsWritten = new Counter({ name: 'records_written_total', help: 'records acknowledged by a sink', labelNames: ['sink', 'mode'], registers: [this.registry] });
  readonly recordsPerSecond = new Gauge({ name: 'records_per_second', help: 'throughput over the last 10s', labelNames: ['mode'], registers: [this.registry] });
  readonly incrementalCursor = new Gauge({ name: 'incremental_cursor', help: 'last committed change_log.seq', registers: [this.registry] });
  readonly incrementalLagSeq = new Gauge({ name: 'incremental_lag_seq', help: 'change_log rows not yet processed (max(seq) - cursor)', registers: [this.registry] });
  readonly incrementalLagSeconds = new Gauge({ name: 'incremental_lag_seconds', help: 'age of the oldest unprocessed change', registers: [this.registry] });
  readonly incrementalStatus = new Gauge({ name: 'incremental_status', help: '1 running, 2 paused', registers: [this.registry] });
  readonly dlqSize = new Gauge({ name: 'dlq_size', help: 'open DLQ entries', labelNames: ['sink'], registers: [this.registry] });
  readonly sinkUp = new Gauge({ name: 'sink_up', help: '1 if the sink accepted the last write, 0 while waiting for it', labelNames: ['sink'], registers: [this.registry] });
  readonly sinkRetry = new Counter({ name: 'sink_retry_total', help: 'retries against a sink (whole batch)', labelNames: ['sink'], registers: [this.registry] });
  readonly duplicatesAbsorbed = new Counter({ name: 'duplicates_absorbed_total', help: 'writes the sink recognised as already applied (replays / stale)', labelNames: ['sink'], registers: [this.registry] });
  readonly dlqItems = new Counter({ name: 'dlq_items_total', help: 'records sent to the DLQ', labelNames: ['sink'], registers: [this.registry] });
  readonly pipelineState = new Gauge({ name: 'pipeline_state', help: '0 healthy, 1 degraded (a sink is down)', registers: [this.registry] });
  readonly batchesCommitted = new Counter({ name: 'batches_committed_total', help: 'checkpoints written', labelNames: ['mode'], registers: [this.registry] });

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: 'pipeline_' });
    this.sinkUp.set({ sink: 'es' }, 1);
    this.sinkUp.set({ sink: 'rabbitmq' }, 1);
    this.pipelineState.set(0);
    this.backfillResumeFromId.set(0);
    this.dlqSize.set({ sink: 'es' }, 0);
    this.dlqSize.set({ sink: 'rabbitmq' }, 0);
    this.recordsPerSecond.set({ mode: 'backfill' }, 0);
    this.recordsPerSecond.set({ mode: 'incremental' }, 0);
    for (const sink of ['es', 'rabbitmq']) {
      this.duplicatesAbsorbed.inc({ sink }, 0);
      this.sinkRetry.inc({ sink }, 0);
      this.dlqItems.inc({ sink }, 0);
      for (const mode of ['backfill', 'incremental', 'replay']) this.recordsWritten.inc({ sink, mode }, 0);
    }
  }

  async render() { return this.registry.metrics(); }
  async value(name: string, labels: Record<string, string> = {}): Promise<number> {
    const m = await this.registry.getSingleMetric(name)?.get();
    const hit = m?.values.find((v) => Object.entries(labels).every(([k, val]) => v.labels[k] === val));
    return hit?.value ?? 0;
  }
}
