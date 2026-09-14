import { Injectable } from '@nestjs/common';
import { Counter, Gauge, Registry, collectDefaultMetrics } from 'prom-client';

@Injectable()
export class MetricsService {
  readonly registry = new Registry();
  readonly received = new Counter({ name: 'messages_received_total', help: 'deliveries received from the broker', registers: [this.registry] });
  readonly stored = new Counter({ name: 'events_stored_total', help: 'events inserted into consumed_events', registers: [this.registry] });
  readonly duplicates = new Counter({ name: 'duplicates_absorbed_total', help: 'deliveries whose event_id was already stored (replays)', registers: [this.registry] });
  readonly batches = new Counter({ name: 'consumer_batches_total', help: 'flushes', registers: [this.registry] });
  readonly dbRetries = new Counter({ name: 'consumer_db_retry_total', help: 'flushes that failed on the database and were requeued', registers: [this.registry] });
  readonly dlqSize = new Gauge({ name: 'consumer_dlq_size', help: 'rows in consumer_dlq', registers: [this.registry] });
  readonly up = new Gauge({ name: 'consumer_up', help: '1 when connected to the broker', registers: [this.registry] });
  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: 'consumer_' });
    this.received.inc(0); this.stored.inc(0); this.duplicates.inc(0); this.batches.inc(0); this.dbRetries.inc(0); this.dlqSize.set(0); this.up.set(0);
  }
  render() { return this.registry.metrics(); }
  async value(name: string): Promise<number> { const m = await this.registry.getSingleMetric(name)?.get(); return m?.values[0]?.value ?? 0; }
}
