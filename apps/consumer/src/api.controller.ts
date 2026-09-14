import { Controller, Get, Header } from '@nestjs/common';
import { DbService } from './db.service';
import { MetricsService } from './metrics.service';
import { ConsumerService } from './consumer.service';

@Controller()
export class ApiController {
  constructor(private readonly db: DbService, private readonly metrics: MetricsService, private readonly consumer: ConsumerService) {}

  @Get('metrics') @Header('content-type', 'text/plain; version=0.0.4; charset=utf-8')
  metricsText() { return this.metrics.render(); }

  @Get('health')
  async health() {
    const db = await this.db.ping();
    const rabbitmq = this.consumer.connected;
    return { status: db && rabbitmq ? 'ok' : 'degraded', db, rabbitmq };
  }

  @Get('api/stats')
  async stats() {
    const [r] = await this.db.query<{ events: number; distinct_products: number }>('select count(*) as events, count(distinct product_id) as distinct_products from consumed_events');
    const [d] = await this.db.query<{ c: number }>('select count(*) as c from consumer_dlq');
    return {
      events: Number(r.events), distinct_products: Number(r.distinct_products), dlq: Number(d.c),
      received: await this.metrics.value('messages_received_total'),
      stored: await this.metrics.value('events_stored_total'),
      duplicates_absorbed: await this.metrics.value('duplicates_absorbed_total'),
      connected: this.consumer.connected,
    };
  }
}
