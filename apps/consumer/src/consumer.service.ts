import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import amqp, { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager';
import { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { config } from './config';
import { logger } from './logger';
import { DbService } from './db.service';
import { MetricsService } from './metrics.service';
import { buildInsert, dedupeBuffer, ParsedEvent, parseEvent, shouldFlush } from './batcher';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const backoffMs = (attempt: number) => { const exp = Math.min(config.retryMaxMs, config.retryBaseMs * 2 ** Math.min(attempt, 20)); return Math.floor(exp / 2 + Math.random() * (exp / 2)); };

/**
 * The independent downstream (SPEC §4.2, D8). Stores every event exactly once by primary key
 * `event_id` — replays from the pipeline are counted as duplicates_absorbed and acked.
 * Deliveries are buffered and written with one multi-row INSERT, then acked up to the last
 * message. A database failure nacks the buffer (requeue) after a capped backoff; a malformed
 * message goes to consumer_dlq and is acked so it cannot poison the queue.
 */
@Injectable()
export class ConsumerService implements OnModuleInit, OnModuleDestroy {
  private conn: AmqpConnectionManager;
  private channel: ChannelWrapper;
  private buffer: ConsumeMessage[] = [];
  private firstBufferedAt: number | null = null;
  private timer: NodeJS.Timeout | null = null;
  private flushing: Promise<void> = Promise.resolve();
  private dbFailures = 0;
  connected = false;

  constructor(private readonly db: DbService, private readonly metrics: MetricsService) {}

  async onModuleInit() {
    this.conn = amqp.connect([config.rabbitmqUrl], { heartbeatIntervalInSeconds: 10, reconnectTimeInSeconds: 2 });
    this.conn.on('connect', () => { this.connected = true; this.metrics.up.set(1); logger.info({ event: 'rabbit_connected' }); });
    this.conn.on('disconnect', ({ err }) => { this.connected = false; this.metrics.up.set(0); this.buffer = []; this.firstBufferedAt = null; logger.warn({ event: 'rabbit_disconnected', error: err?.message }); });
    this.conn.on('connectFailed', ({ err }) => logger.warn({ event: 'rabbit_connect_failed', error: err?.message }));
    this.channel = this.conn.createChannel({
      setup: async (ch: ConfirmChannel) => {
        await ch.assertExchange(config.exchange, 'topic', { durable: true });
        await ch.assertQueue(config.queue, { durable: true });
        await ch.bindQueue(config.queue, config.exchange, '#');
        await ch.prefetch(config.prefetch);
        await ch.consume(config.queue, (msg) => msg && this.onMessage(msg), { noAck: false });
      },
    });
    this.channel.on('error', (e) => logger.warn({ event: 'rabbit_channel_error', error: e.message }));
    setInterval(() => this.refreshDlqGauge().catch(() => undefined), 5000).unref();
    logger.info({ event: 'consumer_started', queue: config.queue, prefetch: config.prefetch });
  }

  private onMessage(msg: ConsumeMessage) {
    this.metrics.received.inc();
    this.buffer.push(msg);
    if (this.firstBufferedAt === null) this.firstBufferedAt = Date.now();
    if (shouldFlush(this.buffer.length, config.prefetch, this.firstBufferedAt, Date.now(), config.flushAfterMs)) this.scheduleFlush(0);
    else if (!this.timer) this.scheduleFlush(config.flushAfterMs);
  }

  private scheduleFlush(inMs: number) {
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => { this.timer = null; this.flushing = this.flushing.then(() => this.flush()); }, inMs);
  }

  private async flush() {
    const msgs = this.buffer; this.buffer = []; this.firstBufferedAt = null;
    if (msgs.length === 0) return;
    const last = msgs[msgs.length - 1];
    const events: ParsedEvent[] = []; const bad: Array<{ id: string; body: string; error: string }> = [];
    for (const m of msgs) {
      const body = m.content.toString('utf8');
      const r = parseEvent(body, m.properties.messageId);
      if (r.event) events.push(r.event); else bad.push({ id: m.properties.messageId ?? '?', body, error: r.error! });
    }
    const { unique, duplicates: inBuffer } = dedupeBuffer(events);
    try {
      let stored = 0;
      if (unique.length) {
        const { text, values } = buildInsert(unique);
        stored = (await this.db.query(text, values)).length;
      }
      for (const b of bad) {
        await this.db.query('insert into consumer_dlq(event_id, payload, error) values ($1, $2::jsonb, $3)', [b.id, JSON.stringify({ raw: b.body }), b.error]);
        logger.warn({ event: 'consumer_dlq_item', event_id: b.id, error: b.error });
      }
      const duplicates = unique.length - stored + inBuffer;
      this.metrics.stored.inc(stored); this.metrics.duplicates.inc(duplicates); this.metrics.batches.inc();
      this.dbFailures = 0;
      this.channel.ack(last, true);
      logger.debug({ event: 'batch_stored', size: msgs.length, stored, duplicates, bad: bad.length });
    } catch (err: any) {
      // database problem: keep the messages, back off, hand them back to the broker
      this.metrics.dbRetries.inc();
      const wait = backoffMs(this.dbFailures++);
      logger.warn({ event: 'consumer_db_retry', size: msgs.length, wait_ms: wait, error: err?.message });
      await sleep(wait);
      try { this.channel.nack(last, true, true); } catch (e: any) { logger.warn({ event: 'consumer_nack_failed', error: e?.message }); }
    }
  }

  async refreshDlqGauge() {
    const r = await this.db.query<{ c: number }>('select count(*) as c from consumer_dlq');
    this.metrics.dlqSize.set(Number(r[0]?.c ?? 0));
  }

  async onModuleDestroy() { await this.conn?.close(); }
}
