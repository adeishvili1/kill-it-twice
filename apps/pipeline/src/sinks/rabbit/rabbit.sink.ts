import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import amqp, { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager';
import { ConfirmChannel } from 'amqplib';
import { config } from '../../config';
import { logger } from '../../logger';
import { ChangeEvent } from '../../source/product';
import { SinkUnavailableError } from '../sink-errors';

/**
 * Publisher with confirms. amqp-connection-manager reconnects with backoff on its own and
 * attaches error handlers (a bare amqplib `on('close') => connect()` would be a busy loop).
 * `publish` carries a timeout so a batch cannot hang forever while the broker is away: the
 * batch writer sees SinkUnavailableError and parks in the SinkGate instead.
 */
@Injectable()
export class RabbitSink implements OnModuleInit, OnModuleDestroy {
  private conn: AmqpConnectionManager;
  private channel: ChannelWrapper;
  private connected = false;

  async onModuleInit() {
    this.conn = amqp.connect([config.rabbitmqUrl], { heartbeatIntervalInSeconds: 10, reconnectTimeInSeconds: 2 });
    this.conn.on('connect', () => { this.connected = true; logger.info({ event: 'rabbit_connected' }); });
    this.conn.on('disconnect', ({ err }) => { this.connected = false; logger.warn({ event: 'rabbit_disconnected', error: err?.message }); });
    this.conn.on('connectFailed', ({ err }) => { this.connected = false; logger.warn({ event: 'rabbit_connect_failed', error: err?.message }); });
    this.channel = this.conn.createChannel({
      json: true,
      confirm: true,
      setup: async (ch: ConfirmChannel) => {
        await ch.assertExchange(config.rabbitExchange, 'topic', { durable: true });
        await ch.assertQueue(config.rabbitQueue, { durable: true });
        await ch.bindQueue(config.rabbitQueue, config.rabbitExchange, '#');
      },
    });
    this.channel.on('error', (e) => logger.warn({ event: 'rabbit_channel_error', error: e.message }));
  }

  async publishBatch(events: ChangeEvent[]): Promise<void> {
    if (!this.connected) throw new SinkUnavailableError('rabbitmq', 'not connected');
    try {
      await Promise.all(
        events.map((e) =>
          this.channel.publish(config.rabbitExchange, `product.${e.op}`, e, {
            persistent: true,
            messageId: e.event_id,
            contentType: 'application/json',
            timestamp: Math.floor(Date.now() / 1000),
            timeout: config.esRequestTimeoutMs,
          } as any),
        ),
      );
    } catch (err: any) {
      throw new SinkUnavailableError('rabbitmq', err?.message ?? String(err), err);
    }
  }

  isConnected() { return this.connected; }

  async purgeQueue() {
    await this.channel.waitForConnect();
    await (this.channel as any).purgeQueue(config.rabbitQueue);
  }

  async queueDepth(): Promise<number | null> {
    try {
      const r = await (this.channel as any).checkQueue(config.rabbitQueue);
      return r?.messageCount ?? null;
    } catch { return null; }
  }

  async onModuleDestroy() { await this.conn?.close(); }
}
