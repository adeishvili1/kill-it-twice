import { Injectable, OnModuleInit } from '@nestjs/common';
import { Client, errors } from '@elastic/elasticsearch';
import { config } from '../../config';
import { logger } from '../../logger';
import { Product } from '../../source/product';
import { SinkUnavailableError } from '../sink-errors';
import { Classified, classifyBulkItems, flattenBulkResponse } from './bulk-classifier';
import { productsIndexBody } from './index-mapping';

export interface EsWriteResult { classified: Classified; attempts: number }

@Injectable()
export class EsSink implements OnModuleInit {
  readonly client = new Client({
    node: config.elasticsearchUrl,
    maxRetries: 0,                        // retrying is owned by SinkGate (SPEC §6.3)
    requestTimeout: config.esRequestTimeoutMs,
  });
  readonly index = config.esIndex;
  private indexReady = false;

  async onModuleInit() {
    try { await this.ensureIndex(); }
    catch (e: any) { logger.warn({ event: 'es_index_init_deferred', error: e.message }); }
  }

  async ensureIndex() {
    const exists = await this.client.indices.exists({ index: this.index });
    if (!exists) {
      await this.client.indices.create({ index: this.index, ...(productsIndexBody as any) });
      logger.info({ event: 'es_index_created', index: this.index });
    }
    this.indexReady = true;
  }

  async recreateIndex() {
    await this.client.indices.delete({ index: this.index, ignore_unavailable: true });
    await this.client.indices.create({ index: this.index, ...(productsIndexBody as any) });
    logger.info({ event: 'es_index_recreated', index: this.index });
  }

  /**
   * One bulk request for the batch. Whole-request failures (connection refused, timeout,
   * 5xx on the request) become SinkUnavailableError. Per-item outcomes are classified;
   * items that ES asked us to retry (429/5xx) are retried inside this call with the
   * SinkGate's backoff so that the caller only ever sees ok / duplicate / dlq.
   */
  async writeBatch(products: Product[]): Promise<Classified> {
    const operations = products.flatMap((p) => [
      { index: { _index: this.index, _id: String(p.id), version: p.version, version_type: 'external' } },
      p,
    ]);
    let res;
    try {
      if (!this.indexReady) await this.ensureIndex();   // never let ES auto-create the index with a loose mapping
      res = await this.client.bulk({ operations, refresh: false });
    } catch (err) {
      throw this.asUnavailable(err);
    }
    return classifyBulkItems(flattenBulkResponse(res.items as any[]));
  }

  async count(): Promise<number> {
    const r = await this.client.count({ index: this.index });
    return r.count;
  }

  async refresh() { await this.client.indices.refresh({ index: this.index }); }

  async ping(): Promise<boolean> {
    try { await this.client.cluster.health({ timeout: '2s' }); return true; } catch { return false; }
  }

  private asUnavailable(err: unknown): Error {
    if (err instanceof errors.ConnectionError || err instanceof errors.TimeoutError || err instanceof errors.NoLivingConnectionsError) {
      return new SinkUnavailableError('es', err.message, err);
    }
    if (err instanceof errors.ResponseError) {
      const status = err.meta.statusCode ?? 0;
      if (status === 429 || status >= 500 || status === 0) return new SinkUnavailableError('es', `${status} ${err.message}`, err);
    }
    return err as Error;
  }
}
