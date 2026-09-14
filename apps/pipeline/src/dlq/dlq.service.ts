import { Injectable } from '@nestjs/common';
import { DbService } from '../db/db.service';
import { MetricsService } from '../metrics/metrics.service';
import { Product } from '../source/product';
import { logger } from '../logger';

export interface DlqEntry {
  id: number; sink: string; record_id: number; version: number; payload: Product; error: string;
  batch_id: string; mode: string; attempts: number; status: string; created_at: string; updated_at: string;
}

@Injectable()
export class DlqService {
  constructor(private readonly db: DbService, private readonly metrics: MetricsService) {}

  /** Upsert keyed by (sink, record_id): a bad row touched again bumps attempts instead of flooding. */
  async add(sink: 'es' | 'rabbitmq', items: Array<{ product: Product; error: string }>, batchId: string, mode: string) {
    for (const it of items) {
      await this.db.query(
        `insert into dlq(sink, record_id, version, payload, error, batch_id, mode)
         values ($1,$2,$3,$4,$5,$6,$7)
         on conflict (sink, record_id) do update
           set version=excluded.version, payload=excluded.payload, error=excluded.error, batch_id=excluded.batch_id,
               mode=excluded.mode, attempts=dlq.attempts+1, status='open', updated_at=clock_timestamp()`,
        [sink, it.product.id, it.product.version, JSON.stringify(it.product), it.error, batchId, mode],
      );
      this.metrics.dlqItems.inc({ sink });
      logger.warn({ event: 'dlq_item', sink, record_id: it.product.id, version: it.product.version, batch_id: batchId, error: it.error });
    }
    await this.refreshGauge();
  }

  async resolve(sink: string, ids: number[]) {
    if (ids.length === 0) return;
    await this.db.query(`update dlq set status='resolved', updated_at=clock_timestamp() where sink=$1 and record_id = any($2::bigint[])`, [sink, ids]);
    await this.refreshGauge();
  }

  async bumpAttempts(sink: string, items: Array<{ id: number; error: string }>) {
    for (const it of items) {
      await this.db.query(`update dlq set attempts=attempts+1, error=$3, updated_at=clock_timestamp() where sink=$1 and record_id=$2`, [sink, it.id, it.error]);
    }
  }

  async list(status = 'open', limit = 100): Promise<DlqEntry[]> {
    return this.db.query<DlqEntry>(`select * from dlq where status=$1 order by updated_at desc limit $2`, [status, limit]);
  }
  async get(id: number): Promise<DlqEntry | undefined> {
    return this.db.one<DlqEntry>(`select * from dlq where id=$1`, [id]);
  }
  async openIds(sink: string): Promise<number[]> {
    return this.db.query<{ record_id: string }>(`select record_id from dlq where sink=$1 and status='open' order by record_id`, [sink]).then((r) => r.map((x) => Number(x.record_id)));
  }
  async counts(): Promise<Record<string, number>> {
    const rows = await this.db.query<{ sink: string; c: string }>(`select sink, count(*) as c from dlq where status='open' group by sink`);
    const out: Record<string, number> = { es: 0, rabbitmq: 0 };
    for (const r of rows) out[r.sink] = Number(r.c);
    return out;
  }
  async refreshGauge() {
    const c = await this.counts();
    for (const [sink, n] of Object.entries(c)) this.metrics.dlqSize.set({ sink }, n);
    return c;
  }
}
