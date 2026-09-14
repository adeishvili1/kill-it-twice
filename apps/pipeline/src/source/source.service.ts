import { Injectable } from '@nestjs/common';
import { DbService } from '../db/db.service';
import { Op, PRODUCT_COLUMNS, Product } from './product';

export interface Change { seq: number; product_id: number; op: Op; version: number; changed_at: string }

@Injectable()
export class SourceService {
  constructor(private readonly db: DbService) {}

  /** Keyset page for the backfill: strictly after lastId, never beyond the snapshot target. */
  readBackfillBatch(lastId: number, targetMaxId: number, limit: number): Promise<Product[]> {
    return this.db.query<Product>(
      `select ${PRODUCT_COLUMNS} from products where id > $1 and id <= $2 order by id limit $3`,
      [lastId, targetMaxId, limit],
    );
  }

  maxId(): Promise<number> {
    return this.db.one<{ m: string | null }>('select max(id) as m from products').then((r) => Number(r.m ?? 0));
  }

  /**
   * Changes after the cursor, excluding the most recent `safetyWindowMs` so that a change_log
   * row whose transaction committed late (lower seq, later commit) is not skipped. SPEC §6.2.
   */
  readChanges(cursorSeq: number, safetyWindowMs: number, limit: number): Promise<Change[]> {
    return this.db.query<Change>(
      `select seq, product_id, op, version, changed_at from change_log
       where seq > $1 and changed_at <= clock_timestamp() - ($2::int * interval '1 millisecond')
       order by seq limit $3`,
      [cursorSeq, safetyWindowMs, limit],
    ).then((rows) => rows.map((r) => ({ ...r, seq: Number(r.seq), product_id: Number(r.product_id), version: Number(r.version) })));
  }

  /** Lag figures for metrics: rows behind the cursor and age of the oldest unprocessed change. */
  async lag(cursorSeq: number): Promise<{ lagSeq: number; lagSeconds: number; maxSeq: number }> {
    const r = await this.db.one<{ max_seq: string | null; oldest: string | null }>(
      `select (select max(seq) from change_log) as max_seq,
              (select extract(epoch from clock_timestamp() - min(changed_at)) from change_log where seq > $1) as oldest`,
      [cursorSeq],
    );
    const maxSeq = Number(r.max_seq ?? 0);
    return { maxSeq, lagSeq: Math.max(0, maxSeq - cursorSeq), lagSeconds: r.oldest ? Math.round(Number(r.oldest) * 10) / 10 : 0 };
  }

  readByIds(ids: number[]): Promise<Product[]> {
    if (ids.length === 0) return Promise.resolve([]);
    return this.db.query<Product>(`select ${PRODUCT_COLUMNS} from products where id = any($1::bigint[]) order by id`, [ids]);
  }

  count(): Promise<number> {
    return this.db.one<{ c: string }>('select count(*) as c from products').then((r) => Number(r.c));
  }
}
