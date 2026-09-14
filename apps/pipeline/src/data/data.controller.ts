import { Controller, Get, Param, Query } from '@nestjs/common';
import { EsSink } from '../sinks/es/es.sink';
import { DbService } from '../db/db.service';
import { PRODUCT_COLUMNS } from '../source/product';

/** Data browser API (UI screen 2): search the index, compare a record across the three stores. */
@Controller('api/data')
export class DataController {
  constructor(private readonly es: EsSink, private readonly db: DbService) {}

  @Get('search')
  async search(@Query('q') q = '', @Query('category') category = '', @Query('page') page = '1', @Query('size') size = '25') {
    const must: any[] = [];
    if (q.trim()) must.push(/^\d+$/.test(q.trim()) ? { term: { id: Number(q.trim()) } } : { multi_match: { query: q, fields: ['name^2', 'sku', 'attributes.description', 'attributes.brand', 'attributes.tags'] } });
    if (category) must.push({ term: { category } });
    const from = (Math.max(1, Number(page)) - 1) * Number(size);
    try {
      const r = await this.es.client.search({
        index: this.es.index, from, size: Number(size),
        query: must.length ? { bool: { must } } : { match_all: {} },
        sort: must.length && q.trim() ? undefined : [{ updated_at: 'desc' }, { id: 'asc' }],
        track_total_hits: true,
      });
      const total = typeof r.hits.total === 'number' ? r.hits.total : r.hits.total?.value ?? 0;
      return { total, hits: r.hits.hits.map((h) => h._source) };
    } catch (e: any) {
      return { total: 0, hits: [], error: e.message };
    }
  }

  @Get('categories')
  async categories() {
    try {
      const r = await this.es.client.search({ index: this.es.index, size: 0, aggs: { c: { terms: { field: 'category', size: 50 } } } });
      return ((r.aggregations as any)?.c?.buckets ?? []).map((b: any) => ({ key: b.key, count: b.doc_count }));
    } catch { return []; }
  }

  @Get('events')
  events(@Query('limit') limit = '50') {
    return this.db.query(`select event_id, product_id, op, version, consumed_at, payload->'product'->>'name' as name from consumed_events order by consumed_at desc limit $1`, [Number(limit)]);
  }

  @Get(':id')
  async byId(@Param('id') id: string) {
    const [source, es, events, dlq] = await Promise.all([
      this.db.one(`select ${PRODUCT_COLUMNS} from products where id=$1`, [Number(id)]),
      this.es.client.get({ index: this.es.index, id }).then((r) => r._source).catch(() => null),
      this.db.query(`select event_id, op, version, consumed_at from consumed_events where product_id=$1 order by version desc limit 10`, [Number(id)]),
      this.db.query(`select id, sink, error, attempts, status, batch_id, updated_at from dlq where record_id=$1`, [Number(id)]),
    ]);
    return { source: source ?? null, es, events, dlq };
  }
}
