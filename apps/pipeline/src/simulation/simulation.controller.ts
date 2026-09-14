import { Body, Controller, Get, Post } from '@nestjs/common';
import { BatchWriter } from '../batch/batch-writer';
import { DbService } from '../db/db.service';
import { logger } from '../logger';

/** Chaos hooks (SPEC §6.6) used by verify.sh and the UI's Simulation screen. */
@Controller('api/sim')
export class SimulationController {
  constructor(private readonly writer: BatchWriter, private readonly db: DbService) {}

  @Get() state() { return { crash_after_next_batch: this.writer.crashAfterAck }; }

  /** exit(1) after the next batch's sink acks and BEFORE its checkpoint. */
  @Post('crash-after-next-batch')
  crash() { this.writer.crashAfterAck = true; logger.warn({ event: 'crash_armed' }); return { armed: true }; }

  /** Make rows the index cannot accept: attributes.weight_kg becomes a string. */
  @Post('corrupt')
  async corrupt(@Body() body: { ids?: number[]; count?: number }) {
    let ids = body.ids?.map(Number) ?? [];
    if (ids.length === 0) {
      const n = Math.max(1, Math.min(1000, Number(body.count ?? 3)));
      ids = (await this.db.query<{ id: string }>(`select id from products where deleted_at is null order by random() limit $1`, [n])).map((r) => Number(r.id));
    }
    await this.db.query(`update products set attributes = jsonb_set(attributes, '{weight_kg}', '"not-a-number"') where id = any($1::bigint[])`, [ids]);
    logger.warn({ event: 'sim_corrupt', ids });
    return { corrupted: ids };
  }

  /** Undo corruption: give the rows a valid weight again (used by verify before DLQ replay). */
  @Post('fix')
  async fix(@Body() body: { ids: number[] }) {
    const ids = (body.ids ?? []).map(Number);
    await this.db.query(`update products set attributes = jsonb_set(attributes, '{weight_kg}', to_jsonb(round((random()*50)::numeric, 3))) where id = any($1::bigint[])`, [ids]);
    return { fixed: ids };
  }

  /** Random source activity: updates (default), inserts and soft deletes, optionally over a fixed id set. */
  @Post('generate-changes')
  async generate(@Body() body: { count?: number; ops?: Array<'update' | 'insert' | 'delete'>; ids?: number[] }) {
    const count = Math.max(1, Math.min(200_000, Number(body.count ?? 100)));
    const ops = body.ops?.length ? body.ops : ['update'];
    const result = { updates: 0, inserts: 0, deletes: 0 };
    const per = Math.ceil(count / ops.length);
    for (const op of ops) {
      if (op === 'update') {
        const r = body.ids?.length
          ? await this.db.query(`update products set stock = stock + 1 where id = any($1::bigint[]) returning id`, [body.ids.map(Number)])
          : await this.db.query(`update products set stock = stock + 1, price = round((price * 1.01)::numeric, 2) where id in (select id from products where deleted_at is null order by random() limit $1) returning id`, [per]);
        result.updates += r.length;
      } else if (op === 'insert') {
        const r = await this.db.query(
          `insert into products (sku, name, category, price, stock, attributes)
           select 'SKU-NEW-' || gen_random_uuid(), 'Generated item', 'generated', round((random()*990+10)::numeric,2), (random()*100)::int,
                  jsonb_build_object('brand','acme','color','red','weight_kg', round((random()*50)::numeric,3),'tags', jsonb_build_array('generated'),'description','generated row')
           from generate_series(1, $1) returning id`, [per]);
        result.inserts += r.length;
      } else if (op === 'delete') {
        const r = await this.db.query(`update products set deleted_at = clock_timestamp() where id in (select id from products where deleted_at is null order by random() limit $1) returning id`, [per]);
        result.deletes += r.length;
      }
    }
    logger.info({ event: 'sim_generate_changes', ...result });
    return result;
  }
}
