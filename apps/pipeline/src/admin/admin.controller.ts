import { Body, Controller, Post } from '@nestjs/common';
import { DbService } from '../db/db.service';
import { CheckpointService } from '../checkpoint/checkpoint.service';
import { EsSink } from '../sinks/es/es.sink';
import { RabbitSink } from '../sinks/rabbit/rabbit.sink';
import { BackfillService } from '../backfill/backfill.service';
import { IncrementalService } from '../incremental/incremental.service';
import { DlqService } from '../dlq/dlq.service';
import { sleep } from '../util';
import { logger } from '../logger';

/** `make seed`: reset the world and generate N rows. Triggers are off during the seed (db/init/002). */
@Controller('api/admin')
export class AdminController {
  constructor(
    private readonly db: DbService, private readonly ckpt: CheckpointService, private readonly es: EsSink,
    private readonly rabbit: RabbitSink, private readonly backfill: BackfillService, private readonly incremental: IncrementalService,
    private readonly dlq: DlqService,
  ) {}

  @Post('seed')
  async seed(@Body() body: { rows?: number }) {
    const rows = Math.max(1, Math.min(20_000_000, Number(body.rows ?? 1_000_000)));
    const t0 = Date.now();
    // park both loops, wait for in-flight batches to finish
    await this.ckpt.setBackfill({ status: 'idle', lastId: 0, targetMaxId: 0 });
    await this.ckpt.setIncremental({ status: 'paused' });
    while (this.backfill.isBusy() || this.incremental.isBusy()) await sleep(100);
    logger.info({ event: 'seed_started', rows });
    await this.db.query('select seed_products($1)', [rows]);
    await this.es.recreateIndex();
    await this.rabbit.purgeQueue();
    await this.ckpt.reload();                    // seed_products reset pipeline_state
    await this.ckpt.setIncremental({ status: 'running', cursorSeq: 0 });
    await this.dlq.refreshGauge();
    const ms = Date.now() - t0;
    logger.info({ event: 'seed_done', rows, ms });
    return { rows, ms, backfill: this.ckpt.backfill, incremental: this.ckpt.incremental };
  }
}
