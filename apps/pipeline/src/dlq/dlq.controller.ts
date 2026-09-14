import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { DlqService } from './dlq.service';
import { SourceService } from '../source/source.service';
import { EsSink } from '../sinks/es/es.sink';
import { SinkGate } from '../sinks/sink-gate';
import { MetricsService } from '../metrics/metrics.service';
import { logger } from '../logger';

/**
 * DLQ replay (SPEC §6.4): re-read the CURRENT source row and write it again. Success or 409
 * (a newer version is already indexed) both resolve the entry; a fresh rejection bumps attempts.
 */
@Controller('api/dlq')
export class DlqController {
  constructor(private readonly dlq: DlqService, private readonly source: SourceService, private readonly es: EsSink, private readonly gate: SinkGate, private readonly metrics: MetricsService) {}

  @Get() list(@Query('status') status = 'open', @Query('limit') limit = '100') { return this.dlq.list(status, Number(limit)); }
  @Get('counts') counts() { return this.dlq.counts(); }

  @Post('replay')
  async replay(@Body() body: { ids?: number[]; all?: boolean } = {}) {
    const ids = body.all || !body.ids?.length ? await this.dlq.openIds('es') : body.ids.map(Number);
    if (ids.length === 0) return { replayed: 0, resolved: 0, failed: 0 };
    const products = await this.source.readByIds(ids);
    const missing = ids.filter((id) => !products.some((p) => p.id === id));
    let resolved = 0, failed = 0;
    for (let i = 0; i < products.length; i += 500) {
      const chunk = products.slice(i, i + 500);
      const c = await this.gate.retryUntilOk('es', () => this.es.writeBatch(chunk));
      const okIds = [...c.ok, ...c.duplicates];
      await this.dlq.resolve('es', okIds);
      await this.dlq.bumpAttempts('es', c.dlq);
      this.metrics.recordsWritten.inc({ sink: 'es', mode: 'replay' }, c.ok.length);
      if (c.duplicates.length) this.metrics.duplicatesAbsorbed.inc({ sink: 'es' }, c.duplicates.length);
      resolved += okIds.length; failed += c.dlq.length;
    }
    if (missing.length) { await this.dlq.resolve('es', missing); resolved += missing.length; }   // row gone from source: nothing to replay
    await this.dlq.refreshGauge();
    logger.info({ event: 'dlq_replayed', requested: ids.length, resolved, failed });
    return { replayed: ids.length, resolved, failed };
  }

  @Post(':id/replay')
  async replayOne(@Param('id') id: string) {
    const entry = await this.dlq.get(Number(id));
    if (!entry) return { error: 'not found' };
    return this.replay({ ids: [entry.record_id] });
  }
}
