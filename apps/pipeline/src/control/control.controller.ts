import { Body, Controller, Get, Post, Put } from '@nestjs/common';
import { BackfillService } from '../backfill/backfill.service';
import { IncrementalService } from '../incremental/incremental.service';
import { CheckpointService } from '../checkpoint/checkpoint.service';

@Controller('api/control')
export class ControlController {
  constructor(private readonly backfill: BackfillService, private readonly incremental: IncrementalService, private readonly ckpt: CheckpointService) {}

  @Post('backfill/start')  startBackfill()  { return this.backfill.start(); }
  @Post('backfill/pause')  pauseBackfill()  { return this.backfill.pause(); }
  @Post('backfill/resume') resumeBackfill() { return this.backfill.resume(); }
  @Post('backfill/reset')  resetBackfill()  { return this.backfill.reset(); }
  @Post('incremental/pause')  pauseIncremental()  { return this.incremental.pause(); }
  @Post('incremental/resume') resumeIncremental() { return this.incremental.resume(); }

  @Get('params') getParams() { return this.ckpt.params; }
  @Put('params') async setParams(@Body() body: Partial<{ batchSize: number; incrementalIntervalMs: number; safetyWindowMs: number }>) {
    await this.ckpt.setParams({
      ...(body.batchSize !== undefined ? { batchSize: Number(body.batchSize) } : {}),
      ...(body.incrementalIntervalMs !== undefined ? { incrementalIntervalMs: Number(body.incrementalIntervalMs) } : {}),
      ...(body.safetyWindowMs !== undefined ? { safetyWindowMs: Number(body.safetyWindowMs) } : {}),
    });
    return this.ckpt.params;
  }
}
