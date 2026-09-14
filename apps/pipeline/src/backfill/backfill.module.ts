import { Module } from '@nestjs/common';
import { BackfillService } from './backfill.service';
import { BatchWriter } from '../batch/batch-writer';
import { DlqService } from '../dlq/dlq.service';
@Module({ providers: [BackfillService, BatchWriter, DlqService], exports: [BackfillService, BatchWriter, DlqService] })
export class BackfillModule {}
