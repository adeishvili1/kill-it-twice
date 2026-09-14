import { Module } from '@nestjs/common';
import { IncrementalService } from './incremental.service';
import { BackfillModule } from '../backfill/backfill.module';
@Module({ imports: [BackfillModule], providers: [IncrementalService], exports: [IncrementalService] })
export class IncrementalModule {}
