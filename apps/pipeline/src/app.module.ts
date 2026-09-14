import { Module } from '@nestjs/common';
import { DbModule } from './db/db.module';
import { MetricsModule } from './metrics/metrics.module';
import { CheckpointModule } from './checkpoint/checkpoint.module';
import { SourceModule } from './source/source.module';
import { SinksModule } from './sinks/sinks.module';
import { BackfillModule } from './backfill/backfill.module';
import { IncrementalModule } from './incremental/incremental.module';
import { ControlController } from './control/control.controller';
import { StatusController } from './control/status.controller';
import { StatusService } from './control/status.service';
import { DlqController } from './dlq/dlq.controller';
import { SimulationController } from './simulation/simulation.controller';
import { AdminController } from './admin/admin.controller';
import { DataController } from './data/data.controller';

@Module({
  imports: [DbModule, MetricsModule, CheckpointModule, SourceModule, SinksModule, BackfillModule, IncrementalModule],
  controllers: [ControlController, StatusController, DlqController, SimulationController, AdminController, DataController],
  providers: [StatusService],
})
export class AppModule {}
