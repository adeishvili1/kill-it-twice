import { Module } from '@nestjs/common';
import { ApiController } from './api.controller';
import { ConsumerService } from './consumer.service';
import { DbService } from './db.service';
import { MetricsService } from './metrics.service';
@Module({ controllers: [ApiController], providers: [DbService, MetricsService, ConsumerService] })
export class AppModule {}
