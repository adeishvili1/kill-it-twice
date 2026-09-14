import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { config } from './config';
import { logger } from './logger';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
  app.enableCors();
  app.enableShutdownHooks();
  await app.listen(config.port);
  logger.info({ event: 'pipeline_started', port: config.port, batch_size: config.batchSize, incremental_interval_ms: config.incrementalIntervalMs });
}
process.on('unhandledRejection', (e: any) => logger.error({ event: 'unhandled_rejection', error: e?.message ?? String(e) }));
bootstrap().catch((e) => { logger.error({ event: 'bootstrap_failed', error: e.message }); process.exit(1); });
