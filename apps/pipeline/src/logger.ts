import pino from 'pino';
import { config } from './config';

// One JSON logger for the process. Every state transition logs an `event` field with a fixed
// name (see SPEC §6.5) so `docker compose logs pipeline | grep '"event":"backfill_resumed"'` works.
export const logger = pino({ level: config.logLevel, base: { app: 'pipeline' }, timestamp: pino.stdTimeFunctions.isoTime });
