const num = (v: string | undefined, d: number) => (v && !Number.isNaN(Number(v)) ? Number(v) : d);
export const config = {
  port: num(process.env.PORT, 3001),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://app:app@localhost:5435/replication',
  rabbitmqUrl: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
  prefetch: num(process.env.PREFETCH, 500),
  flushAfterMs: num(process.env.FLUSH_AFTER_MS, 200),
  logLevel: process.env.LOG_LEVEL ?? 'info',
  exchange: process.env.RABBIT_EXCHANGE ?? 'products.events',
  queue: process.env.RABBIT_QUEUE ?? 'products.consumer',
  retryBaseMs: 500,
  retryMaxMs: 30000,
};
