const num = (v: string | undefined, d: number) => (v && !Number.isNaN(Number(v)) ? Number(v) : d);

export const config = {
  port: num(process.env.PORT, 3000),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://app:app@localhost:5435/replication',
  elasticsearchUrl: process.env.ELASTICSEARCH_URL ?? 'http://localhost:9200',
  rabbitmqUrl: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
  logLevel: process.env.LOG_LEVEL ?? 'info',
  // defaults for runtime-tunable params (persisted in pipeline_params once changed)
  batchSize: num(process.env.BATCH_SIZE, 500),
  incrementalIntervalMs: num(process.env.INCREMENTAL_INTERVAL_MS, 1000),
  safetyWindowMs: num(process.env.SAFETY_WINDOW_MS, 2000),
  // sink retry policy
  retryBaseMs: num(process.env.RETRY_BASE_MS, 500),
  retryMaxMs: num(process.env.RETRY_MAX_MS, 30000),
  esRequestTimeoutMs: num(process.env.ES_REQUEST_TIMEOUT_MS, 10000),
  esIndex: process.env.ES_INDEX ?? 'products',
  rabbitExchange: process.env.RABBIT_EXCHANGE ?? 'products.events',
  rabbitQueue: process.env.RABBIT_QUEUE ?? 'products.consumer',
};
