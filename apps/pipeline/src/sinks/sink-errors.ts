/** Thrown by a sink when the sink itself is unavailable (not a data problem). The batch writer
 *  retries the same batch with capped backoff until the sink is back. */
export class SinkUnavailableError extends Error {
  constructor(public readonly sink: 'es' | 'rabbitmq', message: string, public readonly cause?: unknown) {
    super(`${sink} unavailable: ${message}`);
    this.name = 'SinkUnavailableError';
  }
}
