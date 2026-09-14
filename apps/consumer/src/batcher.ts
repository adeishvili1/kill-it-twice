/**
 * Pure helpers for the consumer's batching (unit-tested, no I/O).
 * The consumer buffers deliveries and flushes when the buffer is full (prefetch) or the oldest
 * buffered message is older than flushAfterMs — whichever comes first.
 */
export interface ParsedEvent { event_id: string; product_id: number; op: string; version: number; payload: unknown }

export function shouldFlush(bufferLen: number, prefetch: number, firstBufferedAt: number | null, now: number, flushAfterMs: number): boolean {
  if (bufferLen === 0) return false;
  if (bufferLen >= prefetch) return true;
  return firstBufferedAt !== null && now - firstBufferedAt >= flushAfterMs;
}

/** Parses a message body; returns an error string instead of throwing so bad messages go to consumer_dlq. */
export function parseEvent(body: string, messageId?: string): { event?: ParsedEvent; error?: string } {
  let obj: any;
  try { obj = JSON.parse(body); } catch (e: any) { return { error: `invalid json: ${e.message}` }; }
  if (!obj || typeof obj !== 'object') return { error: 'body is not an object' };
  const event_id = typeof obj.event_id === 'string' && obj.event_id ? obj.event_id : messageId;
  if (!event_id) return { error: 'missing event_id' };
  const product_id = Number(obj.product_id ?? obj.product?.id);
  const version = Number(obj.version ?? obj.product?.version);
  if (!Number.isFinite(product_id) || !Number.isFinite(version)) return { error: 'missing product_id/version' };
  return { event: { event_id, product_id, op: typeof obj.op === 'string' ? obj.op : 'update', version, payload: obj } };
}

/** One multi-row INSERT ... ON CONFLICT DO NOTHING RETURNING event_id. Duplicates are the rows NOT returned. */
export function buildInsert(events: ParsedEvent[]): { text: string; values: unknown[] } {
  const values: unknown[] = [];
  const tuples = events.map((e, i) => {
    values.push(e.event_id, e.product_id, e.op, e.version, JSON.stringify(e.payload));
    const b = i * 5;
    return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5}::jsonb)`;
  });
  return {
    text: `insert into consumed_events(event_id, product_id, op, version, payload) values ${tuples.join(',')} on conflict (event_id) do nothing returning event_id`,
    values,
  };
}

/** Same event_id twice in one buffer (broker redelivery inside a batch) must not break the multi-row insert. */
export function dedupeBuffer(events: ParsedEvent[]): { unique: ParsedEvent[]; duplicates: number } {
  const seen = new Set<string>();
  const unique: ParsedEvent[] = [];
  for (const e of events) {
    if (seen.has(e.event_id)) continue;
    seen.add(e.event_id);
    unique.push(e);
  }
  return { unique, duplicates: events.length - unique.length };
}
