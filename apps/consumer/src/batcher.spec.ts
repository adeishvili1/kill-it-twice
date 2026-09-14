import { buildInsert, dedupeBuffer, parseEvent, shouldFlush } from './batcher';

describe('shouldFlush', () => {
  it('flushes when the buffer reaches prefetch', () => expect(shouldFlush(500, 500, 0, 10, 200)).toBe(true));
  it('flushes when the oldest message is older than flushAfterMs', () => expect(shouldFlush(3, 500, 1000, 1250, 200)).toBe(true));
  it('does not flush a young, small buffer', () => expect(shouldFlush(3, 500, 1000, 1100, 200)).toBe(false));
  it('never flushes an empty buffer', () => expect(shouldFlush(0, 500, null, 99999, 200)).toBe(false));
});

describe('parseEvent', () => {
  const good = JSON.stringify({ event_id: '7:3', product_id: 7, op: 'update', version: 3, product: { id: 7 } });
  it('parses a good event', () => expect(parseEvent(good).event).toMatchObject({ event_id: '7:3', product_id: 7, version: 3, op: 'update' }));
  it('falls back to messageId', () => expect(parseEvent(JSON.stringify({ product_id: 1, version: 1 }), '1:1').event?.event_id).toBe('1:1'));
  it('reports invalid json instead of throwing', () => expect(parseEvent('{oops').error).toMatch(/invalid json/));
  it('reports missing ids', () => expect(parseEvent(JSON.stringify({ event_id: 'x' })).error).toMatch(/product_id/));
});

describe('buildInsert', () => {
  it('numbers placeholders per row and uses ON CONFLICT DO NOTHING', () => {
    const { text, values } = buildInsert([
      { event_id: '1:1', product_id: 1, op: 'insert', version: 1, payload: { a: 1 } },
      { event_id: '2:1', product_id: 2, op: 'insert', version: 1, payload: { a: 2 } },
    ]);
    expect(text).toContain('($1,$2,$3,$4,$5::jsonb),($6,$7,$8,$9,$10::jsonb)');
    expect(text).toContain('on conflict (event_id) do nothing returning event_id');
    expect(values).toHaveLength(10);
  });
});

describe('dedupeBuffer', () => {
  it('drops repeated event_ids inside one buffer', () => {
    const e = (id: string) => ({ event_id: id, product_id: 1, op: 'update', version: 1, payload: {} });
    const r = dedupeBuffer([e('a'), e('b'), e('a')]);
    expect(r.unique.map((x) => x.event_id)).toEqual(['a', 'b']);
    expect(r.duplicates).toBe(1);
  });
});
