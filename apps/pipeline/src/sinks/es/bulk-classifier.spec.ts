import { classifyBulkItems, flattenBulkResponse } from './bulk-classifier';

describe('classifyBulkItems', () => {
  it('splits a mixed batch into ok / duplicate / retry / dlq', () => {
    const c = classifyBulkItems([
      { id: 1, status: 201 },
      { id: 2, status: 200 },
      { id: 3, status: 409, error: 'version_conflict_engine_exception' },
      { id: 4, status: 400, error: 'mapper_parsing_exception: failed to parse field [attributes.weight_kg]' },
      { id: 5, status: 429, error: 'es_rejected_execution_exception' },
      { id: 6, status: 503, error: 'unavailable_shards_exception' },
      { id: 7, status: 404 },
    ]);
    expect(c.ok).toEqual([1, 2]);
    expect(c.duplicates).toEqual([3]);
    expect(c.retry).toEqual([5, 6]);
    expect(c.dlq.map((d) => d.id)).toEqual([4, 7]);
    expect(c.dlq[0].error).toContain('mapper_parsing_exception');
  });

  it('497 good + 3 bad never marks the batch for rollback', () => {
    const items = Array.from({ length: 500 }, (_, i) => ({ id: i + 1, status: i < 3 ? 400 : 201, error: i < 3 ? 'bad' : undefined }));
    const c = classifyBulkItems(items);
    expect(c.ok).toHaveLength(497);
    expect(c.dlq).toHaveLength(3);
    expect(c.retry).toHaveLength(0);
  });
});

describe('flattenBulkResponse', () => {
  it('extracts id, status and a readable error', () => {
    const flat = flattenBulkResponse([
      { index: { _id: '10', status: 201 } },
      { index: { _id: '11', status: 400, error: { type: 'mapper_parsing_exception', reason: 'failed to parse', caused_by: { type: 'number_format_exception', reason: 'For input string: "oops"' } } } },
    ]);
    expect(flat[0]).toEqual({ id: 10, status: 201, error: undefined });
    expect(flat[1].id).toBe(11);
    expect(flat[1].error).toBe('mapper_parsing_exception: failed to parse (number_format_exception: For input string: "oops")');
  });
});
