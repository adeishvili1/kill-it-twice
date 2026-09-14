/**
 * Classifies every item of an Elasticsearch bulk response (SPEC §6.4):
 *   2xx           -> ok        (written)
 *   409           -> duplicate (version already applied or newer: replay / stale write)
 *   429, 5xx      -> retry     (the sink is struggling; not a data problem)
 *   other 4xx     -> dlq       (the record itself is rejected; the rest of the batch stands)
 * Pure function so it can be unit-tested without ES.
 */
export interface BulkItemResult { id: number; status: number; error?: string }
export interface Classified {
  ok: number[];
  duplicates: number[];
  retry: number[];
  dlq: Array<{ id: number; error: string }>;
}

export function classifyBulkItems(items: BulkItemResult[]): Classified {
  const out: Classified = { ok: [], duplicates: [], retry: [], dlq: [] };
  for (const it of items) {
    if (it.status >= 200 && it.status < 300) out.ok.push(it.id);
    else if (it.status === 409) out.duplicates.push(it.id);
    else if (it.status === 429 || it.status >= 500) out.retry.push(it.id);
    else out.dlq.push({ id: it.id, error: it.error ?? `http ${it.status}` });
  }
  return out;
}

/** Flattens the raw bulk response into BulkItemResult[]; ids are parsed from `_id`. */
export function flattenBulkResponse(items: Array<Record<string, any>>): BulkItemResult[] {
  return items.map((wrapper) => {
    const action = wrapper.index ?? wrapper.create ?? wrapper.update ?? wrapper.delete ?? {};
    const err = action.error;
    const error = err ? `${err.type}: ${err.reason}${err.caused_by ? ` (${err.caused_by.type}: ${err.caused_by.reason})` : ''}` : undefined;
    return { id: Number(action._id), status: Number(action.status), error };
  });
}
