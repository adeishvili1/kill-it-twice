/** A source row as read from Postgres, already shaped as the document/event payload. */
export interface Product {
  id: number;
  sku: string;
  name: string;
  category: string;
  price: number;
  stock: number;
  attributes: Record<string, unknown>;
  version: number;
  updated_at: string;
  deleted_at: string | null;
}

export type Op = 'insert' | 'update' | 'delete';

export interface ChangeEvent {
  event_id: string;       // "<id>:<version>" — the idempotency key for the consumer
  product_id: number;
  op: Op;
  version: number;
  product: Product;
  emitted_at: string;
}

export const PRODUCT_COLUMNS = `id, sku, name, category, price::float8 as price, stock, attributes, version, updated_at, deleted_at`;

export function toEvent(p: Product, op: Op): ChangeEvent {
  return { event_id: `${p.id}:${p.version}`, product_id: p.id, op, version: p.version, product: p, emitted_at: new Date().toISOString() };
}
