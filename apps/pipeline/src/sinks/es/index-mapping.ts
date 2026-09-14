/**
 * Strict mapping: an unknown field or a wrongly typed value is rejected per item by the bulk
 * API with a 400 mapper_parsing_exception. That is the G4 vector — `attributes.weight_kg`
 * set to a string in the source is "bad data" the index cannot accept.
 */
export const productsIndexBody = {
  settings: {
    number_of_shards: 1,
    number_of_replicas: 0,
    refresh_interval: '5s',
  },
  mappings: {
    dynamic: 'strict',
    properties: {
      id: { type: 'long' },
      sku: { type: 'keyword' },
      name: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
      category: { type: 'keyword' },
      price: { type: 'double' },
      stock: { type: 'integer' },
      version: { type: 'long' },
      updated_at: { type: 'date' },
      deleted_at: { type: 'date' },
      attributes: {
        dynamic: 'strict',
        properties: {
          brand: { type: 'keyword' },
          color: { type: 'keyword' },
          weight_kg: { type: 'double' },
          tags: { type: 'keyword' },
          description: { type: 'text' },
        },
      },
    },
  },
} as const;
