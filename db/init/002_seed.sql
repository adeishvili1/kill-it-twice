-- Reset the world and generate n source rows. Called by the pipeline's /api/admin/seed
-- (make seed). Triggers are disabled for the transaction so the seed does not produce
-- 1M change_log rows for the incremental loop to chase.
create or replace function seed_products(n bigint) returns bigint language plpgsql as $$
declare
  cats text[] := array['electronics','books','garden','toys','sports','kitchen','fashion','auto','beauty','office','pets','music'];
  brands text[] := array['acme','globex','initech','umbrella','stark','wayne','hooli','vandelay'];
  colors text[] := array['red','blue','green','black','white','silver','gold'];
begin
  set local session_replication_role = replica;   -- disables triggers for this transaction
  truncate products, change_log, dlq, consumed_events, consumer_dlq restart identity;
  perform setseed(0.42);
  insert into products (sku, name, category, price, stock, attributes)
  select
    'SKU-' || lpad(g::text, 9, '0'),
    initcap(brands[1 + (g % 8)]) || ' ' || cats[1 + (g % 12)] || ' item ' || g,
    cats[1 + (g % 12)],
    round((random() * 990 + 10)::numeric, 2),
    (random() * 1000)::int,
    jsonb_build_object(
      'brand',     brands[1 + (g % 8)],
      'color',     colors[1 + ((g * 7) % 7)],
      'weight_kg', round((random() * 50)::numeric, 3),
      'tags',      jsonb_build_array(cats[1 + (g % 12)], colors[1 + (g % 7)]),
      'description', repeat(md5(g::text), 4)     -- ~130 bytes of filler so a row is ~0.5 KB in JSON
    )
  from generate_series(1, n) as g;
  update pipeline_state set status = 'idle', last_id = 0, target_max_id = 0, updated_at = clock_timestamp() where key = 'backfill';
  update pipeline_state set status = 'running', cursor_seq = 0, updated_at = clock_timestamp() where key = 'incremental';
  return n;
end $$;
