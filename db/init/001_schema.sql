-- Source table -------------------------------------------------------------
create table products (
  id          bigserial primary key,
  sku         text        not null,
  name        text        not null,
  category    text        not null,
  price       numeric(12,2) not null,
  stock       int         not null,
  attributes  jsonb       not null default '{}'::jsonb,
  version     bigint      not null default 1,
  updated_at  timestamptz not null default clock_timestamp(),
  deleted_at  timestamptz null
);

-- Change log: the incremental cursor source ---------------------------------
create table change_log (
  seq         bigserial primary key,
  product_id  bigint      not null,
  op          text        not null check (op in ('insert','update','delete')),
  version     bigint      not null,
  changed_at  timestamptz not null default clock_timestamp()
);

create or replace function products_before_update() returns trigger language plpgsql as $$
begin
  new.version    := old.version + 1;
  new.updated_at := clock_timestamp();
  return new;
end $$;

create or replace function products_after_change() returns trigger language plpgsql as $$
begin
  if tg_op = 'INSERT' then
    insert into change_log(product_id, op, version) values (new.id, 'insert', new.version);
  else
    insert into change_log(product_id, op, version)
      values (new.id, case when new.deleted_at is not null and old.deleted_at is null then 'delete' else 'update' end, new.version);
  end if;
  return null;
end $$;

create trigger trg_products_before_update before update on products
  for each row execute function products_before_update();
create trigger trg_products_after_change after insert or update on products
  for each row execute function products_after_change();

-- Pipeline progress: the ONLY durable checkpoint --------------------------
create table pipeline_state (
  key           text primary key,            -- 'backfill' | 'incremental'
  status        text not null,               -- backfill: idle|running|paused|done ; incremental: running|paused
  last_id       bigint not null default 0,   -- backfill: last committed product id
  target_max_id bigint not null default 0,   -- backfill: snapshot of max(id) at start
  cursor_seq    bigint not null default 0,   -- incremental: last committed change_log.seq
  updated_at    timestamptz not null default clock_timestamp()
);
insert into pipeline_state(key, status) values ('backfill', 'idle'), ('incremental', 'running');

-- Runtime-tunable parameters ------------------------------------------------
create table pipeline_params (
  key   text primary key,
  value text not null
);

-- Dead-letter queue --------------------------------------------------------
create table dlq (
  id          bigserial primary key,
  sink        text        not null,                   -- 'es' | 'rabbitmq'
  record_id   bigint      not null,
  version     bigint      not null,
  payload     jsonb       not null,
  error       text        not null,
  batch_id    text        not null,
  mode        text        not null,                   -- 'backfill' | 'incremental' | 'replay'
  attempts    int         not null default 1,
  status      text        not null default 'open',    -- open | resolved
  created_at  timestamptz not null default clock_timestamp(),
  updated_at  timestamptz not null default clock_timestamp(),
  unique (sink, record_id)
);
create index dlq_status_idx on dlq(status);

-- Consumer's own store (independent service, same instance for simplicity) --
create table consumed_events (
  event_id    text primary key,                       -- "<product_id>:<version>"
  product_id  bigint      not null,
  op          text        not null,
  version     bigint      not null,
  payload     jsonb       not null,
  consumed_at timestamptz not null default clock_timestamp()
);
create index consumed_events_consumed_at_idx on consumed_events(consumed_at desc);

create table consumer_dlq (
  id          bigserial primary key,
  event_id    text        not null,
  payload     jsonb       not null,
  error       text        not null,
  created_at  timestamptz not null default clock_timestamp()
);
