-- Cache table for bulk price tier scrape results
-- Used by app.py save_bulk_cache_to_supabase/load_bulk_cache_from_supabase
create table if not exists public.bulk_pricing_cache (
  id bigint primary key,
  data jsonb not null default '{}'::jsonb,
  last_updated timestamptz not null default now()
);

-- Optional seed row expected by eq('id', 1) reads
insert into public.bulk_pricing_cache (id, data, last_updated)
values (1, '{}'::jsonb, now())
on conflict (id) do nothing;
