alter table public.dashboard_items rename column data to options;
alter table public.dashboard_items add column data bytea;
alter table public.dashboard_items add column refresh_interval int;
alter table public.dashboard_items add column last_query_duration int;