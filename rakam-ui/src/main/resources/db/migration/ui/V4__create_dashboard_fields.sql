alter table dashboard_items rename column data to options;
alter table dashboard_items add column data bytea;
alter table dashboard_items add column refresh_interval int;
alter table dashboard_items add column last_query_duration int;