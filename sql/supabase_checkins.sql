-- Daily check-in tables for Supabase (PostgreSQL)
-- Connection string example:
-- postgresql://postgres:NCMHv0876200812@@db.lfwqhuqtqnfbbapthtpc.supabase.co:5432/postgres

create table if not exists public.daily_checkins (
    group_chat_id text not null,
    user_id text not null,
    streak integer not null default 0,
    last_checkin_date text,
    last_checkin_at bigint,
    total_checkins integer not null default 0,
    updated_at bigint,
    constraint daily_checkins_pk primary key (group_chat_id, user_id)
);

create table if not exists public.daily_checkin_logs (
    id bigserial primary key,
    group_chat_id text not null,
    user_id text not null,
    checkin_date text not null,
    checkin_at bigint not null,
    streak integer not null
);

create index if not exists daily_checkin_logs_group_date_idx
    on public.daily_checkin_logs (group_chat_id, checkin_date);
