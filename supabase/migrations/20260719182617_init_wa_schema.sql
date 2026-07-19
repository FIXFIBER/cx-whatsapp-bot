-- WhatsApp bot → Supabase sync schema
-- Project: sbwppnecqxdxhftcgtnm (evolution-whatsapp-bot)
-- Apply with: supabase db push --linked --include-all  (or paste into Supabase SQL Editor)

-- Contacts (one row per WhatsApp JID)
create table if not exists wa_contacts (
  jid         text primary key,
  name        text,
  notify      text,
  phone       text,
  created_at  timestamptz default now(),
  updated_at  timestamptz default now()
);

-- Chats (conversations; only real threads with messages)
create table if not exists wa_chats (
  jid                  text primary key,
  name                 text,
  is_group             boolean default false,
  last_message_body    text,
  last_message_time    bigint,
  unread_count         int default 0,
  created_at           timestamptz default now(),
  updated_at           timestamptz default now()
);

-- Messages (one row per message key)
create table if not exists wa_messages (
  id              text not null,
  chat_jid        text not null,
  from_me         boolean default false,
  sender_jid      text,
  body            text,
  media_type      text,
  media_url       text,
  timestamp       bigint,
  is_view_once    boolean default false,
  raw             jsonb,
  created_at      timestamptz default now(),
  primary key (chat_jid, id)
);

create index if not exists wa_messages_chat_idx on wa_messages (chat_jid, timestamp);
create index if not exists wa_chats_updated_idx on wa_chats (updated_at desc);

-- Upsert helpers (idempotent on re-sync)
create or replace function upsert_wa_contact(
  p_jid text, p_name text, p_notify text, p_phone text
) returns void language sql as $$
  insert into wa_contacts (jid, name, notify, phone, updated_at)
  values (p_jid, p_name, p_notify, p_phone, now())
  on conflict (jid) do update set
    name = coalesce(excluded.name, wa_contacts.name),
    notify = coalesce(excluded.notify, wa_contacts.notify),
    phone = coalesce(excluded.phone, wa_contacts.phone),
    updated_at = now();
$$;

create or replace function upsert_wa_chat(
  p_jid text, p_name text, p_is_group boolean,
  p_last_body text, p_last_time bigint, p_unread int
) returns void language sql as $$
  insert into wa_chats (jid, name, is_group, last_message_body, last_message_time, unread_count, updated_at)
  values (p_jid, p_name, p_is_group, p_last_body, p_last_time, p_unread, now())
  on conflict (jid) do update set
    name = coalesce(excluded.name, wa_chats.name),
    is_group = excluded.is_group,
    last_message_body = excluded.last_message_body,
    last_message_time = excluded.last_message_time,
    unread_count = excluded.unread_count,
    updated_at = now();
$$;

create or replace function upsert_wa_message(
  p_id text, p_chat_jid text, p_from_me boolean, p_sender_jid text,
  p_body text, p_media_type text, p_media_url text, p_timestamp bigint,
  p_is_view_once boolean, p_raw jsonb
) returns void language sql as $$
  insert into wa_messages (id, chat_jid, from_me, sender_jid, body, media_type, media_url, timestamp, is_view_once, raw, created_at)
  values (p_id, p_chat_jid, p_from_me, p_sender_jid, p_body, p_media_type, p_media_url, p_timestamp, p_is_view_once, p_raw, now())
  on conflict (chat_jid, id) do update set
    body = excluded.body,
    media_type = excluded.media_type,
    media_url = excluded.media_url,
    is_view_once = excluded.is_view_once,
    raw = excluded.raw;
$$;
