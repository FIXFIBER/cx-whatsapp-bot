'use strict';
// Supabase sync layer for the WhatsApp bot.
// Gracefully degrades: if SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are not set,
// every call is a no-op so the bot keeps running on local storage only.
const fs = require('fs');
const path = require('path');

function loadEnv() {
  const envPath = path.join(__dirname, '..', '.env');
  if (fs.existsSync(envPath)) {
    for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
      const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
      if (m && !process.env[m[1]]) {
        let v = m[2].trim();
        if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
        process.env[m[1]] = v;
      }
    }
  }
}
loadEnv();

const URL = process.env.SUPABASE_URL;
const KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

let sb = null;        // supabase client
let enabled = false;
let autoDisabled = false;   // flipped true if the DB is unreachable, so it never breaks the bot
let schemaReady = false;    // probe ran
let schemaMissing = false;  // wa_* tables don't exist yet

if (URL && KEY) {
  try {
    // @supabase/supabase-js loaded lazily so absence never breaks the bot.
    const { createClient } = require('@supabase/supabase-js');
    sb = createClient(URL, KEY, { auth: { persistSession: false } });
    enabled = true;
    console.log('[supabase] sync ENABLED ->', URL);
  } catch (e) {
    console.log('[supabase] client lib missing, sync disabled:', e.message);
  }
} else {
  console.log('[supabase] credentials not set -> local-only mode (sync disabled)');
}

// Batch writer to avoid hammering the DB on every message.
const queue = { contacts: new Map(), chats: new Map(), messages: new Map() };
let flushTimer = null;
let errorStreak = 0;

function enqueue(kind, key, row) {
  if (!enabled || autoDisabled || schemaMissing) return;
  if (kind === 'contacts') queue.contacts.set(key, row);
  else if (kind === 'chats') queue.chats.set(key, row);
  else if (kind === 'messages') queue.messages.set(key, row);
  if (!flushTimer) flushTimer = setTimeout(flush, 1500);
}

async function flush() {
  flushTimer = null;
  if (!enabled || autoDisabled || schemaMissing) return;
  try {
    if (!schemaReady) await probeSchema();
    if (schemaMissing) return;
    const contacts = [...queue.contacts.values()];
    const chats = [...queue.chats.values()];
    const messages = [...queue.messages.values()];
    queue.contacts.clear(); queue.chats.clear(); queue.messages.clear();
    if (contacts.length) await sb.from('wa_contacts').upsert(contacts, { onConflict: 'jid' });
    if (chats.length) await sb.from('wa_chats').upsert(chats, { onConflict: 'jid' });
    if (messages.length) await sb.from('wa_messages').upsert(messages, { onConflict: 'chat_jid,id' });
    errorStreak = 0;
  } catch (e) {
    // Supabase unreachable/paused must NOT break the WhatsApp bot or QR pairing.
    errorStreak++;
    console.log('[supabase] flush error (sync will self-disable after 3 fails):', e.message);
    if (errorStreak >= 3) {
      autoDisabled = true;
      console.log('[supabase] PERMANENTLY disabled this session due to repeated errors — bot continues on local storage only.');
    }
  }
}

// ── public API ──────────────────────────────────────────────
async function probeSchema() {
  if (!enabled || autoDisabled || schemaReady) return;
  try {
    const { error } = await sb.from('wa_contacts').select('jid').limit(1);
    if (error && /relation|does not exist/.test(error.message)) {
      console.log('[supabase] wa_* tables NOT FOUND — sync will pause until schema is applied.');
      console.log('[supabase] Apply supabase/schema.sql once (dashboard SQL editor or `supabase db execute -p <pw> < supabase/schema.sql`).');
      schemaMissing = true;        // stop trying to upsert; bot keeps running on local storage
    }
    schemaReady = true;
  } catch (e) {
    console.log('[supabase] probeSchema error:', e.message);
  }
}
// Convert a timestamp (ISO string / Date / epoch-s) to epoch milliseconds (bigint column).
function toEpoch(ts) {
  if (ts == null) return null;
  if (typeof ts === 'number') {
    // treat values < 1e12 as seconds, larger as ms
    return ts < 1e12 ? Math.round(ts * 1000) : Math.round(ts);
  }
  if (ts instanceof Date) return ts.getTime();
  if (typeof ts === 'string') {
    const n = Number(ts);
    if (!Number.isNaN(n)) return n < 1e12 ? Math.round(n * 1000) : Math.round(n);
    const d = Date.parse(ts);
    return Number.isNaN(d) ? null : d;
  }
  return null;
}

function upsertContact(jid, { name, notify, phone } = {}) {
  enqueue('contacts', jid, { jid, name: name || null, notify: notify || null, phone: phone || null, updated_at: new Date().toISOString() });
}
function upsertChat(jid, { name, isGroup, lastBody, lastTime, unread } = {}) {
  enqueue('chats', jid, {
    jid, name: name || null, is_group: !!isGroup,
    last_message_body: lastBody || null, last_message_time: toEpoch(lastTime),
    unread_count: unread || 0, updated_at: new Date().toISOString()
  });
}
function upsertMessage({ id, chatJid, fromMe, senderJid, body, mediaType, mediaUrl, timestamp, isViewOnce, raw }) {
  if (!id || !chatJid) return;
  enqueue('messages', chatJid + ':' + id, {
    id, chat_jid: chatJid, from_me: !!fromMe, sender_jid: senderJid || null,
    body: body || null, media_type: mediaType || null, media_url: mediaUrl || null,
    timestamp: toEpoch(timestamp), is_view_once: !!isViewOnce, raw: raw || null
  });
}

async function clearAll() {
  if (!enabled) return false;
  try {
    await sb.from('wa_messages').delete().neq('id', '__never__');
    await sb.from('wa_chats').delete().neq('jid', '__never__');
    await sb.from('wa_contacts').delete().neq('jid', '__never__');
    return true;
  } catch (e) {
    console.log('[supabase] clearAll error:', e.message);
    return false;
  }
}

module.exports = { upsertContact, upsertChat, upsertMessage, clearAll, probeSchema, isEnabled: () => enabled, isSchemaMissing: () => schemaMissing, flush };
