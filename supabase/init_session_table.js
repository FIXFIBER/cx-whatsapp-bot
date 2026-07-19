'use strict';
// One-shot: create the wa_session_kv table (stores the Baileys auth session
// in Supabase so the bot reconnects after any restart WITHOUT re-scanning).
// Uses the Supabase SQL REST API — no psql, no dashboard needed.
// Replicates supabase/sync.js env loading so credentials are picked up.
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
if (!URL || !KEY) { console.error('Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY'); process.exit(1); }

const base = URL.replace(/\/$/, '');
const sql = `
CREATE TABLE IF NOT EXISTS public.wa_session_kv (
  key text PRIMARY KEY,
  value jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.wa_session_kv ENABLE ROW LEVEL SECURITY;
-- Service role bypasses RLS; this deny-all policy blocks anonymous API access
-- so the session can never be read from the public/anon key.
DROP POLICY IF EXISTS "deny anon" ON public.wa_session_kv;
CREATE POLICY "deny anon" ON public.wa_session_kv FOR ALL TO anon USING (false) WITH CHECK (false);
`;

(async () => {
  const res = await fetch(base + '/sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + KEY, 'Accept': 'application/json' },
    body: JSON.stringify({ query: sql })
  });
  const text = await res.text();
  if (!res.ok) { console.error('SQL API error', res.status, text); process.exit(1); }
  console.log('OK — wa_session_kv ready. Response:', text.slice(0, 200));
})().catch(e => { console.error('FAILED', e.message); process.exit(1); });
