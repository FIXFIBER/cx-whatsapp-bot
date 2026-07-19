CREATE TABLE IF NOT EXISTS public.wa_session_kv (
  key text PRIMARY KEY,
  value jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.wa_session_kv ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "deny anon session" ON public.wa_session_kv;
CREATE POLICY "deny anon session" ON public.wa_session_kv FOR ALL TO anon USING (false) WITH CHECK (false);
