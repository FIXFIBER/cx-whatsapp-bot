# CX WhatsApp Web

A self-hosted **WhatsApp Web clone** built on [Baileys](https://github.com/WhiskeySockets/Baileys).
One WhatsApp number, accessible from many devices through a web client. The number is
**owned by a single "head" device** (the phone that paired it) — other devices can only
join by scanning an invite QR the head generates, and only the head can see/remove
connected devices.

The bot **serves its own client** at `/app`, so the entire thing is ONE service — no
separate frontend host, no custom domain required.

## What it does
- Pair your real WhatsApp number via QR (same as WhatsApp Web).
- Full chat list + **complete message history** (no 50-message cap) — backfills from day one.
- Send & receive **text, images, video, audio, voice notes, documents**.
- Mobile-first responsive UI (phone / tablet / desktop).
- **Head/owner access model**: you (the paired phone's owner) control who can connect.
  - First open → "Claim as owner" → you become the head.
  - Other phones scan your **invite QR** → join as pending → you approve in "My Devices".
  - You can remove any device anytime; removed devices lose access immediately.
- Optional Supabase sync (chat/message backup).

## Run locally
```bash
npm install
DATA_DIR=./data PORT=3001 npm start
# open http://localhost:3001/app
```

## Deploy to Render (one service, no computer needed)
1. Push this folder to a GitHub repo.
2. In Render: **New → Blueprint** and select the repo (uses `render.yaml`), OR
   **New → Web Service** → connect repo, root directory `.`, runtime **Docker** (uses `Dockerfile`).
3. Plan: **Starter** (the free plan spins down and would drop the WhatsApp socket — use Starter
   so the number stays connected 24/7).
4. Disk: a 1 GB persistent disk is mounted at `/data` (holds the WhatsApp session + device registry,
   so the link survives redeploys/restarts — **do not skip this**).
5. Set env vars: `PORT=3001`, `DATA_DIR=/data`, `PUBLIC_URL=<your-render-url>`,
   optionally `ADMIN_KEY` (after you've claimed ownership once).
6. Deploy. Open `https://<your-service>.onrender.com/app`, click **Claim as owner**, scan the
   WhatsApp QR with your phone. Done — it runs in the cloud, independent of your laptop.

### First-deploy owner claim
On a brand-new service with no `ADMIN_KEY` and no devices yet, the **first** client to open `/app`
and click "Claim as owner" becomes the head. Copy the generated **Head key** (My Devices → Head key)
and set it as `ADMIN_KEY` so nobody else can take ownership. If you ever lose the key, the only
recovery is to delete `devices.json` on the disk and re-claim.

## Files
- `whats.js` — the bot (Baileys + Express + Socket.IO) and the admin UI at `/`.
- `access.js` — head/device registry + invite logic (persisted to `devices.json`).
- `public/index.html` — the self-contained web client served at `/app`.
- `render.yaml` / `Dockerfile` — Render deploy config.
- `supabase/` — optional DB sync.
