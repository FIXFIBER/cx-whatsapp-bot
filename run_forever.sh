#!/usr/bin/env bash
# Keep the CX WhatsApp web client reachable: runs the bot and a localhost.run
# tunnel, restarting either if it dies. Run with: nohup bash /home/darkaxis/whatsappforreal/whatsapp-bot/run_forever.sh >/tmp/cx_forever.log 2>&1 &
set -u
DIR=/home/darkaxis/whatsappforreal/whatsapp-bot
PORT=3001
while true; do
  # ── bot ──
  if ! pgrep -f "node $DIR/whats.js" >/dev/null; then
    echo "[$(date)] starting bot"
    ( cd "$DIR" && DATA_DIR="$DIR/data" PORT=$PORT node whats.js >>/tmp/bot_render.log 2>&1 ) &
  fi
  # ── localhost.run tunnel (free, anonymous) ──
  if ! pgrep -f "nokey@localhost.run" >/dev/null; then
    echo "[$(date)] starting tunnel"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=30 -R 80:localhost:$PORT nokey@localhost.run >>/tmp/tunnel.log 2>&1 &
  fi
  sleep 20
done
