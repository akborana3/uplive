---
title: livegram
emoji: 🌍
colorFrom: red
colorTo: blue
sdk: docker
pinned: false
---
# master-livegram

Advanced Telegram livegram system built with **Python + Telethon**.

## Features
- Master controller bot with:
  - CONNECT BOT
  - MY ALL BOTS
  - DISCONNECT
  - ADMIN PANEL
- Multi-assistant runtime with dynamic `.session` upload/start/stop
- HuggingFace Dataset-backed persistent storage with:
  - startup load/sync
  - manual sync button
  - periodic auto-sync
- Assistant features:
  - full message logging to group/owner DM
  - reply bridge (admin reply to forwarded message -> user)
  - `/menu` with Set Start Post, Set Message, Stats, Broadcast
  - broadcast progress (25/50/75/100), ETA, speed, summary

## Configuration
Copy `.env.example` to `.env` and fill values:
- `API_ID`
- `API_HASH`
- `HF_TOKEN`
- `HF_REPO_ID`
- optional: `HF_DATA_PATH`, `MASTER_SESSION_FILE`, `AUTO_SYNC_INTERVAL`

Super admin is enforced by spec as Telegram user ID `8413365423`.

## Run locally
```bash
pip install -r requirements.txt
python -m app.main
```

## Docker
```bash
docker build -t master-livegram .
docker run --env-file .env master-livegram
```
