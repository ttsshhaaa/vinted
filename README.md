# Vinted Autopilot

Multi-user Vinted search dashboard with favorites, Discord watchers, admin panel and SQLite persistence.

## Local start

```powershell
cd C:\par
python -m pip install -r requirements.txt
python .\app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000).

## Default admin

- Username: `kon1337`
- Password: `thklty13`

Change it after first login in production.

## Railway setup

1. Add a Railway Volume to the service.
2. Mount it to `/data` or `/app/data`.
3. Add env var `DATA_DIR` to the same mount path.
4. Add env var `FLASK_SECRET_KEY` with a long random value.
5. Optional but recommended for hosted deployments:
   - `SEARCH_MODE=lite`
   - `GEO_COOLDOWN_SECONDS=1800`
   - `DETAIL_CACHE_TTL_SECONDS=21600`
6. Keep a single web worker/replica for the built-in watcher loop.
7. Redeploy.

The app will then store:
- SQLite DB at `<DATA_DIR>/app.db`
- exports at `<DATA_DIR>/output`

If `DATA_DIR` is not set but Railway volume is mounted at `/data` or `/app/data`, the app now auto-detects that path and keeps watchers/favorites/database across redeploys.

## Features

- login-first homepage
- user accounts
- admin panel for user management
- user-scoped favorites
- user-scoped Discord watchers
- persistent SQLite database
- multi-geo search with extended geo list
- listing age and seller last-online display

## Console mode

```powershell
python .\vinted_parser.py --query "nike tech fleece" --geo fr,de,it --pages 2
```
