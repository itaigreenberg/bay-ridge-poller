# Bay Ridge Citi Bike — Polling Service

Polls the Citi Bike GBFS feed every 5 minutes via GitHub Actions
and writes station snapshots + inferred trip counts to Supabase.

## Setup

### 1. Supabase
Run `schema.sql` in your Supabase SQL Editor to create the tables.

### 2. GitHub Secrets
In your GitHub repo → Settings → Secrets and variables → Actions → New repository secret:
- `SUPABASE_URL` — your Supabase project URL (e.g. https://xxxx.supabase.co)
- `SUPABASE_SERVICE_KEY` — your Supabase secret/service role key

### 3. Enable GitHub Actions
Push this repo to GitHub. The workflow will start automatically.
To trigger a manual run: Actions tab → "Poll Citi Bike Live Data" → Run workflow.

## How it works

Each run (every 5 min):
1. Loads previous bike counts from Supabase (stateless — no memory between runs)
2. Fetches current GBFS data from Citi Bike
3. Writes a snapshot of all Bay Ridge stations
4. Computes delta vs previous snapshot → infers trips departed/arrived
5. Updates hourly rollup aggregates
6. Prunes raw snapshots older than 48 hours (keeps DB lean)

## Local testing

```bash
pip install -r requirements.txt
export SUPABASE_URL=https://xxxx.supabase.co
export SUPABASE_SERVICE_KEY=your-secret-key
python poller.py
```
