"""
Bay Ridge Citi Bike — Stateless Polling Service
================================================
Designed to run as a GitHub Actions cron job every 5 minutes.
Pulls previous state from Supabase at startup (stateless between runs).

Environment variables required:
  SUPABASE_URL          — your Supabase project URL
  SUPABASE_SERVICE_KEY  — your Supabase service role / secret key
"""

import os, logging, requests
from datetime import datetime, timezone
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']

INFO_URL   = 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json'
STATUS_URL = 'https://gbfs.citibikenyc.com/gbfs/en/station_status.json'

# Max organic bike change between polls before flagging as rebalance truck
REBALANCE_THRESHOLD = 4

BAY_RIDGE_POLYGON = [
    [40.642136, -74.028204],
    [40.642574, -74.033397],
    [40.639062, -74.037645],
    [40.625731, -74.042459],
    [40.615543, -74.041988],
    [40.609104, -74.038589],
    [40.608351, -74.035143],
    [40.610179, -74.03028],
    [40.617274, -74.024333],
    [40.622865, -74.019092],
    [40.633146, -74.014655],
    [40.636048, -74.014372],
    [40.642136, -74.028204],
]

# ── HELPERS ───────────────────────────────────────────────

def in_bay_ridge(lat, lon):
    poly = BAY_RIDGE_POLYGON
    n = len(poly)
    inside = False
    j = n - 1
    for i in range(n):
        lat_i, lon_i = poly[i]
        lat_j, lon_j = poly[j]
        if ((lon_i > lon) != (lon_j > lon)) and \
           (lat < (lat_j - lat_i) * (lon - lon_i) / (lon_j - lon_i) + lat_i):
            inside = not inside
        j = i
    return inside

def fetch_gbfs():
    """Fetch and merge station info + status for Bay Ridge stations."""
    info   = requests.get(INFO_URL,   timeout=15).json()
    status = requests.get(STATUS_URL, timeout=15).json()
    status_map = {s['station_id']: s for s in status['data']['stations']}
    stations = []
    for s in info['data']['stations']:
        lat, lon = s.get('lat', 0), s.get('lon', 0)
        if not in_bay_ridge(lat, lon):
            continue
        st = status_map.get(s['station_id'], {})
        stations.append({
            'station_id':       s['station_id'],
            'station_name':     s['name'],
            'lat':              lat,
            'lon':              lon,
            'bikes_available':  st.get('num_bikes_available', 0),
            'ebikes_available': st.get('num_ebikes_available', 0),
            'docks_available':  st.get('num_docks_available', 0),
            'capacity':         s.get('capacity', 0),
            'is_renting':       bool(st.get('is_renting', 1)),
        })
    return stations

def get_previous_state(supabase: Client) -> dict:
    """
    Pull the most recent snapshot per station from Supabase.
    Returns dict of station_id -> bikes_available.
    This replaces the in-memory state from the old always-on version.
    """
    log.info("Fetching previous state from Supabase...")
    try:
        # Get the most recent captured_at timestamp
        latest = supabase.table('station_snapshots')\
            .select('captured_at')\
            .order('captured_at', desc=True)\
            .limit(1)\
            .execute()

        if not latest.data:
            log.info("No previous state found — first run")
            return {}

        last_ts = latest.data[0]['captured_at']
        log.info(f"Last snapshot: {last_ts}")

        # Get all snapshots from that timestamp
        rows = supabase.table('station_snapshots')\
            .select('station_id, bikes_available')\
            .eq('captured_at', last_ts)\
            .execute()

        state = {r['station_id']: r['bikes_available'] for r in rows.data}
        log.info(f"Loaded previous state for {len(state)} stations")
        return state

    except Exception as e:
        log.error(f"Failed to load previous state: {e}")
        return {}

def write_snapshots(supabase: Client, stations: list, now: datetime):
    rows = [{
        'captured_at':      now.isoformat(),
        'station_id':       s['station_id'],
        'station_name':     s['station_name'],
        'lat':              s['lat'],
        'lon':              s['lon'],
        'bikes_available':  s['bikes_available'],
        'ebikes_available': s['ebikes_available'],
        'docks_available':  s['docks_available'],
        'capacity':         s['capacity'],
        'is_renting':       s['is_renting'],
    } for s in stations]
    supabase.table('station_snapshots').insert(rows).execute()
    log.info(f"Wrote {len(rows)} snapshots")

def infer_and_write_trips(supabase: Client, stations: list, prev_state: dict, now: datetime):
    if not prev_state:
        log.info("No previous state — skipping trip inference on first run")
        return

    trip_rows = []
    total_departed = 0
    total_arrived  = 0
    total_rebalance = 0

    for s in stations:
        sid = s['station_id']
        if sid not in prev_state:
            continue

        curr = s['bikes_available']
        prev = prev_state[sid]
        delta = curr - prev

        if delta == 0:
            continue

        is_rebalance = abs(delta) > REBALANCE_THRESHOLD

        trip_rows.append({
            'detected_at':    now.isoformat(),
            'station_id':     sid,
            'station_name':   s['station_name'],
            'trips_departed': abs(delta) if delta < 0 and not is_rebalance else 0,
            'trips_arrived':  delta       if delta > 0 and not is_rebalance else 0,
            'prev_bikes':     prev,
            'curr_bikes':     curr,
            'delta':          delta,
            'is_rebalance':   is_rebalance,
        })

        if not is_rebalance:
            if delta < 0:
                total_departed += abs(delta)
            else:
                total_arrived += delta
        else:
            total_rebalance += 1

    if trip_rows:
        supabase.table('inferred_trips').insert(trip_rows).execute()
        log.info(f"Trips — departed: {total_departed}, arrived: {total_arrived}, rebalances flagged: {total_rebalance}")
    else:
        log.info("No trip changes detected this poll")

def update_hourly_rollups(supabase: Client, stations: list, now: datetime):
    hour_start = now.replace(minute=0, second=0, microsecond=0).isoformat()
    for s in stations:
        bikes = s['bikes_available']
        try:
            existing = supabase.table('hourly_rollups')\
                .select('id, avg_bikes, min_bikes, max_bikes')\
                .eq('hour_start', hour_start)\
                .eq('station_id', s['station_id'])\
                .execute()

            if existing.data:
                row = existing.data[0]
                supabase.table('hourly_rollups').update({
                    'avg_bikes': round((row['avg_bikes'] + bikes) / 2, 1),
                    'min_bikes': min(row['min_bikes'], bikes),
                    'max_bikes': max(row['max_bikes'], bikes),
                }).eq('id', row['id']).execute()
            else:
                supabase.table('hourly_rollups').insert({
                    'hour_start':     hour_start,
                    'station_id':     s['station_id'],
                    'station_name':   s['station_name'],
                    'total_departed': 0,
                    'total_arrived':  0,
                    'avg_bikes':      float(bikes),
                    'min_bikes':      bikes,
                    'max_bikes':      bikes,
                }).execute()
        except Exception as e:
            log.error(f"Rollup failed for {s['station_name']}: {e}")

def prune_old_snapshots(supabase: Client):
    """Keep only last 48 hours of raw snapshots to control database size."""
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        result = supabase.table('station_snapshots')\
            .delete()\
            .lt('captured_at', cutoff)\
            .execute()
        log.info(f"Pruned old snapshots before {cutoff}")
    except Exception as e:
        log.error(f"Pruning failed: {e}")

# ── MAIN ──────────────────────────────────────────────────

def main():
    log.info("=== Bay Ridge Citi Bike Poller ===")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    now = datetime.now(timezone.utc)

    # 1. Load previous state from Supabase (replaces in-memory state)
    prev_state = get_previous_state(supabase)

    # 2. Fetch current GBFS data
    log.info("Fetching live GBFS data...")
    stations = fetch_gbfs()
    log.info(f"Found {len(stations)} Bay Ridge stations")

    # 3. Write snapshots
    write_snapshots(supabase, stations, now)

    # 4. Infer trips from delta vs previous state
    infer_and_write_trips(supabase, stations, prev_state, now)

    # 5. Update hourly rollups
    update_hourly_rollups(supabase, stations, now)

    # 6. Prune snapshots older than 48 hours
    prune_old_snapshots(supabase)

    log.info("=== Poll complete ===")

if __name__ == '__main__':
    main()
