print("POLLER VERSION: REST API — no supabase client")

"""
Bay Ridge Citi Bike — Stateless Polling Service
================================================
Uses Supabase REST API directly (no supabase client library).
Runs as a GitHub Actions cron job every 5 minutes.
"""

import os, logging, requests
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────

SUPABASE_URL = os.environ['SUPABASE_URL'].rstrip('/')
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']

INFO_URL   = 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json'
STATUS_URL = 'https://gbfs.citibikenyc.com/gbfs/en/station_status.json'

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

# ── SUPABASE REST HELPERS ─────────────────────────────────

def sb_headers():
    return {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }

def sb_get(table, params=None):
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/{table}',
        headers={**sb_headers(), 'Prefer': 'return=representation'},
        params=params,
        timeout=15
    )
    r.raise_for_status()
    return r.json()

def sb_post(table, data):
    r = requests.post(
        f'{SUPABASE_URL}/rest/v1/{table}',
        headers=sb_headers(),
        json=data,
        timeout=15
    )
    r.raise_for_status()
    return r

def sb_patch(table, data, match_params):
    r = requests.patch(
        f'{SUPABASE_URL}/rest/v1/{table}',
        headers=sb_headers(),
        params=match_params,
        json=data,
        timeout=15
    )
    r.raise_for_status()
    return r

def sb_delete(table, params):
    r = requests.delete(
        f'{SUPABASE_URL}/rest/v1/{table}',
        headers=sb_headers(),
        params=params,
        timeout=15
    )
    r.raise_for_status()
    return r

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

def get_previous_state():
    """Pull most recent snapshot per station from Supabase."""
    try:
        # Get the latest timestamp
        rows = sb_get('station_snapshots', {
            'select': 'captured_at',
            'order':  'captured_at.desc',
            'limit':  '1'
        })
        if not rows:
            log.info("No previous state found — first run")
            return {}
        last_ts = rows[0]['captured_at']
        log.info(f"Last snapshot: {last_ts}")
        # Get all snapshots from that timestamp
        rows = sb_get('station_snapshots', {
            'select':      'station_id,bikes_available',
            'captured_at': f'eq.{last_ts}'
        })
        state = {r['station_id']: r['bikes_available'] for r in rows}
        log.info(f"Loaded previous state for {len(state)} stations")
        return state
    except Exception as e:
        log.error(f"Failed to load previous state: {e}")
        return {}

def write_snapshots(stations, now):
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
    sb_post('station_snapshots', rows)
    log.info(f"Wrote {len(rows)} snapshots")

def infer_and_write_trips(stations, prev_state, now):
    if not prev_state:
        log.info("No previous state — skipping trip inference on first run")
        return
    trip_rows = []
    total_dep = total_arr = total_rebal = 0
    for s in stations:
        sid = s['station_id']
        if sid not in prev_state:
            continue
        curr, prev = s['bikes_available'], prev_state[sid]
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
            if delta < 0: total_dep += abs(delta)
            else:         total_arr += delta
        else:
            total_rebal += 1
    if trip_rows:
        sb_post('inferred_trips', trip_rows)
        log.info(f"Trips — departed: {total_dep}, arrived: {total_arr}, rebalances: {total_rebal}")
    else:
        log.info("No trip changes this poll")

def update_hourly_rollups(stations, now):
    hour_start = now.replace(minute=0, second=0, microsecond=0).isoformat()
    for s in stations:
        bikes = s['bikes_available']
        try:
            existing = sb_get('hourly_rollups', {
                'select':      'id,avg_bikes,min_bikes,max_bikes',
                'hour_start':  f'eq.{hour_start}',
                'station_id':  f'eq.{s["station_id"]}'
            })
            if existing:
                row = existing[0]
                sb_patch('hourly_rollups', {
                    'avg_bikes': round((row['avg_bikes'] + bikes) / 2, 1),
                    'min_bikes': min(row['min_bikes'], bikes),
                    'max_bikes': max(row['max_bikes'], bikes),
                }, {'id': f'eq.{row["id"]}'})
            else:
                sb_post('hourly_rollups', {
                    'hour_start':     hour_start,
                    'station_id':     s['station_id'],
                    'station_name':   s['station_name'],
                    'total_departed': 0,
                    'total_arrived':  0,
                    'avg_bikes':      float(bikes),
                    'min_bikes':      bikes,
                    'max_bikes':      bikes,
                })
        except Exception as e:
            log.error(f"Rollup failed for {s['station_name']}: {e}")

def prune_old_snapshots(now):
    cutoff = (now - timedelta(hours=48)).isoformat()
    try:
        sb_delete('station_snapshots', {'captured_at': f'lt.{cutoff}'})
        log.info(f"Pruned snapshots older than 48h")
    except Exception as e:
        log.error(f"Pruning failed: {e}")

# ── MAIN ──────────────────────────────────────────────────

def main():
    log.info("=== Bay Ridge Citi Bike Poller ===")
    now = datetime.now(timezone.utc)

    prev_state = get_previous_state()

    log.info("Fetching GBFS...")
    stations = fetch_gbfs()
    log.info(f"Found {len(stations)} Bay Ridge stations")

    write_snapshots(stations, now)
    infer_and_write_trips(stations, prev_state, now)
    update_hourly_rollups(stations, now)
    prune_old_snapshots(now)

    log.info("=== Poll complete ===")

if __name__ == '__main__':
    main()
