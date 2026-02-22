-- Bay Ridge Citi Bike — Supabase Schema
-- Run this in the Supabase SQL editor

-- Station snapshots — one row per station per poll
CREATE TABLE IF NOT EXISTS station_snapshots (
  id              BIGSERIAL PRIMARY KEY,
  captured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  station_id      TEXT NOT NULL,
  station_name    TEXT NOT NULL,
  lat             DOUBLE PRECISION,
  lon             DOUBLE PRECISION,
  bikes_available INTEGER NOT NULL DEFAULT 0,
  ebikes_available INTEGER NOT NULL DEFAULT 0,
  docks_available INTEGER NOT NULL DEFAULT 0,
  capacity        INTEGER NOT NULL DEFAULT 0,
  is_renting      BOOLEAN NOT NULL DEFAULT TRUE
);

-- Index for fast recent queries
CREATE INDEX idx_snapshots_captured_at ON station_snapshots(captured_at DESC);
CREATE INDEX idx_snapshots_station_id  ON station_snapshots(station_id, captured_at DESC);

-- Inferred trips — computed from consecutive snapshots
CREATE TABLE IF NOT EXISTS inferred_trips (
  id              BIGSERIAL PRIMARY KEY,
  detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  station_id      TEXT NOT NULL,
  station_name    TEXT NOT NULL,
  trips_departed  INTEGER NOT NULL DEFAULT 0,
  trips_arrived   INTEGER NOT NULL DEFAULT 0,
  prev_bikes      INTEGER,
  curr_bikes      INTEGER,
  delta           INTEGER,  -- negative = departures, positive = arrivals
  is_rebalance    BOOLEAN DEFAULT FALSE  -- flagged if delta too large to be organic
);

CREATE INDEX idx_trips_detected_at ON inferred_trips(detected_at DESC);
CREATE INDEX idx_trips_station_id  ON inferred_trips(station_id, detected_at DESC);

-- Hourly rollups — pre-aggregated for fast dashboard queries
CREATE TABLE IF NOT EXISTS hourly_rollups (
  id              BIGSERIAL PRIMARY KEY,
  hour_start      TIMESTAMPTZ NOT NULL,
  station_id      TEXT NOT NULL,
  station_name    TEXT NOT NULL,
  total_departed  INTEGER DEFAULT 0,
  total_arrived   INTEGER DEFAULT 0,
  avg_bikes       DOUBLE PRECISION,
  min_bikes       INTEGER,
  max_bikes       INTEGER,
  UNIQUE(hour_start, station_id)
);

CREATE INDEX idx_rollups_hour_start ON hourly_rollups(hour_start DESC);

-- Enable Row Level Security but allow public read
ALTER TABLE station_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE inferred_trips    ENABLE ROW LEVEL SECURITY;
ALTER TABLE hourly_rollups    ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read snapshots" ON station_snapshots FOR SELECT USING (true);
CREATE POLICY "Public read trips"     ON inferred_trips    FOR SELECT USING (true);
CREATE POLICY "Public read rollups"   ON hourly_rollups    FOR SELECT USING (true);

-- Service role writes (our poller uses the service key)
CREATE POLICY "Service write snapshots" ON station_snapshots FOR INSERT WITH CHECK (true);
CREATE POLICY "Service write trips"     ON inferred_trips    FOR INSERT WITH CHECK (true);
CREATE POLICY "Service write rollups"   ON hourly_rollups    FOR INSERT WITH CHECK (true);
CREATE POLICY "Service write rollups update" ON hourly_rollups FOR UPDATE USING (true);
