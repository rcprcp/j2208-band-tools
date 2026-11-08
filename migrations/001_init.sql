-- J2208 collector schema, version 1.
--
-- Run via tools/j2208_pg_setup.py, which creates the schema and then sets:
--     SET search_path TO <schema>, pg_catalog;
--
-- Unqualified object names below therefore land in the configured schema.

CREATE TABLE IF NOT EXISTS schema_version (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS devices (
    device_id       text PRIMARY KEY,
    advertised_name text,
    model           text,
    firmware        text,
    first_seen      timestamptz NOT NULL DEFAULT now(),
    last_seen       timestamptz NOT NULL DEFAULT now()
);

-- One row per decoded history record. The band's timestamps are naive local
-- time, so device_time is timestamp (not timestamptz), while fetched_at is UTC.
CREATE TABLE IF NOT EXISTS history (
    device_id   text NOT NULL REFERENCES devices(device_id),
    type        text NOT NULL,
    device_time timestamp NOT NULL,
    value       double precision,
    payload     jsonb NOT NULL,
    raw         bytea,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, type, device_time)
);

CREATE INDEX IF NOT EXISTS history_device_time_idx
    ON history (device_id, device_time DESC);

CREATE INDEX IF NOT EXISTS history_payload_gin
    ON history USING gin (payload);

CREATE TABLE IF NOT EXISTS live (
    device_id text NOT NULL REFERENCES devices(device_id),
    ts        timestamptz NOT NULL,
    hr        smallint,
    spo2      smallint,
    hrv       smallint,
    stress    smallint,
    bp_sys    smallint,
    bp_dia    smallint,
    temp_c    real,
    PRIMARY KEY (device_id, ts)
);

CREATE TABLE IF NOT EXISTS battery (
    device_id text NOT NULL REFERENCES devices(device_id),
    ts        timestamptz NOT NULL,
    pct       smallint NOT NULL CHECK (pct BETWEEN 0 AND 100),
    mode      text NOT NULL,
    PRIMARY KEY (device_id, ts)
);

CREATE TABLE IF NOT EXISTS events (
    id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id text REFERENCES devices(device_id) ON DELETE SET NULL,
    ts        timestamptz NOT NULL DEFAULT now(),
    kind      text NOT NULL,
    detail    text
);

CREATE INDEX IF NOT EXISTS events_ts_idx ON events (ts DESC);

-- Typed projections over the lossless JSONB history table. Add more views as
-- new metrics are decoded; the raw row remains available in history.payload.
CREATE OR REPLACE VIEW v_hr AS
SELECT device_id, device_time,
       (NULLIF(payload->>'value', ''))::integer AS bpm,
       fetched_at
FROM history
WHERE type = 'hr';

CREATE OR REPLACE VIEW v_spo2 AS
SELECT device_id, device_time,
       (NULLIF(payload->>'value', ''))::integer AS percent,
       fetched_at
FROM history
WHERE type = 'spo2_auto';

CREATE OR REPLACE VIEW v_temp AS
SELECT device_id, device_time,
       (NULLIF(payload->>'value', ''))::numeric AS temp_c,
       fetched_at
FROM history
WHERE type = 'temp_axil';

CREATE OR REPLACE VIEW v_hrv AS
SELECT device_id, device_time,
       (NULLIF(payload->>'hrv', ''))::integer    AS hrv,
       (NULLIF(payload->>'hr', ''))::integer     AS hr,
       (NULLIF(payload->>'stress', ''))::integer AS stress,
       (NULLIF(payload->>'bp_sys', ''))::integer AS bp_sys,
       (NULLIF(payload->>'bp_dia', ''))::integer AS bp_dia,
       fetched_at
FROM history
WHERE type = 'hrv';

CREATE OR REPLACE VIEW v_steps AS
SELECT device_id, device_time,
       (NULLIF(payload->>'steps', ''))::integer         AS steps,
       (NULLIF(payload->>'calories_kcal', ''))::numeric AS calories_kcal,
       (NULLIF(payload->>'distance_km', ''))::numeric   AS distance_km,
       payload->'step_series'                           AS step_series,
       fetched_at
FROM history
WHERE type = 'steps';

CREATE OR REPLACE VIEW v_sleep AS
SELECT device_id, device_time,
       (NULLIF(payload->>'deep_min', ''))::integer  AS deep_min,
       (NULLIF(payload->>'light_min', ''))::integer AS light_min,
       (NULLIF(payload->>'rem_min', ''))::integer   AS rem_min,
       (NULLIF(payload->>'awake_min', ''))::integer AS awake_min,
       (NULLIF(payload->>'count', ''))::integer     AS count,
       payload->'stages'                            AS stages,
       fetched_at
FROM history
WHERE type = 'sleep';

CREATE OR REPLACE VIEW v_total AS
SELECT device_id, device_time,
       (NULLIF(payload->>'steps', ''))::bigint           AS steps,
       (NULLIF(payload->>'distance_km', ''))::numeric    AS distance_km,
       (NULLIF(payload->>'calories_kcal', ''))::numeric  AS calories_kcal,
       (NULLIF(payload->>'active_minutes', ''))::integer AS active_minutes,
       fetched_at
FROM history
WHERE type = 'total';
