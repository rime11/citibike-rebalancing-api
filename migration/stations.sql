INSERT INTO stations (station_id, station_name, short_name, latitude, longitude, capacity)
VALUES (...)
ON CONFLICT (station_id) DO UPDATE SET
    station_name = EXCLUDED.station_name,
    short_name   = EXCLUDED.short_name,
    latitude     = EXCLUDED.latitude,
    longitude    = EXCLUDED.longitude,
    capacity     = EXCLUDED.capacity;