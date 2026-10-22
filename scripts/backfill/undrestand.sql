station_status_changes
-- scan snapshots,
-- add if a station is flagged: its empty, full or 
-- scan previous row to see if station has n_bikes = 0, or n_docks = 0 and current are not 0
-- or if current = 0 and previous is not
-- cte that reads lag
insert into station_status_changes 
    (station_id, change_type, date, value_before, value_after, snapshot_id)

with lagged( select 
stations_id
, captured_at
, snapshot_id
curren biles/docks, is renting, is returning
lag(num_bikes) over w as previous_n_bikes,
lag(num_docks) over w as previous_docks

from snapshots
window w as (PARTITION by station_id order by captured_at, snapshot_id))

select station_id, 'became_empty', prev_n_bikes, n_bikes, snapshot_id
from lagged 
where prev_bikes > 0, n_bikes = 0

