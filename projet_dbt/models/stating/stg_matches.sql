select *
from {{ source('raw', 'raw_matches') }}
where series in ('Grand Slam', 'Masters 1000', 'ATP500', 'ATP250', 'Masters')
  and date >= '2015-01-01'