select *
from {{ source('raw', 'raw_matches') }}
where series in ('Grand Slam', 'Masters 1000', 'ATP 500', 'ATP 250')
  and date >= '2015-01-01'