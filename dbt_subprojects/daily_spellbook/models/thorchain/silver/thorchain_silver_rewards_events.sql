{{ config(
    schema = 'thorchain_silver',
    alias = 'rewards_events',
    tags = ['thorchain', 'rewards', 'silver']
) }}

with base as (
SELECT
  bond_e8,
  event_id,
  block_timestamp,
  _updated_at as _inserted_timestamp,
  row_number() over(PARTITION BY event_id ORDER BY _updated_at DESC) as rn
FROM
  {{ source('thorchain', 'rewards_events') }}
)
select *
from base
where rn = 1