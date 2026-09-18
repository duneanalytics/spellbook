with expected as (
  select blockchain, address, first_funded_by, first_funded_at
  from {{ ref('arc_first_funding_seed') }}
)

select e.address
from expected as e
left join {{ ref('addresses_events_arc_first_funded_by') }} as f
  on f.blockchain = e.blockchain and f.address = e.address
where f.address is null
  or f.first_funded_by is distinct from e.first_funded_by
  or f.block_time is distinct from e.first_funded_at
