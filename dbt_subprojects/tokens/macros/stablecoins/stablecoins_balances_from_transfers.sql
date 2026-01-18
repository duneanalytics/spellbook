{%- macro stablecoins_balances_from_transfers(transfers, start_date) %}

{% set is_celo = 'celo' in (transfers | string | lower) %}

with
{% if is_celo %}
-- Celo L1-era validators received epoch rewards without transfer events
-- these addresses must be excluded from transfer-based balance tracking
-- uses last action before L2 migration to handle register/deregister/re-register cases
celo_l1_validators as (
  select validator_address
  from (
    select
      validator as validator_address,
      evt_block_time,
      'register' as action
    from {{ source('celo_core_celo', 'validators_evt_validatorregistered') }}
    where evt_block_time < timestamp '2025-03-26'
    union all
    select
      validator as validator_address,
      evt_block_time,
      'deregister' as action
    from {{ source('celo_core_celo', 'validators_evt_validatorderegistered') }}
    where evt_block_time < timestamp '2025-03-26'
  )
  where action = max_by(action, evt_block_time) over (partition by validator_address)
    and action = 'register'
),
{% endif %}

transfers_in as (
  select
    t.blockchain,
    t.block_date as day,
    t.block_time,
    t."to" as address,
    t.token_address,
    t.amount_raw as inflow,
    uint256 '0' as outflow
  from {{ transfers }} t
  {% if is_celo %}
  left join celo_l1_validators v on t."to" = v.validator_address
  where v.validator_address is null  -- exclude L1 validators
  {% endif %}
  {% if is_incremental() %}
  {% if is_celo %}and{% else %}where{% endif %} {{ incremental_predicate('t.block_time') }}
  {% endif %}
),

transfers_out as (
  select
    t.blockchain,
    t.block_date as day,
    t.block_time,
    t."from" as address,
    t.token_address,
    uint256 '0' as inflow,
    t.amount_raw as outflow
  from {{ transfers }} t
  {% if is_celo %}
  left join celo_l1_validators v on t."from" = v.validator_address
  where v.validator_address is null  -- exclude L1 validators
  {% endif %}
  {% if is_incremental() %}
  {% if is_celo %}and{% else %}where{% endif %} {{ incremental_predicate('t.block_time') }}
  {% endif %}
),

all_flows as (
  select * from transfers_in
  union all
  select * from transfers_out
),

daily_aggregated as (
  select
    blockchain,
    day,
    max(block_time) as last_updated,
    address,
    token_address,
    sum(inflow) as daily_inflow,
    sum(outflow) as daily_outflow
  from all_flows
  group by 1, 2, 4, 5
),

{% if is_incremental() %}
prior_balances as (
  select
    address,
    token_address,
    max_by(balance_raw, day) as prior_balance
  from {{ this }}
  where not {{ incremental_predicate('last_updated') }}
  group by 1, 2
),
{% endif %}

-- use slightly smaller value for safe double comparison
{% set uint256_max_double = '1.0e77' %}

changed_balances as (
  select
    d.blockchain,
    d.day,
    d.last_updated,
    d.address,
    d.token_address,
    cast(greatest(0e0, least({{ uint256_max_double }},
      {% if is_incremental() %}
      coalesce(cast(p.prior_balance as double), 0e0) +
      {% endif %}
      sum(cast(d.daily_inflow as double) - cast(d.daily_outflow as double)) over (
        partition by d.address, d.token_address
        order by d.day
        rows between unbounded preceding and current row
      )
    )) as uint256) as balance_raw,
    lead(cast(d.day as timestamp)) over (
      partition by d.address, d.token_address
      order by d.day
    ) as next_update_day
  from daily_aggregated d
  {% if is_incremental() %}
  left join prior_balances p
    on d.address = p.address
    and d.token_address = p.token_address
  {% endif %}
),

days as (
  select cast(timestamp as date) as day
  from {{ source('utils', 'days') }}
  where cast(timestamp as date) >= cast('{{ start_date }}' as date)
    and cast(timestamp as date) < current_date
),

forward_fill as (
  select
    b.blockchain,
    d.day,
    b.address,
    b.token_address,
    b.balance_raw,
    b.day as last_updated
  from days d
  left join changed_balances b
    on d.day >= b.day
    and (b.next_update_day is null or cast(d.day as timestamp) < b.next_update_day)
)

select
  blockchain,
  day,
  address,
  token_address,
  'erc20' as token_standard,
  cast(null as uint256) as token_id,
  balance_raw,
  last_updated
from forward_fill
where (balance_raw > uint256 '0'
  or (balance_raw = uint256 '0' and last_updated = day))  -- include actual 0-balance changes, not forward-fills
{% if is_incremental() %}
  and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}
