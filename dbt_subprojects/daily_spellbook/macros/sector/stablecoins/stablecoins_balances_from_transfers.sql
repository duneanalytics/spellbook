{%- macro stablecoins_balances_from_transfers(transfers, start_date) %}

{% set is_celo = 'celo' in (transfers | string | lower) %}
{% set is_polygon = 'polygon' in (transfers | string | lower) %}

with
{% if is_celo %}
-- Celo L1-era validators received epoch rewards without transfer events
-- these addresses must be excluded from transfer-based balance tracking
-- uses last action before L2 migration to handle register/deregister/re-register cases
celo_l1_validators as (
  select validator_address
  from (
    select
      validator_address,
      action,
      max_by(action, evt_block_time) over (partition by validator_address) as last_action
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
  )
  where action = last_action
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
  where t."from" != t."to"  -- exclude self-transfers (they cancel out but add unnecessary rows)
  {% if is_celo %}
  and not exists (
    select 1
    from celo_l1_validators v
    where t."to" = v.validator_address
  )
  {% endif %}
  {% if is_incremental() %}
  and {{ incremental_predicate('t.block_time') }}
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
  where t."from" != t."to"  -- exclude self-transfers (they cancel out but add unnecessary rows)
  {% if is_celo %}
  and not exists (
    select 1
    from celo_l1_validators v
    where t."from" = v.validator_address
  )
  {% endif %}
  {% if is_incremental() %}
  and {{ incremental_predicate('t.block_time') }}
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
-- get last known balance for each address/token from before the incremental window
prior_balances as (
  select
    blockchain,
    address,
    token_address,
    max(day) as day,
    max_by(last_updated, day) as last_updated,
    max_by(balance_raw, day) as balance_raw
  from {{ this }}
  where not {{ incremental_predicate('day') }}
  group by 1, 2, 3
),

{% endif %}

-- use slightly smaller value for safe double comparison
{% set uint256_max_double = '1.0e77' %}

changed_balances as (
  select
    blockchain,
    day,
    last_updated,
    address,
    token_address,
    balance_raw,
    lead(cast(day as timestamp)) over (
      partition by address, token_address
      order by day
    ) as next_update_day
  from (
    -- addresses with transfers in the incremental window: calculate running balance
    select
      d.blockchain,
      d.day,
      d.last_updated,
      d.address,
      d.token_address,
      cast(greatest(0e0, least({{ uint256_max_double }},
        {% if is_incremental() %}
        coalesce(cast(p.balance_raw as double), 0e0) +
        {% endif %}
        sum(cast(d.daily_inflow as double) - cast(d.daily_outflow as double)) over (
          partition by d.address, d.token_address
          order by d.day
          rows between unbounded preceding and current row
        )
      )) as uint256) as balance_raw
    from daily_aggregated d
    {% if is_incremental() %}
    left join prior_balances p
      on d.address = p.address
      and d.token_address = p.token_address
    {% endif %}
    {% if is_incremental() %}
    union all
    -- include ALL prior balances to anchor forward_fill properly
    -- for addresses WITH activity: bridges gap between prior balance and first activity day
    -- for addresses WITHOUT activity: ensures they're forward-filled through the window
    select
      p.blockchain,
      p.day,
      p.last_updated,
      p.address,
      p.token_address,
      p.balance_raw
    from prior_balances p
    {% endif %}
  )
),

days as (
  select cast(timestamp as date) as day
  from {{ source('utils', 'days') }}
  where cast(timestamp as date) >= cast('{{ start_date }}' as date)
    and cast(timestamp as date) < current_date
  {% if is_incremental() %}
    and {{ incremental_predicate('cast(timestamp as date)') }}
  {% endif %}
),

forward_fill as (
  select
    b.blockchain,
    d.day,
    b.address,
    b.token_address,
    b.balance_raw,
    b.last_updated
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
  or (balance_raw = uint256 '0' and cast(last_updated as date) = day))  -- include actual 0-balance changes, not forward-fills
{% if is_polygon %}
  -- exclude self-holdings on agEUR token contract
  and not (blockchain = 'polygon'
    and token_address = 0xe0b52e49357fd4daf2c15e02058dce6bc0057db4
    and address = token_address)
{% endif %}
  and not (
    (blockchain = 'ethereum'
      and exists (
        select 1
        from {{ ref('labels_burn_addresses') }} b
        where b.blockchain = blockchain and b.address = address
      ))
    or (blockchain != 'ethereum' and address = 0x0000000000000000000000000000000000000000)
  )
{% if is_incremental() %}
  and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}
