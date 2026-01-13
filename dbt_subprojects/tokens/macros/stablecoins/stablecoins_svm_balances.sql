{%- macro stablecoins_svm_balances(
  blockchain,
  token_list,
  start_date
) %}

with stablecoin_tokens as (
  select token_mint_address
  from {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins_' ~ token_list) }}
),

-- get all address/token combinations that have held stablecoins
address_tokens as (
  select distinct
    b.token_balance_owner as address,
    b.token_mint_address,
  from {{ source('solana_utils', 'daily_balances') }} b
  inner join stablecoin_tokens t on b.token_mint_address = t.token_mint_address
  where b.token_balance_owner is not null
    and b.day >= date '{{start_date}}'
),

-- generate day/address/token combinations
days as (
  select cast(timestamp as date) as day
  from {{ source('utils', 'days') }}
  where cast(timestamp as date) >= date '{{start_date}}'
    and cast(timestamp as date) < current_date
  {% if is_incremental() %}
    and {{ incremental_predicate('cast(timestamp as date)') }}
  {% endif %}
),

address_token_days as (
  select
    d.day,
    at.address,
    at.token_mint_address,
  from days d
  cross join address_tokens at
),

-- asof join to get the most recent balance as of each day
forward_fill as (
  select
    atd.day,
    atd.address,
    atd.token_mint_address,
    b.token_balance,
    b.day as last_updated,
  from address_token_days atd
  asof join {{ source('solana_utils', 'daily_balances') }} b
    on atd.address = b.token_balance_owner
    and atd.token_mint_address = b.token_mint_address
    and b.day <= atd.day
)

select
  '{{blockchain}}' as blockchain,
  day,
  address,
  token_mint_address,
  token_balance,
  last_updated,
from forward_fill
where token_balance > 0
{% if is_incremental() %}
  and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}
