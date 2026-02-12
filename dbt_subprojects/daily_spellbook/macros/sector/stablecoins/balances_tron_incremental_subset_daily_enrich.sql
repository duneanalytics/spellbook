{%- macro balances_tron_incremental_subset_daily_enrich(
        base_balances,
        chain,
        token_list
    )
-%}
-- Tron-specific enrich: passes through contract_address from base balances.

with

base as (
  select *
  from {{ base_balances }}
  {% if is_incremental() %}
  where {{ incremental_predicate('day') }}
  {% endif %}
),

tokens_metadata as (
  select
    blockchain,
    contract_address,
    symbol,
    decimals
  from {{ source('tokens', 'erc20') }}
),

stablecoin_tokens as (
  select blockchain, contract_address, currency
  from {{ source('tokens_' ~ chain, 'erc20_stablecoins_' ~ token_list) }}
),

enriched_with_tokens as (
  select
    b.blockchain,
    b.day,
    b.address,
    b.token_address,
    b.contract_address,
    b.token_standard,
    b.token_id,
    b.balance_raw,
    b.last_updated,
    case
      when b.token_standard = 'erc20' then t.symbol
      else null
    end as token_symbol,
    case
      when b.token_standard = 'erc20' then b.balance_raw / power(10, t.decimals)
      when b.token_standard = 'native' then b.balance_raw / power(10, 18)
      else b.balance_raw
    end as balance,
    s.currency
  from base b
  left join tokens_metadata t
    on t.blockchain = b.blockchain
    and t.contract_address = b.token_address
    and b.token_standard = 'erc20'
  left join stablecoin_tokens s
    on s.blockchain = b.blockchain
    and s.contract_address = b.token_address
)

select
  e.blockchain,
  e.day,
  e.address,
  e.token_symbol,
  e.token_address,
  e.contract_address,
  e.token_standard,
  e.token_id,
  e.balance_raw,
  e.balance,
  e.balance * fx.exchange_rate as balance_usd,
  e.currency,
  e.last_updated
from enriched_with_tokens e
left join {{ source('prices', 'fx_exchange_rates') }} fx
  on e.currency = fx.base_currency
  and fx.target_currency = 'USD'
  and e.day = fx.date
  {% if is_incremental() %}
  and {{ incremental_predicate('fx.date') }}
  {% endif %}

{%- endmacro %}
