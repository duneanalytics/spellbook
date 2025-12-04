{% macro stablecoins_balances(blockchain, start_date, filter_tokens = none) %}

with

stablecoin_tokens as (
  select
    st.symbol,
    st.contract_address as token_address
  from {{ source('tokens_' ~ blockchain, 'erc20_stablecoins') }} st
  {% if filter_tokens %}
  inner join ({{ filter_tokens }}) ft on st.contract_address = ft.contract_address
  {% endif %}
),

balances as (
  {{
    balances_incremental_subset_daily(
      blockchain = blockchain,
      token_list = 'stablecoin_tokens',
      start_date = start_date,
    )
  }}
)

select
  t.symbol,
  b.*
from balances b
  left join stablecoin_tokens t on b.token_address = t.token_address

{% endmacro %}
