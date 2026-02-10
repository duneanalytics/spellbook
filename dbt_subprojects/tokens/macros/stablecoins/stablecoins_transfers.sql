{%- macro stablecoins_transfers(
    blockchain,
    token_list
) %}

with stablecoin_tokens as (
    select contract_address as token_address, currency
    from {{ ref('tokens_' ~ blockchain ~ '_erc20_stablecoins_' ~ token_list) }}
)

select
    t.blockchain
    , t.block_month
    , t.block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.evt_index
    , t.trace_address
    , t.token_standard
    , t.contract_address as token_address
    , t.symbol as token_symbol
    , t.amount_raw
    , t.amount
    , coalesce(t.price_usd, fx.exchange_rate) as price_usd
    , coalesce(t.amount_usd, t.amount * fx.exchange_rate) as amount_usd
    , t."from"
    , t."to"
    , t.unique_key
from {{ ref('tokens_' ~ blockchain ~ '_transfers') }} t
inner join stablecoin_tokens s
    on t.contract_address = s.token_address
left join {{ source('prices', 'fx_exchange_rates') }} fx
    on s.currency = fx.base_currency
    and fx.target_currency = 'USD'
    and t.block_date = fx.date
{% if is_incremental() %}
where {{ incremental_predicate('t.block_date') }}
{% endif %}

{% endmacro %}

