{%- macro transfers_from_traces_macro(
    blockchain
    , transfers_start_date = '2000-01-01'
    , easy_dates=true
    , prices_interval='hour'
    )
-%}

{%- if blockchain is none or blockchain == '' -%}
    {{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}

with base_traces as (
    select
        blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , trace_address
        , type
        , token_standard
        , contract_address
        , amount_raw
        , "from"
        , "to"
        , unique_key
    from
        {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base') }}
    where
        1=1
        {% if easy_dates -%} and block_date > current_date - interval '10' day {%- endif %} -- easy_dates mode for dev, to prevent full scan
        {% if is_incremental() -%}
        and {{ incremental_predicate('block_date') }}
        {% else -%}
        and block_date >= timestamp '{{ transfers_start_date }}'
        {% endif -%}
)
, base_traces_wrapper_deposits as (
    select
        blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , trace_address
        , type
        , token_standard
        , contract_address
        , amount_raw
        , "from"
        , "to"
        , unique_key
    from
        {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base_wrapper_deposits') }}
    where
        1=1
        {% if easy_dates -%} and block_date > current_date - interval '10' day {%- endif %} -- easy_dates mode for dev, to prevent full scan
        {% if is_incremental() -%}
        and {{ incremental_predicate('block_date') }}
        {% else -%}
        and block_date >= timestamp '{{ transfers_start_date }}'
        {% endif -%}
)
, union_traces as (
    select * from base_traces
    union all
    select * from base_traces_wrapper_deposits
)
, erc20 as (
    select
        blockchain
        , contract_address
        , decimals
        , symbol
    from
        {{ source('tokens', 'erc20') }}
    where
        blockchain = '{{ blockchain }}'
)
, prices as (
    select
        timestamp
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    from
        {{ source('prices_external', prices_interval) }}
    where
        blockchain = '{{ blockchain }}'
        {% if easy_dates -%} and timestamp > current_date - interval '10' day {%- endif %} -- easy_dates mode for dev, to prevent full scan
        {% if is_incremental() %}
        and {{ incremental_predicate('timestamp') }}
        {% else %}
        and timestamp >= timestamp '{{ transfers_start_date }}'
        {% endif %}
)
select
    t.blockchain
    , t.block_month
    , t.block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.trace_address
    , t.type
    , t.token_standard
    , t.contract_address
    , erc20.symbol
    , t.amount_raw
    , t.amount_raw / power(10, coalesce(erc20.decimals, p.decimals)) as amount
    , p.price as price_usd
    , t.amount_raw / power(10, coalesce(erc20.decimals, p.decimals)) * p.price as amount_usd
    , t."from"
    , t."to"
    , t.unique_key
from union_traces as t
left join erc20
    on erc20.contract_address = t.contract_address
left join prices as p
    on p.contract_address = t.contract_address
    and p.timestamp = date_trunc('{{ prices_interval }}', t.block_time)
{%- endmacro -%}