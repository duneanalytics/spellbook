{%- macro transfers_from_traces_macro(
    blockchain
    , easy_dates=false
    , prices_interval='hour'
    )
-%}

{%- if blockchain is none or blockchain == '' -%}
    {{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}
/*
    - split into 2 operations and fetching from pre-materialized tables to prevent doubling full-scan of traces
*/
WITH base_traces as (
    SELECT
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
    FROM
        {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base') }}
    where
        1=1
        {% if is_incremental() -%}
        and {{ incremental_predicate('block_date') }}
        {% else -%}
        and block_date >= TIMESTAMP '{{ transfers_start_date }}'
        {% endif -%}
)
, erc20 as (
    SELECT
        blockchain
        , contract_address
        , decimals
        , symbol
    FROM
        {{ source('tokens', 'erc20') }}
    WHERE
        blockchain = '{{ blockchain }}'
)
, prices AS (
    SELECT
        timestamp
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM
        {{ source('prices_external', prices_interval) }}
    where
        blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('timestamp') }}
        {% else %}
        and timestamp >= TIMESTAMP '{{ transfers_start_date }}'
        {% endif %}
)
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
    , symbol
    , amount_raw
    , amount_raw / power(10, coalesce(erc20.decimals, p.decimals)) AS amount
    , p.price AS price_usd
    , amount_raw / power(10, coalesce(erc20.decimals, p.decimals)) * p.price AS amount_usd
    , "from"
    , "to"
    , unique_key
from base_traces as bt
left join erc20
    on erc20.contract_address = bt.contract_address
left join prices as p
    on p.contract_address = bt.contract_address
    and p.timestamp = date_trunc('{{ prices_interval }}', bt.block_time)

/*
--will add this after testing above
union all
-- the wrapper deposit includes two transfers: native and wrapped, so need to add second one manually reversing from/to
select * from {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base_wrapper_deposits') }}
*/
{%- endmacro -%}