{%- macro
    transfers_from_traces_macro(
        blockchain,
        transfers_start_date='2000-01-01',
        easy_dates=false,
        prices_interval='hour'
    )
-%}

{%- if blockchain is none or blockchain == '' -%}
    {{- exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") -}}
{%- endif -%}



with

base_tft as (
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
        , date_trunc('{{ prices_interval }}', block_time) as timestamp
    from {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base') }}
    where true
        and block_date >= timestamp '{{ transfers_start_date }}'
        {% if easy_dates -%} and block_date > current_date - interval '10' day {%- endif %} -- easy_dates mode for dev, to prevent full scan
        {% if is_incremental() -%} and {{ incremental_predicate('block_date') }} {%- endif %}
)

, base_tft_wrapper_deposits as (
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
        , date_trunc('{{ prices_interval }}', block_time) as timestamp
    from {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base_wrapper_deposits') }}
    where true
        and block_date >= timestamp '{{ transfers_start_date }}'
        {% if easy_dates -%} and block_date > current_date - interval '10' day {%- endif %} -- easy_dates mode for dev, to prevent full scan
        {% if is_incremental() -%} and {{ incremental_predicate('block_date') }} {%- endif %}
)

, tft as (
    select * from base_tft
    union all
    select * from base_tft_wrapper_deposits
)

, tokens as (
    select
        contract_address
        , decimals as token_decimals
        , symbol as token_symbol
    from {{ source('tokens', 'erc20') }}
    where true
        and blockchain = '{{ blockchain }}'
)

, prices as (
    select
        contract_address
        , timestamp
        , decimals
        , symbol
        , price
    from {{ source('prices_external', prices_interval) }}
    where true
        and blockchain = '{{ blockchain }}'
        and timestamp >= timestamp '{{ transfers_start_date }}'
        {% if easy_dates -%} and timestamp > current_date - interval '10' day {%- endif %} -- easy_dates mode for dev, to prevent full scan
        {% if is_incremental() -%} and {{ incremental_predicate('timestamp') }} {%- endif %}
)

-- output --

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
    , coalesce(token_symbol, symbol) as symbol
    , amount_raw
    , amount_raw / power(10, coalesce(token_decimals, decimals)) as amount
    , price as price_usd
    , amount_raw / power(10, coalesce(token_decimals, decimals)) * price as amount_usd
    , "from"
    , "to"
    , unique_key
from tft
left join tokens using(contract_address)
left join prices using(contract_address, timestamp)



{%- endmacro -%}