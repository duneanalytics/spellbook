{{  
    config(
        schema = 'oneinch_evms',
        alias = 'aggregation_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'trace_address'],
    )
}}

{% set src_symbol = "coalesce(src_executed_symbol, '')" %}
{% set dst_symbol = "coalesce(dst_executed_symbol, '')" %}



select
    blockchain
    , '1inch' as project
    , 'AR v' || protocol_version as version
    , block_date
    , block_month
    , block_time
    , {{ dst_symbol }} as token_bought_symbol
    , {{ src_symbol }} as token_sold_symbol
    , case
        when lower({{ src_symbol }}) > lower({{ dst_symbol }}) then concat({{ dst_symbol }}, '-', {{ src_symbol }})
        else concat({{ src_symbol }}, '-', {{ dst_symbol }})
    end as token_pair
    , cast(dst_executed_amount as double) / pow(10, cast(element_at(complement, 'dst_decimals') as bigint)) as token_bought_amount
    , cast(src_executed_amount as double) / pow(10, cast(element_at(complement, 'src_decimals') as bigint)) as token_sold_amount
    , dst_executed_amount as token_bought_amount_raw
    , src_executed_amount as token_sold_amount_raw
    , amount_usd
    , dst_token_address as token_bought_address
    , src_token_address as token_sold_address
    , user as taker
    , cast(null as varbinary) as maker
    , call_to as project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address as trace_address
    , -1 as evt_index
from {{ ref('oneinch_executions') }}
where true
    and mode in ('classic', 'fusion')