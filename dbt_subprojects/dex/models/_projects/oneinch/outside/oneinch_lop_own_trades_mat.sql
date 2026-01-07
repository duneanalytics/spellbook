{{  
    config(
        schema = 'oneinch',
        alias = 'lop_own_trades_mat',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'evt_index'],
        partition_by = ['blockchain', 'block_month'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set src_symbol = "coalesce(src_executed_symbol, '')" %}
{% set dst_symbol = "coalesce(dst_executed_symbol, '')" %}



select
    blockchain
    , '1inch-LOP' as project
    , cast(protocol_version as varchar) as version
    , block_date
    , block_month
    , block_time
    , block_number
    , {{ src_symbol }} as token_bought_symbol
    , {{ dst_symbol }} as token_sold_symbol
    , case
        when lower({{ src_symbol }}) > lower({{ dst_symbol }}) then concat({{ dst_symbol }}, '-', {{ src_symbol }})
        else concat({{ src_symbol }}, '-', {{ dst_symbol }})
    end as token_pair
    , cast(src_executed_amount as double) / pow(10, cast(element_at(complement, 'src_decimals') as bigint)) as token_bought_amount
    , cast(dst_executed_amount as double) / pow(10, cast(element_at(complement, 'dst_decimals') as bigint)) as token_sold_amount
    , src_executed_amount as token_bought_amount_raw
    , dst_executed_amount as token_sold_amount_raw
    , amount_usd
    , src_token_address as token_bought_address
    , dst_token_address as token_sold_address
    , call_from as taker
    , user as maker
    , call_to as project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , row_number() over(partition by tx_hash order by call_trace_address) as evt_index
from {{ ref('oneinch_swaps') }}
where true
    and mode = 'limits'
    and tx_success
    and call_success
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}

