{{
    config(
        schema = 'oneinch',
        alias = 'lop_aggregator_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'trace_address', 'evt_index'],
    )
}}

{% set src_symbol = "coalesce(src_executed_symbol, '')" %}
{% set dst_symbol = "coalesce(dst_executed_symbol, '')" %}



with fills as (
    -- same numbering as oneinch_lop_own_trades: over ALL limits fills, before the split
    select *
        , row_number() over(partition by tx_hash order by call_trace_address) as evt_index
    from {{ ref('oneinch_swaps') }}
    where true
        and mode = 'limits'
        and tx_success
        and call_success
)

-- venue-settled LOP fills: the underlying venue's own row stays in dex.trades, so the
-- intent-layer fill is recorded here instead (user perspective, like oneinch_ar_trades)
select
    blockchain
    , '1inch-LOP' as project
    , cast(protocol_version as varchar) as version
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
    , evt_index
from fills as f
where true
    and exists (
        select 1
        from {{ ref('oneinch_lop_venue_settled_fills') }} as v
        where true
            and v.blockchain = f.blockchain
            and v.block_month = f.block_month
            and v.execution_id = f.execution_id
    )
