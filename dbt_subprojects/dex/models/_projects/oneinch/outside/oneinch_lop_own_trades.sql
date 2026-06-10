{{
    config(
        schema = 'oneinch',
        alias = 'lop_own_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'evt_index'],
    )
}}

{% set src_symbol = "coalesce(src_executed_symbol, '')" %}
{% set dst_symbol = "coalesce(dst_executed_symbol, '')" %}
{% set placeholder_tokens = oneinch_cross_chain_placeholder_tokens_cfg_macro() | join(', ') %}



with fills as (
    -- evt_index is numbered over ALL limits fills, before the venue-settled exclusion:
    -- kept rows must retain their historical evt_index because dex_<blockchain>_trades
    -- merges can only upsert, never delete
    select *
        , row_number() over(partition by tx_hash order by call_trace_address) as evt_index
    from {{ ref('oneinch_swaps') }}
    where true
        and mode = 'limits'
        and tx_success
        and call_success
        -- exclude Fusion+ cross-chain fills: the ERC20True placeholder leg makes these degenerate single-chain rows
        and (src_token_address is null or src_token_address not in ({{ placeholder_tokens }}))
        and (dst_token_address is null or dst_token_address not in ({{ placeholder_tokens }}))
)

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
    , evt_index
from fills as f
where true
    and not exists ( -- venue-settled fills are reclassified into dex_aggregator.trades (see oneinch_lop_aggregator_trades)
        select 1
        from {{ ref('oneinch_lop_venue_settled_fills') }} as v
        where true
            and v.blockchain = f.blockchain
            and v.block_month = f.block_month
            and v.execution_id = f.execution_id
    )
    -- maturity delay: the 1inch lineage builds from raw traces while venue base trades
    -- build from decoded events; a fill merged into dex_<blockchain>_trades before its
    -- venue's event decodes would never be removed (merge can't delete), so fills only
    -- pass through once old enough for the venue side to have landed
    and f.block_time <= now() - interval '6' hour
