{%- macro
    oneinch_project_swaps_second_side_macro(
        blockchain
        , project_swaps_base_table
    )
-%}

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address
    , project
    , tag
    , flags
    , call_selector
    , method
    , call_from
    , call_to
    , tx_from as user
    , order_hash
    , maker
    , maker_asset
    , making_amount
    , taker_asset
    , taking_amount
    , order_flags
    , tokens
    , user_tokens
    , caller_tokens
    , amount_usd
    , user_amount_usd
    , user_amount_usd_trusted
    , caller_amount_usd
    , caller_amount_usd_trusted
    , contract_amount_usd
    , contract_amount_usd_trusted
    , call_amount_usd
    , call_amount_usd_trusted
    , tx_swaps
    , users
    , direct_users
    , senders
    , receivers
    , block_date
    , block_month
    , call_trade_id
    , false as intent
    , false as entry
    , true as second_side
    , contracts_only
    , map_from_entries(array[
          ('intra-chain: classic: direct', true)
        , ('intra-chain: classic: external', false)
        , ('intra-chain: intents: auction', false)
        , ('intra-chain: intents: user limit order', false)
        , ('intra-chain: intents: contracts only', false)
        , ('cross-chain', false)
    ]) as modes
    , 1 as modes_count
    , 'intra-chain: classic: direct' as mode
from {{ project_swaps_base_table }}
where true
    and flags['direct']
    and order_hash is not null -- intent
    and maker is not null
    and not auction
    and not cross_chain
    {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}

{%- endmacro -%}