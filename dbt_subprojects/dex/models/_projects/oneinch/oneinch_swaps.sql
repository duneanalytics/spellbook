{% set exposed = oneinch_meta_cfg_macro(property = 'blockchains').exposed.keys() %}

{{
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'view',
        post_hook = '{{ expose_spells(exposed, "project", "oneinch", ["max-morrow", "grkhr"]) }}'
    )
}}



select
    blockchain
    , chain_id
    , dst_blockchain
    , order_hash
    , user
    , max(mode) as mode
    , max(protocol) as protocol
    , max(protocol_version) as protocol_version
    , max(contract_address) as contract_address
    , max(contract_name) as contract_name

    , max_by(block_number, amount_usd) as main_block_number
    , max_by(block_time, amount_usd) as main_block_time
    , max_by(tx_hash, amount_usd) as main_tx_hash
    , max_by(tx_from, amount_usd) as main_tx_from
    , max_by(tx_to, amount_usd) as main_tx_to
    
    , sum(amount_usd) as amount_usd
    , sum(execution_cost) as execution_cost
    
    , max(coalesce(src_executed_address, src_token_address)) as src_token_address
    , max(src_token_amount) as src_token_amount
    , max(coalesce(src_executed_symbol, '')) as src_token_symbol
    , sum(src_executed_amount) as src_executed_amount
    , sum(src_executed_amount_usd) as src_executed_amount_usd

    , max(coalesce(dst_executed_address, dst_token_address)) as dst_token_address
    , max(dst_token_amount) as dst_token_amount
    , max(coalesce(dst_executed_symbol, '')) as dst_token_symbol
    , sum(dst_executed_amount) as dst_executed_amount
    , sum(dst_executed_amount_usd) as dst_executed_amount_usd

    , max_by(remains, amount_usd) as remains
    , max_by(flags, amount_usd) as flags
    
    , array_agg(cast(
        row(
            id
            , hashlock
            , receiver
            , call_method
            , call_selector
            , amount_usd
        ) as row(
            id number
            , hashlock varbinary
            , receiver varbinary
            , call_method varchar
            , call_selector varbinary
            , amount_usd double
        )
    )) as executions
from {{ ref('oneinch_executions') }}
where true
    and tx_success
    and call_success
group by 1, 2, 3, 4, 5