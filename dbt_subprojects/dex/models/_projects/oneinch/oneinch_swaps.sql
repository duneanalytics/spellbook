{%- set exposed = oneinch_meta_cfg_macro()['blockchains']['exposed'] -%}

{{-
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['blockchain', 'block_month'],
        unique_key = ['blockchain', 'block_month', 'mode', 'swap_id'],
        post_hook = '{{ expose_spells(
            blockchains = \'exposed\',
            spell_type = "project",
            spell_name = "oneinch",
            contributors = \'["max-morrow"]\'
        ) }}',
    )
-}}



with

incremental as (
    {% for stream, stream_data in oneinch_meta_cfg_macro()['streams'].items() if stream != 'ar' %}
        select order_hash
        from {{ ref('oneinch_' + stream) }}
        where {{ incremental_predicate('block_time') }}
        group by 1
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

-- output --

select
    blockchain
    , mode
    , coalesce(order_hash, execution_id) as swap_id

    , max(dst_blockchain) as dst_blockchain
    , min_by(user, block_time) as user
    , min_by(block_number, block_time) as block_number
    , min_by(block_month, block_time) as block_month
    , min_by(block_date, block_time) as block_date
    , min(block_time) as block_time
    , min_by(tx_hash, block_time) as tx_hash
    , min_by(tx_from, block_time) as tx_from
    , min_by(tx_to, block_time) as tx_to
    , min_by(call_trace_address, block_time) as call_trace_address
    , min_by(call_from, block_time) as call_from
    , min_by(call_to, block_time) as call_to
    , min_by(call_selector, block_time) as call_selector
    , min_by(call_method, block_time) as call_method

    , min_by(protocol, block_time) as protocol
    , min_by(protocol_version, block_time) as protocol_version
    , min_by(contract_name, block_time) as contract_name
    
    , sum(amount_usd) as amount_usd
    , sum(execution_cost) as execution_cost
    
    , max(coalesce(src_executed_address, src_token_address)) as src_token_address
    , max(src_token_amount) as src_token_amount
    , max(src_executed_symbol) as src_token_symbol
    , sum(src_executed_amount) as src_executed_amount
    , sum(src_executed_amount_usd) as src_executed_amount_usd
    
    , max(coalesce(dst_executed_address, dst_token_address)) as dst_token_address
    , max(dst_token_amount) as dst_token_amount
    , max(dst_executed_symbol) as dst_token_symbol
    , sum(dst_executed_amount) as dst_executed_amount
    , sum(dst_executed_amount_usd) as dst_executed_amount_usd

    , min_by(remains, block_time) as remains
    , min_by(flags, block_time) as flags
    
    , array_agg(map_from_entries(array[
        ('amount', concat('$', format_number(amount_usd)))
        , ('execution', cast(execution_id as varchar))
        , ('resolver', resolver_name)
        , ('hashlock', cast(hashlock as varchar))
        , ('receiver', cast(receiver as varchar))
    ])) as executions
from {{ ref('oneinch_executions') }}
where true
    and coalesce(tx_success, true) -- for solana trades // TO DO
    and coalesce(call_success, true) -- for solana trades
    {% if is_incremental() -%}
        and (
            order_hash is null and {{ incremental_predicate('block_time') }}
            or order_hash in (select order_hash from incremental)
        ) -- e.g. if a new fill happens a week later, update the whole trade
    {%- endif %}
group by 1, 2, 3