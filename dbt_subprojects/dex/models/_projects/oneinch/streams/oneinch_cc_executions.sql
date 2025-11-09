{%- set stream = oneinch_cc_executions_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch',
        alias = stream.name + '_executions',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['blockchain', 'block_month'],
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{%- set date_from = stream.start -%}



with

incremental as (
    select hashlock
    from {{ ref('oneinch_' + stream.name) }}
    where {{ incremental_predicate('block_time') }}
    group by 1
)

, iterations as (
    {% for blockchain in oneinch_blockchains_cfg_macro() if stream.name in blockchain.exposed %}
        select *
        from {{ ref('oneinch_' + blockchain.name + '_' + stream.name + '_executions') }}
        where true
            and block_date >= date('{{ date_from }}')
            {% if is_incremental() -%}
                and hashlock in (select hashlock from incremental) -- e.g. if "cancel" happens a week later, update the whole trade
            {%- endif %}
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
)

, actions as (
    select
        order_hash
        , hashlock
        , max_by(cast(row(blockchain, token) as row(blockchain varchar, token varbinary)), block_time) filter(where flow = 'dst_creation') as dst_creation_info
        , max_by(transfered, block_time) filter(where flow = 'src_withdraw') as src_executed
        , max_by(transfered, block_time) filter(where flow = 'dst_withdraw') as dst_executed
        , max(amount_usd) filter(where flow in ('src_withdraw', 'dst_withdraw') and transfered.trusted) as sources_executed_trusted_amount_usd
        , max(amount_usd) filter(where flow in ('src_withdraw', 'dst_withdraw')) as sources_executed_amount_usd
        , sum(action_cost) as execution_cost
        , array_agg(cast(
            row(
                flow
                , tx_success and call_success
                , action_cost
                , tx_hash
                , escrow
                , token
                , amount
            ) as row(
                action varchar
                , success boolean
                , cost double
                , tx_hash varbinary
                , escrow varbinary
                , token varbinary
                , amount uint256
            )
        )) as actions
    from iterations
    group by 1, 2
)

-- output --

select
    blockchain
    , chain_id
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , tx_from
    , tx_to
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , tx_index -- it is necessary to determine the order of creations in the block
    , call_trace_address
    , call_success
    , call_gas_used
    , call_selector
    , call_method
    , call_from
    , call_to
    , call_output
    , call_error
    , call_type
    , protocol
    , protocol_version
    , contract_name

    , coalesce(null
        , sources_executed_trusted_amount_usd
        , sources_executed_amount_usd
    ) as amount_usd
    , execution_cost

    , from_hex(complement['order_maker']) as user
    , from_hex(complement['order_receiver']) as receiver
    , token as src_token_address
    , cast(complement['order_maker_amount'] as uint256) as src_token_amount
    , src_executed.address as src_executed_address
    , src_executed.symbol as src_executed_symbol
    , src_executed.amount as src_executed_amount
    , src_executed.amount_usd as src_executed_amount_usd
    
    , dst_creation_info.blockchain as dst_blockchain
    , dst_creation_info.token as dst_token_address
    , cast(complement['order_taker_amount'] as uint256) as dst_token_amount
    , dst_executed.address as dst_executed_address
    , dst_executed.symbol as dst_executed_symbol
    , dst_executed.amount as dst_executed_amount
    , dst_executed.amount_usd as dst_executed_amount_usd

    , order_hash
    , hashlock
    , actions

    , map_concat(complement, map_from_entries(array[
        ('executed_receiver', cast(dst_executed.receiver as varchar))
        , ('src_decimals', cast(src_executed.decimals as varchar))
        , ('dst_decimals', cast(dst_executed.decimals as varchar))
    ])) as complement

    , remains
    , map_from_entries(array[('cross_chain', true)]) as flags
    , minute
    , block_date
    , block_month
    , native_price
    , native_decimals
from (
    select *
    from iterations
    where true
        and flow = 'src_creation'
)
join actions using(order_hash, hashlock)