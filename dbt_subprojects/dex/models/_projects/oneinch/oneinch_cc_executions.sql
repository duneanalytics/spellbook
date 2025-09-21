{% set stream = 'cc_executions' %}

{{
    config(
        schema = 'oneinch',
        alias = stream,
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['block_month'],
        unique_key = ['unique_key'],
    )
}}

{% set meta = oneinch_meta_cfg_macro(property = 'blockchains') %}
{% set date_from = oneinch_meta_cfg_macro(property = 'streams')[stream]['start'] %}
{% set wrapper = meta['wrapped_native_token_address'][blockchain] %}
{% set same = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, ' + wrapper + ')' %}



with

meta(dst_blockchain, dst_chain_id) as (values
    {% for blockchain, exposed in meta['exposed'].items() if exposed == 'evms' %} -- TO DO: all exposed blockchains, i.e. add solana for now
        {% if not loop.first %}, {% endif %}('{{ blockchain }}', {{ meta['chain_id'][blockchain] }})
    {% endfor %}
)

, iterations as (
    select *
        , case
            when position('src' in lower(action)) > 0 then 'src_escrow_creation'
            when position('dst' in lower(action)) > 0 then 'dst_escrow_creation'
            when array_position(src_escrows, escrow) > 0 then concat('src_', action)
            when array_position(dst_escrows, escrow) > 0 then concat('dst_', action)
            else 'unknown'
        end as flow
    from (
        select *
            , cast(element_at(complement, 'dst_chain_id') as int) as dst_chain_id
            , array_agg(distinct if(action = 'SrcEscrowCreated', escrow)) over(partition by hashlock) as src_escrows
            , array_agg(distinct if(action = 'createDstEscrow', escrow)) over(partition by hashlock) as dst_escrows
        from {{ ref('oneinch_cc') }} -- all blockchains are needed to merge src and dst results
        where true
            and block_date >= timestamp '{{ date_from }}'
            {% if is_incremental() %}and hashlock in (select hashlock from {{ ref('oneinch_cc') }} where {{ incremental_predicate('block_time') }} group by 1){% endif %}
    )
    left join meta using(dst_chain_id)
)

, transfers as (
    select *
        , if(transfer_contract_address in {{ same }}, {{ same }}, (transfer_contract_address)) as same
        , row_number() over(partition by block_month, block_date, block_number, tx_hash order by transfer_trace_address desc) as transfer_number_desc
    from {{ ref('oneinch_evms_raw_transfers') }} -- also need all the blockchains
    where true
        and nested
        and related
        and protocol = 'CC'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and block_time >= (select min(block_time) from {{ ref('oneinch_cc') }} where {{ incremental_predicate('block_time') }}){% endif %}
)

, amounts as (
    select
        hashlock
        , max_by(cast(row(transfer_contract_address, transfer_symbol, transfer_decimals) as row(address varbinary, symbol varchar, decimals bigint)), (transfer_amount, transfer_trace_address)) filter(where flow = 'src_escrow_creation' and transfer_from = maker) as src_executed
        , sum(transfer_amount) filter(where flow = 'src_withdraw' and token in same) as src_executed_amount -- same: token address of iteration ~ transfer address (i.e. native ~ wrapped_native)
        , sum(transfer_amount_usd) filter(where flow = 'src_withdraw' and token in same) as src_executed_amount_usd
        , sum(transfer_amount_usd) filter(where flow = 'src_withdraw' and token in same and trusted) as src_executed_trusted_amount_usd
        , max_by(cast(row(transfer_contract_address, transfer_symbol, transfer_decimals, transfer_to) as row(address varbinary, symbol varchar, decimals bigint, receiver varbinary)), (transfer_amount, transfer_trace_address)) filter(where flow = 'dst_escrow_creation' and transfer_amount = amount) as dst_executed
        , sum(transfer_amount) filter(where flow = 'dst_withdraw' and token in same) as dst_executed_amount
        , sum(transfer_amount_usd) filter(where flow = 'dst_withdraw' and token in same) as dst_executed_amount_usd
        , sum(transfer_amount_usd) filter(where flow = 'dst_withdraw' and token in same and trusted) as dst_executed_trusted_amount_usd
        , array_agg(distinct cast(
            row(
                flow
                , tx_success and call_success
                , tx_gas_used * gas_price * native_price / pow(10, native_decimals)
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
        , sum(tx_gas_price * tx_gas_used * native_price / pow(10, native_decimals)) as execution_cost
    from iterations
    left join transfers using(blockchain, block_month, block_date, block_time, block_number, tx_hash, call_trace_address, call_to, protocol, contract_name, call_selector, call_method) -- even with missing transfers, as transfers may not have been parsed
    group by 1
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
    , contract_address
    , contract_name

    , coalesce(src_executed_trusted_amount_usd, dst_executed_trusted_amount_usd, src_executed_amount_usd, dst_executed_amount_usd) as amount_usd
    , execution_cost

    , maker as user
    , receiver

    , token as src_token_address
    , cast(element_at(complement, 'order_src_amount') as uint256) as src_token_amount
    , src_executed.address
    , src_executed.symbol
    , src_executed_amount
    , src_executed_amount_usd

    , dst_blockchain

    , cast(element_at(complement, 'dst_token') as uint256) as dst_token_address
    , cast(element_at(complement, 'order_dst_amount') as uint256) as dst_token_amount
    , dst_executed.address
    , dst_executed.symbol
    , dst_executed_amount
    , dst_executed_amount_usd

    , actions
    , order_hash
    , hashlock

    , map_concat(complement, map_from_entries(array[
        ('executed_receiver', cast(dst_executed.receiver as varchar))
        , ('src_decimals', cast(src_executed.decimals as varchar))
        , ('dst_decimals', cast(dst_executed.decimals as varchar))
    ])) as complement

    , remains
    , flags
    , minute
    , block_date
    , block_month
    , native_price
    , native_decimals
from (
    select *
    from iterations
    where flow = 'src_escrow_creation'
)
left join amounts using(hashlock)