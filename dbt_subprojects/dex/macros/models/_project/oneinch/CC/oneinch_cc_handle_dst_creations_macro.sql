{% macro
    oneinch_cc_handle_dst_creations_macro(
        blockchain,
        contracts,
        date_from
    )
%}



with

createDstEscrow as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data.blockchains and contract_data.addresses != 'creations' %}
        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %} -- method-level blockchains override contract-level blockchains
            select
                call_block_number as block_number
                , call_block_time as block_time
                , call_block_date as block_date
                , call_tx_hash as tx_hash
                , call_trace_address
                , call_success
                , '{{ contract }}' as contract_name
                , '{{ contract_data['version'] }}' as protocol_version
                , '{{ method }}' as call_method
                , '{{ method_data.selector }}' as call_selector
                , contract_address as factory
                , {{ method_data.get("order_hash", "null") }} as order_hash
                , {{ method_data.get("hashlock", "null") }} as hashlock
                , {{ method_data.get("maker", "null") }} as maker
                , {{ method_data.get("taker", "null") }} as taker
                , {{ method_data.get("token", "null") }} as token
                , {{ method_data.get("amount", "null") }} as amount
                , {{ method_data.get("safety_deposit", "null") }} as safety_deposit
                , {{ method_data.get("timelocks", "null") }} as timelocks
            from (
                select *, cast(json_parse({{ method_data.get("dstImmutables", '"dstImmutables"') }}) as map(varchar, varchar)) as creation_map
                from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                where true
                    and call_block_date >= timestamp '{{ date_from }}' -- there are only calls after the contract creation in the decoded table
                    {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
            )
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

, raw_calls as (
    select
        block_number
        , block_date
        , tx_hash
        , tx_success
        , trace_address as call_trace_address
        , "from" as call_from
        , "to" as factory
        , selector as call_selector
        , gas_used as call_gas_used
        , output as call_output
        , error as call_error
        , call_type
    from {{ ref('oneinch_' + blockchain + '_cc_raw_calls') }}
    where true
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

-- output --

select
    blockchain
    , {{ oneinch_meta_cfg_macro(property = 'blockchains')['chain_id'][blockchain] }} as chain_id
    , block_number
    , block_time
    , tx_hash
    , tx_success
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
    , protocol_version
    , factory as contract_address
    , contract_name
    , call_method as action
    , call_selector as action_id
    , order_hash
    , hashlock
    , substr(keccak(concat(
        0xff
        , factory
        , keccak(concat(
            order_hash
            , hashlock
            , lpad(maker, 32, 0x00)
            , lpad(taker, 32, 0x00)
            , lpad(token, 32, 0x00)
            , cast(amount as varbinary)
            , cast(safety_deposit as varbinary)
            , to_big_endian_32(cast(to_unixtime(block_time) as int))
            , substr(timelocks, 5) -- replace the first 4 bytes with current block time
        ))
        , keccak(concat(
            0x3d602d80600a3d3981f3363d3d373d3d3d363d73
            , substr(keccak(concat(0xd6, 0x94, factory, 0x03)), 13) -- dst nonce = 3 (0x03)
            , 0x5af43d82803e903d91602b57fd5bf3)
        )
    )), 13) as escrow
    , cast(null as varbinary) as secret
    , maker
    , taker
    , cast(null as varbinary) as receiver
    , token
    , amount
    , safety_deposit
    , timelocks
    , null as complement
    , null as remains
    , null as flags
    , minute
    , block_date
    , block_month
from createDstEscrow
join raw_calls using(block_date, block_number, tx_hash, factory, call_trace_address)

{% endmacro %}