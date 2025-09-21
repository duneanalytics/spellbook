{% macro
    oneinch_cc_handle_src_creations_macro(
        blockchain,
        stream,
        contracts,
        date_from
    )
%}



with

calls as (
    select *
        , call_from as taker
        , maker_asset as token
    from (
        {{ oneinch_lo_macro(blockchain = blockchain, for_stream = stream) }}
        where factory_in_args -- only calls where factory in args
    )
)

, SrcEscrowCreated as (
    {% for contract, contract_data in contracts.items() if contract_data.addresses != 'creations' %}
        {% for event, event_data in contract_data.events.items() if blockchain in event_data.get('blockchains', contract_data.blockchains) %} -- event-level blockchains override contract-level blockchains
            select
                evt_block_number as block_number
                , evt_block_date as block_date
                , evt_tx_hash as tx_hash
                , '{{ contract }}' as contract_name
                , '{{ contract_data['version'] }}' as protocol_version
                , '{{ event }}' as action
                , '{{ event_data.topic0 }}' as action_id
                , contract_address as factory
                , {{ event_data.get("order_hash", "null") }} as order_hash
                , {{ event_data.get("hashlock", "null") }} as hashlock
                , {{ event_data.get("maker", "null") }} as maker
                , {{ event_data.get("taker", "null") }} as taker
                , {{ event_data.get("token", "null") }} as token
                , {{ event_data.get("amount", "null") }} as amount
                , {{ event_data.get("safety_deposit", "null") }} as safety_deposit
                , {{ event_data.get("timelocks", "null") }} as timelocks
                , {{ event_data.get("dst_maker", "cast(null as varbinary)") }} as dst_maker
                , {{ event_data.get("dst_token", "cast(null as varbinary)") }} as dst_token
                , {{ event_data.get("dst_amount", "null") }} as dst_amount
                , {{ event_data.get("dst_chain_id", "null") }} as dst_chain_id
            from (
                select
                    *
                    , cast(json_parse({{ event_data.get("srcImmutables", '"srcImmutables"') }}) as map(varchar, varchar)) as creation_map
                    , cast(json_parse({{ event_data.get("dstImmutablesComplement", '"dstImmutablesComplement"') }}) as map(varchar, varchar)) as complement_map
                from {{ source('oneinch_' + blockchain, contract + '_evt_' + event) }}
                where true
                    and evt_block_date >= timestamp '{{ date_from }}'
                    {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
            )
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

-- output --

select
    blockchain
    , chain_id
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
    , SrcEscrowCreated.protocol_version
    , factory as contract_address
    , SrcEscrowCreated.contract_name
    , coalesce(action, 'createSrcEscrow') as action
    , action_id
    , order_hash
    , hashlock
    , if(hashlock is not null, substr(keccak(concat(
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
            , substr(keccak(concat(0xd6, 0x94, factory, 0x02)), 13) -- src nonce = 2 (0x02)
            , 0x5af43d82803e903d91602b57fd5bf3)
        )
    )), 13)) as escrow
    , cast(null as varbinary) as secret
    , maker
    , taker
    , coalesce(dst_maker, receiver) as receiver
    , token
    , amount
    , safety_deposit
    , timelocks
    , map_from_entries(array[
        ('dst_chain_id', cast(dst_chain_id as varchar))
        , ('dst_token', cast(coalesce(dst_token, taker_asset) as varchar))
        , ('dst_amount', cast(dst_amount as varchar))
        , ('order_src_amount', cast(maker_amount as varchar))
        , ('order_dst_amount', cast(taker_amount as varchar))
    ]) as complement
    , remains
    , flags
    , minute
    , block_date
    , block_month
from calls
left join SrcEscrowCreated using(block_date, block_number, tx_hash, order_hash, maker, taker, token)
where coalesce(varbinary_position(args, hashlock) > 0, true)

{% endmacro %}