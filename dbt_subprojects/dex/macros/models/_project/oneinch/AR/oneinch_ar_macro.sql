{% macro
    oneinch_ar_macro(
        blockchain
    )
%}



{% set native = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}


with
-- pools tokens for unoswap lineage tokens parsing
pools_list as (
    select
        pool as pool_address
        , tokens
    from {{ ref('dex_raw_pools') }}
    where
        type in ('uniswap_compatible', 'curve_compatible')
        and blockchain = '{{ blockchain }}'
    group by 1, 2
)


, calls as (
    {% for contract, contract_data in oneinch_ar_cfg_contracts_macro().items() if blockchain in contract_data.blockchains %}

    select * from (
        with traces_cte as (
            select
                block_number as call_block_number
                , tx_hash as call_tx_hash
                , trace_address as call_trace_address
                , "from" as call_from
                , selector as call_selector
                , gas_used as call_gas_used
                , input as call_input
                , input_length as call_input_length
                , substr(input, input_length - mod(input_length - 4, 32) + 1) as remains
                , output as call_output
                , error as call_error
                , value as call_value
                , call_type
            from {{ ref('oneinch_' + blockchain + '_ar_raw_traces') }}
            where
                {% if is_incremental() %}
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    -- block_time >= timestamp '{{ contract_data['start'] }}'
                    block_time >= timestamp '2024-08-20'
                {% endif %}
        )


        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %} -- method-level blockchains override contract-level blockchains
            {% if method_data.router_type in ['generic', 'clipper'] %}
                {{
                    oneinch_ar_handle_generic(
                        contract=contract,
                        contract_data=contract_data,
                        method=method,
                        method_data=method_data,
                        blockchain=blockchain,
                        traces_cte=traces_cte,
                        start_date=contract_data['start'],
                    )
                }}
            {% elif method_data.router_type in ['unoswap'] %}
                {{
                    oneinch_ar_handle_unoswap(
                        contract=contract,
                        contract_data=contract_data,
                        method=method,
                        method_data=method_data,
                        blockchain=blockchain,
                        traces_cte=traces_cte,
                        pools_list=pools_list,
                        start_date=contract_data['start'],
                    )
                }}
            {% endif %}
        {% if not loop.last %} union all {% endif %}
        {% endfor %}
    )
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select
    blockchain
    , block_number
    , block_time
    , block_date
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , contract_name
    , 'AR' as protocol
    , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    , call_success
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , src_receiver
    , dst_receiver
    , if(element_at(pools[1], 'unwrap') = 0x01 and src_token_address = wrapped_native_token_address and call_value > uint256 '0', {{native}}, src_token_address) as src_token_address
    , if(element_at(reverse(pools)[1], 'unwrap') = 0x01 and dst_token_address = wrapped_native_token_address, {{native}}, dst_token_address) as dst_token_address
    , src_token_amount
    , dst_token_amount
    , dst_token_amount_min
    , map_from_entries(array[('ordinary', ordinary)]) as flags
    , pools
    , router_type
    , concat(cast(length(remains) as bigint), if(length(remains) > 0
        , transform(sequence(1, length(remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(remains), x, 4))))
        , array[bigint '0']
    )) as remains
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from (
    {{
        add_tx_columns(
            model_cte = 'calls'
            , blockchain = blockchain
            , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used']
        )
    }}
)
join ({{ oneinch_blockchain_macro(blockchain) }}) on true

{% endmacro %}