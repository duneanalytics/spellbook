{% macro
    oneinch_cc_handle_results_macro(
        blockchain,
        contracts,
        date_from
    )
%}



with

results as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data.blockchains and contract_data.addresses == 'creations' %}
        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %} -- method-level blockchains override contract-level blockchains
            select
                blockchain
                , block_number
                , block_time
                , block_date
                , tx_hash
                , tx_success
                , trace_address as call_trace_address
                , success as call_success
                , gas_used as call_gas_used
                , selector as call_selector
                , method as call_method
                , "from" as call_from
                , "to" as call_to -- escrow
                , output as call_output
                , error as call_error
                , call_type
                , '{{ contract_data['version'] }}' as protocol_version
                , '{{ contract }}' as contract_name
                , {{ method_data.get("order_hash", "null") }} as order_hash
                , {{ method_data.get("hashlock", "null") }} as hashlock
                , {{ method_data.get("secret", "null") }} as secret

                , {{ method_data.get("maker", "null") }} as maker
                , {{ method_data.get("receiver", "null") }} as receiver

                , {{ method_data.get("token", "null") }} as token
                , {{ method_data.get("amount", "null") }} as amount
                , {{ method_data.get("safety_deposit", "null") }} as safety_deposit
                , {{ method_data.get("timelocks", "null") }} as timelocks
                , {{ method_data.get("rescue_token", "null") }} as rescue_token
                , {{ method_data.get("rescue_amount", "null") }} as rescue_amount
            from {{ ref('oneinch_' + blockchain + '_cc_raw_calls') }}
            where true
                and call_type = 'call'
                and selector = {{ method_data['selector'] }}
                and block_date >= timestamp '{{ date_from }}'
                {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

-- output --

select
    blockchain
    , block_number
    , {{ oneinch_meta_cfg_macro(property = 'blockchains')['chain_id'][blockchain] }} as chain_id
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
    , call_to as contract_address
    , contract_name
    , call_method as action
    , call_selector as action_id
    , order_hash
    , hashlock
    , call_to as escrow
    , secret
    , maker
    , null as taker
    , receiver
    , coalesce(rescue_token, token) as token
    , coalesce(rescue_amount, amount) as amount
    , safety_deposit
    , timelocks
    , map_from_entries(array[
        ('token', cast(if(call_method = 'rescueFunds', token) as varchar))
        , ('amount', cast(if(call_method = 'rescueFunds', amount) as varchar))
    ]) as complement
    , remains
    , flags
    , minute
    , block_date
    , block_month
from results

{% endmacro %}