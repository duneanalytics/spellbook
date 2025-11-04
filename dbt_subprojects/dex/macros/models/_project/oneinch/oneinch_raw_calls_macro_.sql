{%- macro
    oneinch_raw_calls_macro_(
        blockchain,
        stream,
        contracts
    )
-%}

{%- set date_from = [blockchain.start, stream.start] | max -%}



with

payload as (
    {% for contract, contract_data in contracts.items() %}
        {% for method, method_data in contract_data.methods.items() %}
            select
                {% if contract_data.addresses != "creations" %}{{ contract_data.address }} as {% endif %}contract_address
                , '{{ contract }}' as contract_name
                , {{ contract_data['version'] }} as protocol_version
                , timestamp '{{ [date_from, contract_data.start] | max }}' as date_from
                , {{ method_data.get('selector', 'null') }} as selector
                , '{{ method }}' as method
                , {{ method_data.get('auxiliary', contract_data.get('auxiliary', 'false')) }} as auxiliary
            {% if contract_data.addresses == "creations" %}from (
                select distinct contract_address
                from {{ source('oneinch_' + blockchain.name, contract + '_call_' + method) }}
                where contract_address <> {{ contract_data.address }} -- to filter calls for the initial implementation of Escrow Src/Dst contracts
            ){%- endif %}
            {% if not loop.last %}union{% endif %}
        {% endfor %}
        {% if not loop.last %}union{% endif %}
    {% endfor %}
)

, traces as (
    select *
    from {{ source(blockchain.name, 'traces') }}
    where true
        and type = 'call'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

-- output --

select
    '{{ blockchain.name }}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , upper('{{ stream.name }}') as protocol
    , protocol_version
    , contract_name
    , trace_address as call_trace_address -- the call prefix is needed for merging without renaming
    , success as call_success
    , selector as call_selector
    , method as call_method
    , "from" as call_from
    , "to" as call_to
    , gas_used as call_gas_used
    , call_type
    , error as call_error
    , input as call_input
    , value as call_value
    , length(input) as call_input_length
    , output as call_output
    , auxiliary
    , date_trunc('minute', block_time) as minute
    , block_date
    , date(date_trunc('month', block_time)) as block_month
from traces
join payload on true
    and "to" = contract_address
    and substr(input, 1, 4) = selector
    and block_date >= date_from

{%- endmacro -%}