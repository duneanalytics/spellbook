{% macro
    oneinch_raw_calls_macro(
        blockchain,
        stream
    )
%}

{% set meta = oneinch_meta_cfg_macro() %}
{% set contracts = meta['streams'][stream]['contracts'] %}
{% set date_from = [meta['blockchains']['start'][blockchain], meta['streams'][stream]['start']['raw_calls']] | max %}
{% set contract_addresses = {} %}



with

{% if stream == "cc" %}creations as (
    select address as contract_address
    from {{ source(blockchain, 'creation_traces') }}
    where true
        and block_time >= timestamp '{{ date_from }}' -- without an incremental predicate, as the results may be delayed, i.e. need escrow creations for all time
        and "from" in ({{ meta['blockchains']['escrow_factory_addresses'][blockchain] | join(', ') }})
)

, {% endif %}payload as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data.blockchains %}
        {% if contract_data.addresses == "creations" %}
            select
                creations.contract_address
                , '{{ contract }}' as contract_name
                , timestamp '{{ [date_from, contract_data.start] | max }}' as date_from
                , methods.selector
                , methods.method
                , methods.auxiliary
            from (
                {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %}{# method-level blockchains override contract-level blockchains #}
                    select
                        {{ method_data.get('selector', 'cast(null as varbinary)') }} as selector
                        , '{{ method }}' as method
                        , {{ method_data.get('auxiliary', 'null') }} as auxiliary
                    {% if not loop.last %}union{% endif %}
                {% endfor %}
            ) as methods, creations
        {% else %}
            {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %}{# method-level blockchains override contract-level blockchains #}
                select
                    {% for address, blockchains in contract_data.addresses.items() if blockchain in blockchains %}{{ address }}{% endfor %} as contract_address
                    , '{{ contract }}' as contract_name
                    , timestamp '{{ [date_from, contract_data.start] | max }}' as date_from
                    , {{ method_data.get('selector', 'null') }} as selector
                    , '{{ method }}' as method
                    , {{ method_data.get('auxiliary', 'cast(null as boolean)') }} as auxiliary
                {% if not loop.last %}union{% endif %}
            {% endfor %}
        {% endif %}
        {% if not loop.last %}union{% endif %}
    {% endfor %}
    union select * from (values (null, null, null, null, null, null)) as t(contract_address, contract_name, date_from, selector, method, auxiliary)
)

, traces as (
    select *
    from {{ source(blockchain, 'traces') }}
    where true
        and type = 'call'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

-- output --

select
    '{{blockchain}}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , upper('{{ stream }}') as protocol
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

{% endmacro %}