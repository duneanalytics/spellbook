{% macro oneinch_ar_raw_traces_macro(
    blockchain
    , date_from = '2019-06-01'
)%}

with

cfg as (
    {% for contract, contract_data in oneinch_ar_cfg_contracts_macro().items() if blockchain in contract_data.blockchains %}
        {% set outer_loop = loop %}
        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %} -- method-level blockchains override contract-level blockchains
            select
                {% for address, blockchains in contract_data.addresses.items() if blockchain in blockchains %}{{address}}{% endfor %} as "to"
                , '{{ contract }}' as contract_name
                , {{ method_data.get('selector', 'null') }} as selector
                , '{{ method }}' as method
            {% if not outer_loop.last or not loop.last %}union{% endif %}
        {% endfor %}
    {% endfor %}
)

, traces as (
    select
        '{{blockchain}}' as blockchain
        , block_number
        , block_time
        , tx_hash
        , tx_success
        , "from"
        , "to"
        , trace_address
        , success
        , substr(input, 1, 4) as selector
        , gas_used
        , call_type
        , error
        , input 
        , value
        , length(input) as input_length
        , output
        , block_date
    from {{ source(blockchain, 'traces') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= greatest(timestamp '{{ date_from }}', timestamp {{ oneinch_easy_date() }})
        {% endif %}
)

select *
from traces
join cfg using("to", selector)

{% endmacro %}
