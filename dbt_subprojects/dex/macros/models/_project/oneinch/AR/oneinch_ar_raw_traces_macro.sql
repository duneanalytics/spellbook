{% macro oneinch_ar_raw_traces_macro(
    blockchain
    , date_from = '2019-06-01'
)%}

-- test in CI
with decoded_calls as (
    {% for contract, contract_data in oneinch_ar_cfg_contracts_macro().items() if blockchain in contract_data.blockchains %}
        {% set outer_loop = loop %}
        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %} -- method-level blockchains override contract-level blockchains
            select call_tx_hash as tx_hash, call_block_number as block_number, call_trace_address as trace_address, date(call_block_time) as block_date from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
            {% if is_incremental() %}
                where {{ incremental_predicate('call_block_time') }}
            {% else %}
                where call_block_time >= greatest(timestamp '{{ contract_data['start'] }}', timestamp '{{date_from}}')
            {% endif %}
            {% if not outer_loop.last or not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    {% endfor %}
)

, traces as (
    select
        block_number
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
            block_time >= timestamp '{{date_from}}'
        {% endif %}
)


select * from traces
join decoded_calls using(tx_hash, block_number, trace_address, block_date)

{% endmacro %}
