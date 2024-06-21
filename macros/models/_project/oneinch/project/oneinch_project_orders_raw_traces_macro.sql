{% macro oneinch_project_orders_raw_traces_macro(
    blockchain
    , date_from = '2019-01-01'
)%}

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
    , output
    , block_date
from {{ source(blockchain, 'traces') }}
where
    {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
    {% else %}
        block_time >= timestamp '{{date_from}}'
    {% endif %}

    and substr(input, 1, 4) in (
        {% set selectors_array = [] %}
        {% for selectors in oneinch_project_orders_cfg_methods_macro().values() %}
            {% for selector in selectors.keys() %}
                {% do selectors_array.append(selector) %}
            {% endfor %}
        {% endfor %}
        {{ ','.join(selectors_array) }}
    )

{% endmacro %}
