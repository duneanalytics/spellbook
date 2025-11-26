{%- macro
    oneinch_project_orders_raw_traces_macro(
        blockchain,
        date_from = '2019-01-01'
    )
-%}

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
where true
    and block_time >= timestamp '{{ date_from }}'
    and substr(input, 1, 4) in (
        {% set selectors = [] %}
        {% for item in oneinch_project_orders_cfg_methods_macro() %}
            {% do selectors.append(item["selector"]) %}
        {% endfor %}
        {{ ','.join(selectors) }}
    )
    {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}

{%- endmacro -%}