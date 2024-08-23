{% macro oneinch_project_orders_raw_logs_macro(
    blockchain
    , date_from = '2024-08-20'
)%}

select 
    block_number
    , block_time
    , tx_hash
    , index
    , contract_address
    , topic0
    , topic1
    , topic2
    , topic3
    , data
    , row_number() over(partition by block_number, tx_hash order by index) as log_counter
    , date(block_time) as block_date
from {{ source(blockchain, 'logs') }}
where
    {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
    {% else %}
        block_time >= timestamp '{{date_from}}'
    {% endif %}

    and topic0 in (
        {{ oneinch_project_orders_cfg_events_macro().keys() | join(', ') }}
    )

{% endmacro %}
