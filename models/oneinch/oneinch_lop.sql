{{  
    config(
        schema = 'oneinch',
        alias = alias('lop'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql'],
    )
}}

{% set lookback_days = -7 %}

with
    
    methods as (
        select
            contract_address
            , contract_name
            , blockchain
            , json_value(entity, 'lax $.name') as method
            , json_query(entity, 'lax $.inputs') as inputs_raw
            , regexp_extract_all(json_query(entity, 'lax $.inputs'), '"name":"([^"]*)"', 1) as inputs_names
            , regexp_extract_all(json_query(entity, 'lax $.inputs'), '"type":"([^"]*)"', 1) as inputs_types
            , cardinality(regexp_extract_all(json_query(entity, 'lax $.inputs'), '"name":"([^"]*)"', 1)) as inputs_number
            , json_query(entity, 'lax $.outputs') as outputs_raw
            , regexp_extract_all(json_query(entity, 'lax $.outputs'), '"name":"([^"]*)"', 1) as outputs_names
            , regexp_extract_all(json_query(entity, 'lax $.outputs'), '"type":"([^"]*)"', 1) as outputs_types
            , cardinality(regexp_extract_all(json_query(entity, 'lax $.outputs'), '"name":"([^"]*)"', 1)) as outputs_number
            , cardinality(regexp_extract_all(json_query(entity, 'lax $.outputs'), '"name":"([^"]+)"', 1)) as outputs_names_number
        from {{ ref('oneinch_exchange_contracts') }}, unnest(abi) as abi(entity)
        where project = '1inch'
            and json_value(entity, 'lax $.type') = 'function'
            and json_value(entity, 'lax $.stateMutability') in ('payable', 'nonpayable')
            and position('fill' in lower(json_value(entity, 'lax $.name'))) > 0
    )

    , orders as (
        
        {% for row in methods %}

            {% if row.outputs_names_number > 1 %}
                {% set making_amount = 'output_' + row.outputs_names[1] %}
                {% set taking_amount = 'output_' + row.outputs_names[2] %}
            {% elif row.outputs_number > 1 %}
                {% set making_amount = 'output_0' %}
                {% set taking_amount = 'output_1' %}
            {% else %}
                {% set making_amount = 'null' %}
                {% set taking_amount = 'null' %}
            {% endif %}

            select
                -- block
                {{ row.blockchain }} as blockchain
                , transactions.block_time
                
                -- tx
                , hash as tx_hash
                , transactions."from" as tx_from
                , transactions."to" as tx_to
                , transactions.success as tx_success

                -- contract & method
                , {{ row.contract_name }} as contract_name
                , cast(cast(substr({{ row.contract_name }}, length({{ row.contract_name }})) as double) - if(position('limit' in lower({{ row.contract_name }})) > 0, 0, 2) as varchar) as protocol_version
                , {{ row.method }} as method

                -- call
                , traces."from" as call_from
                , orders.contract_address as call_to
                , trace_address as call_trace_address
                , substr(traces.input, 1, 4) as call_selector
                , {% if not 'maker' in row.inputs_names %} substr(order_map['makerAssetData'], 4 + 12 + 1, 20) {% else %} order_map['maker'] {% endif %} as maker
                , from_hex(order_map['makerAsset']) as maker_asset
                , if(orders.making_amount is null, bytearray_to_uint256(substr(order_map['makerAssetData'], 4 + 32*2 + 1, 32)), orders.making_amount) as making_amount
                , from_hex(order_map['takerAsset']) as taker_asset
                , if(orders.taking_amount is null, bytearray_to_uint256(substr(order_map['takerAssetData'], 4 + 32*2 + 1, 32)), orders.taking_amount) as taking_amount
                , traces.success as call_success
                , traces.gas_used as call_gas_used
                , traces.input as call_input
                , traces.output as call_output

                -- ext
                , date_trunc('minute', transactions.block_time) as minute
                , {{ row.inputs_names }} as inputs_names
                , {{ row.inputs_types }} as inputs_types
                , {{ row.outputs_names }} as outputs_names
                , {{ row.outputs_types }} as outputs_types
            from (
                select
                    call_tx_hash as tx_hash
                    , contract_address
                    , call_trace_address as trace_address
                    , {{ making_amount }} as making_amount
                    , {{ taking_amount }} as taking_amount
                    , cast(json_parse({% if "order_" in row.inputs_names %} "order_" {% else %} "order" {% endif %}) as map(varchar, varchar)) as order_map
                from {{ source('oneinch_' + row.blockchain, row.contract_name + '_call_' + row.method) }}
                {% if is_incremental() %}
                    where call_block_time >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
                {% endif %}
            ) as orders
            join {{ source(row.blockchain, 'transactions') }} on transactions.hash = orders.tx_hash
            join {{ source(row.blockchain, 'traces') }} using(tx_hash, trace_address)
            
            {% if not loop.last %} union all {% endif %}

        {% endfor %}
    )

select *
from orders