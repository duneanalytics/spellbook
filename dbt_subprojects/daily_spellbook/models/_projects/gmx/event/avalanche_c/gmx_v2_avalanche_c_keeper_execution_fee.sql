{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'keeper_execution_fee',
    materialized = 'incremental',
    unique_key = ['tx_hash', 'index'],
    incremental_strategy = 'merge'
    )
}}

{%- set event_name = 'KeeperExecutionFee' -%}
{%- set blockchain_name = 'avalanche_c' -%}


WITH evt_data_1 AS (
    SELECT 
        -- Main Variables
        '{{ blockchain_name }}' AS blockchain,
        evt_block_time AS block_time,
        evt_block_number AS block_number, 
        evt_tx_hash AS tx_hash,
        evt_index AS index,
        contract_address,
        eventName AS event_name,
        eventData AS data,
        msgSender AS msg_sender
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog1')}}
    WHERE eventName = '{{ event_name }}'
    {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

, evt_data_2 AS (
    SELECT 
        -- Main Variables
        '{{ blockchain_name }}' AS blockchain,
        evt_block_time AS block_time,
        evt_block_number AS block_number, 
        evt_tx_hash AS tx_hash,
        evt_index AS index,
        contract_address,
        eventName AS event_name,
        eventData AS data,
        msgSender AS msg_sender
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog2')}}
    WHERE eventName = '{{ event_name }}'
    {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

-- unite 2 tables
, evt_data AS (
    SELECT * 
    FROM evt_data_1
    UNION
    SELECT *
    FROM evt_data_2
)

, parsed_data AS (
    SELECT
        tx_hash,
        index, 
        json_query(data, 'lax $.addressItems' OMIT QUOTES) AS address_items,
        json_query(data, 'lax $.uintItems' OMIT QUOTES) AS uint_items
    FROM
        evt_data
)

, address_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(address_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, uint_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(uint_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, combined AS (
    SELECT *
    FROM address_items_parsed
    UNION ALL      
    SELECT *
    FROM uint_items_parsed
)

, evt_data_parsed AS (
    SELECT
        tx_hash,
        index,
        MAX(CASE WHEN key_name = 'keeper' THEN value END) AS keeper,
        MAX(CASE WHEN key_name = 'executionFeeAmount' THEN value END) AS execution_fee_amount
    FROM
        combined
    GROUP BY tx_hash, index
)

-- full data 
, full_data AS (
    SELECT 
        blockchain,
        block_time,
        DATE(block_time) AS block_date,
        block_number,
        ED.tx_hash,
        ED.index,
        contract_address,
        event_name,
        msg_sender,
        
        from_hex(EDP.keeper) AS keeper,
        CAST(EDP.execution_fee_amount AS DOUBLE) / POWER(10, 18) AS execution_fee_amount

    FROM evt_data AS ED
    LEFT JOIN evt_data_parsed AS EDP
        ON ED.tx_hash = EDP.tx_hash
        AND ED.index = EDP.index
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}

