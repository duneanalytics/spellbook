{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'order_cancelled',
    materialized = 'table'
    )
}}

{%- set event_name = 'OrderCancelled' -%}
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
        msgSender AS msg_sender,
        topic1,
        CAST(NULL AS varbinary) AS topic2  -- Ensure topic2 is treated as varbinary
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog1')}}
    WHERE eventName = '{{ event_name }}'
    ORDER BY evt_block_time ASC
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
        msgSender AS msg_sender,
        topic1,
        topic2
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog2')}}
    WHERE eventName = '{{ event_name }}'
    ORDER BY evt_block_time ASC
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
        json_query(data, 'lax $.bytes32Items' OMIT QUOTES) AS bytes32_items,
        json_query(data, 'lax $.addressItems' OMIT QUOTES) AS address_items,
        json_query(data, 'lax $.bytesItems' OMIT QUOTES) AS bytes_items,
        json_query(data, 'lax $.stringItems' OMIT QUOTES) AS string_items
        
    FROM
        evt_data
)

, bytes32_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(bytes32_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
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

, bytes_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(bytes_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, string_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(string_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, combined AS (
    SELECT *
    FROM bytes32_items_parsed
    UNION ALL      
    SELECT *
    FROM address_items_parsed
    UNION ALL    
    SELECT *
    FROM bytes_items_parsed
    UNION ALL
    SELECT *
    FROM string_items_parsed
)

, evt_data_parsed AS (
    SELECT
        tx_hash,
        index,
        MAX(CASE WHEN key_name = 'key' THEN value END) AS key,
        MAX(CASE WHEN key_name = 'account' THEN value END) AS account,
        MAX(CASE WHEN key_name = 'reasonBytes' THEN value END) AS reason_bytes,
        MAX(CASE WHEN key_name = 'reason' THEN value END) AS reason
    FROM
        combined
    GROUP BY tx_hash, index
)

-- full data 
SELECT 
    blockchain,
    block_time,
    block_number,
    ED.tx_hash,
    ED.index,
    contract_address,
    event_name,
    msg_sender,
    topic1, 
    topic2,
    
    key,
    account,
    reason_bytes,
    reason

FROM evt_data AS ED
LEFT JOIN evt_data_parsed AS EDP
    ON ED.tx_hash = EDP.tx_hash
        AND ED.index = EDP.index

