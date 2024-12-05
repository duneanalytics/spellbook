{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'order_updated',
    materialized = 'incremental',
    unique_key = ['tx_hash', 'index'],
    incremental_strategy = 'merge'
    )
}}

{%- set event_name = 'OrderUpdated' -%}
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
    UNION DISTINCT
    SELECT *
    FROM evt_data_2
)

, parsed_data AS (
    SELECT
        tx_hash,
        index, 
        json_query(data, 'lax $.bytes32Items' OMIT QUOTES) AS bytes32_items,
        json_query(data, 'lax $.addressItems' OMIT QUOTES) AS address_items,
        json_query(data, 'lax $.uintItems' OMIT QUOTES) AS uint_items,
        json_query(data, 'lax $.boolItems' OMIT QUOTES) AS bool_items
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

, bool_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(bool_items, '$.items') AS ARRAY(JSON))
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
    FROM uint_items_parsed
    UNION ALL 
    SELECT *
    FROM bool_items_parsed
)

, evt_data_parsed AS (
    SELECT
        tx_hash,
        index,
        MAX(CASE WHEN key_name = 'key' THEN value END) AS key,
        MAX(CASE WHEN key_name = 'account' THEN value END) AS account,
        MAX(CASE WHEN key_name = 'sizeDeltaUsd' THEN value END) AS size_delta_usd,
        MAX(CASE WHEN key_name = 'acceptablePrice' THEN value END) AS acceptable_price,
        MAX(CASE WHEN key_name = 'triggerPrice' THEN value END) AS trigger_price,
        MAX(CASE WHEN key_name = 'minOutputAmount' THEN value END) AS min_output_amount,
        MAX(CASE WHEN key_name = 'updatedAtTime' THEN value END) AS updated_at_time,
        MAX(CASE WHEN key_name = 'validFromTime' THEN value END) AS valid_from_time,
        MAX(CASE WHEN key_name = 'autoCancel' THEN value END) AS auto_cancel
    FROM
        combined
    GROUP BY tx_hash, index
)

-- full data 
, event_data AS (
    SELECT 
        blockchain,
        block_time,
        block_number,
        ED.tx_hash,
        ED.index,
        contract_address,
        event_name,
        msg_sender,
        
        from_hex(key) AS key,
        from_hex(account) AS account,
        TRY_CAST(size_delta_usd AS DOUBLE) AS size_delta_usd,
        TRY_CAST(acceptable_price AS DOUBLE) AS acceptable_price,
        TRY_CAST(trigger_price AS DOUBLE) AS trigger_price,
        TRY_CAST(min_output_amount AS DOUBLE) AS min_output_amount,
        TRY_CAST(updated_at_time AS DOUBLE) AS updated_at_time,
        TRY_CAST(valid_from_time AS DOUBLE) AS valid_from_time,
        TRY_CAST(auto_cancel AS BOOLEAN) AS auto_cancel
        
    FROM evt_data AS ED
    LEFT JOIN evt_data_parsed AS EDP
        ON ED.tx_hash = EDP.tx_hash
            AND ED.index = EDP.index
)

-- full data 
, full_data AS (
    SELECT 
        ED.blockchain,
        ED.block_time,
        DATE(ED.block_time) AS block_date,
        ED.block_number,
        ED.tx_hash,
        ED.index,
        ED.contract_address,
        ED.event_name,
        ED.msg_sender,
        
        ED.key,
        OC.market,
        ED.account,
        ED.size_delta_usd / POWER(10, 30) AS size_delta_usd,
        ED.acceptable_price AS acceptable_price_raw,
        CASE 
            WHEN MD.index_token_decimals IS NULL THEN NULL
            ELSE ED.acceptable_price / POWER(10, 30 - MD.index_token_decimals) 
        END AS acceptable_price,
        ED.trigger_price AS trigger_price_raw,
        CASE 
            WHEN MD.index_token_decimals IS NULL THEN NULL
            ELSE ED.trigger_price / POWER(10, 30 - MD.index_token_decimals) 
        END AS trigger_price,
        ED.min_output_amount AS min_output_amount_raw, 
        CASE 
            WHEN ED.updated_at_time = 0 THEN NULL
            ELSE ED.updated_at_time
        END AS updated_at_time,
        ED.valid_from_time,
        ED.auto_cancel

    FROM event_data AS ED
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_order_created') }} AS OC
        ON ED.key = OC.key
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_markets_data') }} AS MD
        ON OC.market = MD.market
        
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}
