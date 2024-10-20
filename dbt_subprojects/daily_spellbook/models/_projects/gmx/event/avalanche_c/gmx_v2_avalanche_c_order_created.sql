{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'order_created',
    materialized = 'table'
    )
}}

{%- set event_name = 'OrderCreated' -%}
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
        msgSender AS msg_sender
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog2')}}
    WHERE eventName = '{{ event_name }}'
    ORDER BY evt_block_time ASC
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
        json_query(data, 'lax $.addressItems' OMIT QUOTES) AS address_items,
        json_query(data, 'lax $.uintItems' OMIT QUOTES) AS uint_items,
        json_query(data, 'lax $.boolItems' OMIT QUOTES) AS bool_items,
        json_query(data, 'lax $.bytes32Items' OMIT QUOTES) AS bytes32_items
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

, address_array_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_format(json_extract(CAST(item AS VARCHAR), '$.value')) AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(address_items, '$.arrayItems') AS ARRAY(JSON))
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

, combined AS (
    SELECT *
    FROM address_items_parsed
    UNION ALL
    SELECT *
    FROM address_array_items_parsed
    UNION ALL    
    SELECT *
    FROM uint_items_parsed
    UNION ALL
    SELECT *
    FROM bool_items_parsed
    UNION ALL
    SELECT *
    FROM bytes32_items_parsed

)

, evt_data_parsed AS (
    SELECT
        tx_hash,
        index,
        MAX(CASE WHEN key_name = 'account' THEN value END) AS account,
        MAX(CASE WHEN key_name = 'receiver' THEN value END) AS receiver,
        MAX(CASE WHEN key_name = 'callbackContract' THEN value END) AS callback_contract,
        MAX(CASE WHEN key_name = 'uiFeeReceiver' THEN value END) AS ui_fee_receiver,
        MAX(CASE WHEN key_name = 'market' THEN value END) AS market,  
        MAX(CASE WHEN key_name = 'initialCollateralToken' THEN value END) AS initial_collateral_token,

        MAX(CASE WHEN key_name = 'swapPath' THEN value END) AS swap_path,  
        MAX(CASE WHEN key_name = 'orderType' THEN value END) AS order_type,
        MAX(CASE WHEN key_name = 'decreasePositionSwapType' THEN value END) AS decrease_position_swap_type,
        MAX(CASE WHEN key_name = 'sizeDeltaUsd' THEN value END) AS size_delta_usd,
        MAX(CASE WHEN key_name = 'initialCollateralDeltaAmount' THEN value END) AS initial_collateral_delta_amount,
        MAX(CASE WHEN key_name = 'triggerPrice' THEN value END) AS trigger_price,
        MAX(CASE WHEN key_name = 'acceptablePrice' THEN value END) AS acceptable_price,
        MAX(CASE WHEN key_name = 'executionFee' THEN value END) AS execution_fee,
        MAX(CASE WHEN key_name = 'callbackGasLimit' THEN value END) AS callback_gas_limit,
        MAX(CASE WHEN key_name = 'minOutputAmount' THEN value END) AS min_output_amount,
    
        MAX(CASE WHEN key_name = 'updatedAtBlock' THEN value END) AS updated_at_block,
        MAX(CASE WHEN key_name = 'updatedAtTime' THEN value END) AS updated_at_time,
    
        MAX(CASE WHEN key_name = 'isLong' THEN value END) AS is_long,
        MAX(CASE WHEN key_name = 'shouldUnwrapNativeToken' THEN value END) AS should_unwrap_native_token,
        MAX(CASE WHEN key_name = 'isFrozen' THEN value END) AS is_frozen,
    
        MAX(CASE WHEN key_name = 'key' THEN value END) AS key
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
        
        from_hex(account) AS account,
        from_hex(receiver) AS receiver,
        from_hex(callback_contract) AS callback_contract,
        from_hex(ui_fee_receiver) AS ui_fee_receiver,
        from_hex(market) AS market,
        from_hex(initial_collateral_token) AS initial_collateral_token,
        
        swap_path,
        TRY_CAST(order_type AS INTEGER) AS order_type,
        TRY_CAST(decrease_position_swap_type AS INTEGER) AS decrease_position_swap_type,
        TRY_CAST(size_delta_usd AS DOUBLE) AS size_delta_usd,
        TRY_CAST(initial_collateral_delta_amount AS DOUBLE) AS initial_collateral_delta_amount,
        TRY_CAST(trigger_price AS DOUBLE) AS trigger_price,
        TRY_CAST(acceptable_price AS DOUBLE) AS acceptable_price,
        TRY_CAST(execution_fee AS DOUBLE) AS execution_fee,
        TRY_CAST(callback_gas_limit AS DOUBLE) AS callback_gas_limit,
        TRY_CAST(min_output_amount AS DOUBLE) AS min_output_amount,
        TRY_CAST(updated_at_block AS BIGINT) AS updated_at_block,
        TRY_CAST(updated_at_time AS DOUBLE) AS updated_at_time,
        TRY_CAST(is_long AS BOOLEAN) AS is_long,
        TRY_CAST(should_unwrap_native_token AS BOOLEAN) AS should_unwrap_native_token,
        TRY_CAST(is_frozen AS BOOLEAN) AS is_frozen,
        from_hex(key) AS key
        
    FROM evt_data AS ED
    LEFT JOIN evt_data_parsed AS EDP
        ON ED.tx_hash = EDP.tx_hash
            AND ED.index = EDP.index
)

-- full data 
, full_data AS (
    SELECT 
        blockchain,
        block_time,
        DATE(block_time) AS block_date,
        block_number,
        tx_hash,
        index,
        contract_address,
        event_name,
        msg_sender,
        
        account,
        receiver,
        callback_contract,
        ui_fee_receiver,
        ED.market,
        ED.initial_collateral_token,
        swap_path,
        
        CASE 
            WHEN order_type = 0 THEN 'MarketSwap'
            WHEN order_type = 1 THEN 'LimitSwap'
            WHEN order_type = 2 THEN 'MarketIncrease'
            WHEN order_type = 3 THEN 'LimitIncrease'
            WHEN order_type = 4 THEN 'MarketDecrease'
            WHEN order_type = 5 THEN 'LimitDecrease'
            WHEN order_type = 6 THEN 'StopLossDecrease'
            WHEN order_type = 7 THEN 'Liquidation'
            ELSE NULL
        END AS order_type,
        CASE 
            WHEN decrease_position_swap_type = 0 THEN 'NoSwap'
            WHEN decrease_position_swap_type = 1 THEN 'SwapPnlTokenToCollateralToken'
            WHEN decrease_position_swap_type = 2 THEN 'SwapCollateralTokenToPnlToken'
            ELSE NULL
        END AS decrease_position_swap_type,
        
        size_delta_usd / POWER(10, 30) AS size_delta_usd,
        initial_collateral_delta_amount / POWER(10, collateral_token_decimals) AS initial_collateral_delta_amount,
        trigger_price AS trigger_price_raw,
        CASE 
            WHEN index_token_decimals IS NULL THEN NULL
            ELSE trigger_price / POWER(10, 30 - index_token_decimals) 
        END AS trigger_price,
        acceptable_price AS acceptable_price_raw,
        CASE 
            WHEN index_token_decimals IS NULL THEN NULL
            ELSE acceptable_price / POWER(10, 30 - index_token_decimals) 
        END AS acceptable_price,
        execution_fee / POWER(10, 18) AS execution_fee,
        callback_gas_limit,
        min_output_amount AS min_output_amount_raw, 

        updated_at_block,
        CASE 
            WHEN updated_at_time = 0 THEN NULL
            ELSE updated_at_time
        END AS updated_at_time,
        is_long,
        should_unwrap_native_token,
        is_frozen,
        key

    FROM event_data AS ED
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_markets_data') }} AS MD
        ON ED.market = MD.market
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_collateral_tokens_data') }} AS CTD
        ON ED.initial_collateral_token = CTD.collateral_token
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}
