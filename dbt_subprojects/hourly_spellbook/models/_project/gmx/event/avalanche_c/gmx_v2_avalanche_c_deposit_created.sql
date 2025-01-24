{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'deposit_created',
    materialized = 'incremental',
    unique_key = ['tx_hash', 'index'],
    incremental_strategy = 'merge'
    )
}}

{%- set event_name = 'DepositCreated' -%}
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
    UNION ALL
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
        MAX(CASE WHEN key_name = 'market' THEN value END) AS market,
        MAX(CASE WHEN key_name = 'initialLongToken' THEN value END) AS initial_long_token,
        MAX(CASE WHEN key_name = 'initialShortToken' THEN value END) AS initial_short_token,
        MAX(CASE WHEN key_name = 'longTokenSwapPath' THEN value END) AS long_token_swap_path,
        MAX(CASE WHEN key_name = 'shortTokenSwapPath' THEN value END) AS short_token_swap_path,
        
        MAX(CASE WHEN key_name = 'initialLongTokenAmount' THEN value END) AS initial_long_token_amount,
        MAX(CASE WHEN key_name = 'initialShortTokenAmount' THEN value END) AS initial_short_token_amount,
        MAX(CASE WHEN key_name = 'minMarketTokens' THEN value END) AS min_market_tokens,
        MAX(CASE WHEN key_name = 'updatedAtTime' THEN value END) AS updated_at_time,
        MAX(CASE WHEN key_name = 'executionFee' THEN value END) AS execution_fee,
        MAX(CASE WHEN key_name = 'callbackGasLimit' THEN value END) AS callback_gas_limit,
        MAX(CASE WHEN key_name = 'depositType' THEN value END) AS deposit_type,
        
        MAX(CASE WHEN key_name = 'shouldUnwrapNativeToken' THEN value END) AS should_unwrap_native_token,
        
        MAX(CASE WHEN key_name = 'key' THEN value END) AS "key"

    FROM
        combined
    GROUP BY tx_hash, index
)

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
        from_hex(market) AS market,
        from_hex(initial_long_token) AS initial_long_token,
        from_hex(initial_short_token) AS initial_short_token,
        long_token_swap_path,
        short_token_swap_path,

        TRY_CAST(initial_long_token_amount AS DOUBLE) initial_long_token_amount, 
        TRY_CAST(initial_short_token_amount AS DOUBLE) initial_short_token_amount, 
        TRY_CAST(min_market_tokens AS DOUBLE) min_market_tokens,
        TRY_CAST(updated_at_time AS DOUBLE) AS updated_at_time,
        TRY_CAST(execution_fee AS DOUBLE) AS execution_fee,
        TRY_CAST(callback_gas_limit AS DOUBLE) AS callback_gas_limit,
        TRY_CAST(deposit_type AS INTEGER) AS deposit_type,
        
        TRY_CAST(should_unwrap_native_token AS BOOLEAN) AS should_unwrap_native_token,
        
        from_hex("key") AS "key"

    FROM evt_data AS ED
    LEFT JOIN evt_data_parsed AS EDP
        ON ED.tx_hash = EDP.tx_hash
            AND ED.index = EDP.index
)

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
        ED.market,
        initial_long_token,
        initial_short_token,
        long_token_swap_path,
        short_token_swap_path,
        
        initial_long_token_amount / POWER(10, MD.long_token_decimals) AS initial_long_token_amount,
        initial_short_token_amount / POWER(10, MD.short_token_decimals) AS initial_short_token_amount,
        min_market_tokens / POWER(10, 18) AS min_market_tokens,
        CASE 
            WHEN updated_at_time = 0 THEN NULL
            ELSE updated_at_time
        END AS updated_at_time,
        execution_fee / POWER(10, 18) AS execution_fee,
        callback_gas_limit,
        deposit_type,
        
        should_unwrap_native_token,
        "key"
        
    FROM event_data AS ED
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_markets_data') }} AS MD
        ON ED.market = MD.market
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}


