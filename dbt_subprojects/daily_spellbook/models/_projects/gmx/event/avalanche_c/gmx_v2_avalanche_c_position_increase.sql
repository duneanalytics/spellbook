{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'position_increase',
    materialized = 'incremental',
    unique_key = ['tx_hash', 'index'],
    incremental_strategy = 'merge'
    )
}}

{%- set event_name = 'PositionIncrease' -%}
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
        json_query(data, 'lax $.intItems' OMIT QUOTES) AS int_items,
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

, int_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(int_items, '$.items') AS ARRAY(JSON))
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
    FROM int_items_parsed
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
        MAX(CASE WHEN key_name = 'market' THEN value END) AS market,
        MAX(CASE WHEN key_name = 'collateralToken' THEN value END) AS collateral_token,
        MAX(CASE WHEN key_name = 'sizeInUsd' THEN value END) AS size_in_usd,
        MAX(CASE WHEN key_name = 'sizeInTokens' THEN value END) AS size_in_tokens,
        MAX(CASE WHEN key_name = 'collateralAmount' THEN value END) AS collateral_amount,
        MAX(CASE WHEN key_name = 'borrowingFactor' THEN value END) AS borrowing_factor,
        MAX(CASE WHEN key_name = 'fundingFeeAmountPerSize' THEN value END) AS funding_fee_amount_per_size,
        MAX(CASE WHEN key_name = 'longTokenClaimableFundingAmountPerSize' THEN value END) AS long_token_claimable_funding_amount_per_size,
        MAX(CASE WHEN key_name = 'shortTokenClaimableFundingAmountPerSize' THEN value END) AS short_token_claimable_funding_amount_per_size,
        MAX(CASE WHEN key_name = 'executionPrice' THEN value END) AS execution_price,
        MAX(CASE WHEN key_name = 'indexTokenPrice.max' THEN value END) AS index_token_price_max,
        MAX(CASE WHEN key_name = 'indexTokenPrice.min' THEN value END) AS index_token_price_min,
        MAX(CASE WHEN key_name = 'collateralTokenPrice.max' THEN value END) AS collateral_token_price_max,
        MAX(CASE WHEN key_name = 'collateralTokenPrice.min' THEN value END) AS collateral_token_price_min,
        MAX(CASE WHEN key_name = 'sizeDeltaUsd' THEN value END) AS size_delta_usd,
        MAX(CASE WHEN key_name = 'sizeDeltaInTokens' THEN value END) AS size_delta_in_tokens,
        MAX(CASE WHEN key_name = 'orderType' THEN value END) AS order_type,
        MAX(CASE WHEN key_name = 'increasedAtTime' THEN value END) AS increased_at_time,
        MAX(CASE WHEN key_name = 'collateralDeltaAmount' THEN value END) AS collateral_delta_amount,
        MAX(CASE WHEN key_name = 'priceImpactUsd' THEN value END) AS price_impact_usd,
        MAX(CASE WHEN key_name = 'priceImpactAmount' THEN value END) AS price_impact_amount,
        MAX(CASE WHEN key_name = 'isLong' THEN value END) AS is_long,
        MAX(CASE WHEN key_name = 'orderKey' THEN value END) AS order_key,
        MAX(CASE WHEN key_name = 'positionKey' THEN value END) AS position_key
        
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
        from_hex(market) AS market,
        from_hex(collateral_token) AS collateral_token,
        TRY_CAST(size_in_usd AS DOUBLE) AS size_in_usd,
        TRY_CAST(size_in_tokens AS DOUBLE) AS size_in_tokens,
        TRY_CAST(collateral_amount AS DOUBLE) AS collateral_amount,
        TRY_CAST(borrowing_factor AS DOUBLE) AS borrowing_factor,
        TRY_CAST(funding_fee_amount_per_size AS DOUBLE) AS funding_fee_amount_per_size,
        TRY_CAST(long_token_claimable_funding_amount_per_size AS DOUBLE) AS long_token_claimable_funding_amount_per_size,
        TRY_CAST(short_token_claimable_funding_amount_per_size AS DOUBLE) AS short_token_claimable_funding_amount_per_size,
        TRY_CAST(execution_price AS DOUBLE) AS execution_price,        
        TRY_CAST(index_token_price_max AS DOUBLE) AS index_token_price_max,        
        TRY_CAST(index_token_price_min AS DOUBLE) AS index_token_price_min,        
        TRY_CAST(collateral_token_price_max AS DOUBLE) AS collateral_token_price_max,        
        TRY_CAST(collateral_token_price_min AS DOUBLE) AS collateral_token_price_min,        
        TRY_CAST(size_delta_usd AS DOUBLE) AS size_delta_usd,        
        TRY_CAST(size_delta_in_tokens AS DOUBLE) AS size_delta_in_tokens,        
        TRY_CAST(order_type AS INTEGER) AS order_type,        
        TRY_CAST(increased_at_time AS DOUBLE) AS increased_at_time,        
        TRY_CAST(collateral_delta_amount AS DOUBLE) AS collateral_delta_amount,
        TRY_CAST(price_impact_usd AS DOUBLE) AS price_impact_usd,
        TRY_CAST(price_impact_amount AS DOUBLE) AS price_impact_amount,
        TRY_CAST(is_long AS BOOLEAN) AS is_long,
        from_hex(order_key) AS order_key,
        from_hex(position_key) AS position_key
        
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
        ED.market AS market,
        ED.collateral_token AS collateral_token,
        size_in_usd / POWER(10, 30) AS size_in_usd,
        size_in_tokens / POWER(10, index_token_decimals) AS size_in_tokens,
        collateral_amount / POWER(10, collateral_token_decimals) AS collateral_amount,
        borrowing_factor / POWER(10, 30) AS borrowing_factor,
        funding_fee_amount_per_size / POWER(10, collateral_token_decimals + 15) AS funding_fee_amount_per_size, 
        long_token_claimable_funding_amount_per_size / POWER(10, long_token_decimals + 15) AS long_token_claimable_funding_amount_per_size,
        short_token_claimable_funding_amount_per_size / POWER(10, short_token_decimals + 15) AS short_token_claimable_funding_amount_per_size,
        execution_price / POWER(10, 30 - index_token_decimals) AS execution_price, 
        index_token_price_max / POWER(10, 30 - index_token_decimals) AS index_token_price_max, 
        index_token_price_min / POWER(10, 30 - index_token_decimals) AS index_token_price_min, 
        collateral_token_price_max / POWER(10, 30 - collateral_token_decimals) AS collateral_token_price_max,
        collateral_token_price_min / POWER(10, 30 - collateral_token_decimals) AS collateral_token_price_min,
        size_delta_usd / POWER(10, 30) AS size_delta_usd,
        size_delta_in_tokens / POWER(10, index_token_decimals) AS size_delta_in_tokens,
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
            WHEN increased_at_time = 0 THEN NULL
            ELSE increased_at_time
        END AS increased_at_time,
        collateral_delta_amount / POWER(10, collateral_token_decimals) AS collateral_delta_amount,
        price_impact_usd / POWER(10, 30) AS price_impact_usd,
        price_impact_amount / POWER(10, index_token_decimals) AS price_impact_amount,  
        is_long,
        order_key,
        position_key
        
    FROM event_data AS ED
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_markets_data') }} AS MD
        ON ED.market = MD.market
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_collateral_tokens_data') }} AS CTD
        ON ED.collateral_token = CTD.collateral_token
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}
