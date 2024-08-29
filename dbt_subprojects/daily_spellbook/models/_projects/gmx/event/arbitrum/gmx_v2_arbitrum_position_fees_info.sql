{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'position_fees_info',
    materialized = 'table'
    )
}}

{%- set event_name = 'PositionFeesInfo' -%}
{%- set blockchain_name = 'arbitrum' -%}

-- get all markets
WITH markets_data AS (
    SELECT
        MCE.market_token AS market,
        ERC20_IT.decimals AS index_token_decimals,
        ERC20_LT.decimals AS long_token_decimals,
        ERC20_ST.decimals AS short_token_decimals  
    FROM {{ ref('gmx_v2_arbitrum_market_created') }} AS MCE
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_IT
        ON ERC20_IT.contract_address = MCE.index_token
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_LT
        ON ERC20_LT.contract_address = MCE.long_token 
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_ST
        ON ERC20_ST.contract_address = MCE.short_token
)

, evt_data_1 AS (
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
    FROM {{ source('gmx_v2_arbitrum','EventEmitter_evt_EventLog1')}}
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
    FROM {{ source('gmx_v2_arbitrum','EventEmitter_evt_EventLog2')}}
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
        
        MAX(CASE WHEN key_name = 'market' THEN value END) AS market,
        MAX(CASE WHEN key_name = 'collateralToken' THEN value END) AS collateral_token,
        MAX(CASE WHEN key_name = 'affiliate' THEN value END) AS affiliate,
        MAX(CASE WHEN key_name = 'trader' THEN value END) AS trader,
        MAX(CASE WHEN key_name = 'uiFeeReceiver' THEN value END) AS ui_fee_receiver,
        MAX(CASE WHEN key_name = 'collateralTokenPrice.min' THEN value END) AS collateral_token_price_min,
        MAX(CASE WHEN key_name = 'collateralTokenPrice.max' THEN value END) AS collateral_token_price_max,
        MAX(CASE WHEN key_name = 'tradeSizeUsd' THEN value END) AS trade_size_usd,
        MAX(CASE WHEN key_name = 'totalRebateFactor' THEN value END) AS total_rebate_factor,
        MAX(CASE WHEN key_name = 'traderDiscountFactor' THEN value END) AS trader_discount_factor,
        MAX(CASE WHEN key_name = 'totalRebateAmount' THEN value END) AS total_rebate_amount,
        MAX(CASE WHEN key_name = 'traderDiscountAmount' THEN value END) AS trader_discount_amount,
        MAX(CASE WHEN key_name = 'affiliateRewardAmount' THEN value END) AS affiliate_reward_amount,
        MAX(CASE WHEN key_name = 'fundingFeeAmount' THEN value END) AS funding_fee_amount,
        MAX(CASE WHEN key_name = 'claimableLongTokenAmount' THEN value END) AS claimable_long_token_amount,
        MAX(CASE WHEN key_name = 'claimableShortTokenAmount' THEN value END) AS claimable_short_token_amount,
        MAX(CASE WHEN key_name = 'latestFundingFeeAmountPerSize' THEN value END) AS latest_funding_fee_amount_per_size,
        MAX(CASE WHEN key_name = 'latestLongTokenClaimableFundingAmountPerSize' THEN value END) AS latest_long_token_claimable_funding_amount_per_size,
        MAX(CASE WHEN key_name = 'latestShortTokenClaimableFundingAmountPerSize' THEN value END) AS latest_short_token_claimable_funding_amount_per_size,
        MAX(CASE WHEN key_name = 'borrowingFeeUsd' THEN value END) AS borrowing_fee_usd,
        MAX(CASE WHEN key_name = 'borrowingFeeAmount' THEN value END) AS borrowing_fee_amount,
        MAX(CASE WHEN key_name = 'borrowingFeeReceiverFactor' THEN value END) AS borrowing_fee_receiver_factor,
        MAX(CASE WHEN key_name = 'borrowingFeeAmountForFeeReceiver' THEN value END) AS borrowing_fee_amount_for_fee_receiver,
        MAX(CASE WHEN key_name = 'positionFeeFactor' THEN value END) AS position_fee_factor,
        MAX(CASE WHEN key_name = 'protocolFeeAmount' THEN value END) AS protocol_fee_amount,
        MAX(CASE WHEN key_name = 'positionFeeReceiverFactor' THEN value END) AS position_fee_receiver_factor,
        MAX(CASE WHEN key_name = 'feeReceiverAmount' THEN value END) AS fee_receiver_amount,
        MAX(CASE WHEN key_name = 'feeAmountForPool' THEN value END) AS fee_amount_for_pool,
        MAX(CASE WHEN key_name = 'positionFeeAmountForPool' THEN value END) AS position_fee_amount_for_pool,
        MAX(CASE WHEN key_name = 'positionFeeAmount' THEN value END) AS position_fee_amount,
        MAX(CASE WHEN key_name = 'totalCostAmount' THEN value END) AS total_cost_amount,
        MAX(CASE WHEN key_name = 'uiFeeReceiverFactor' THEN value END) AS ui_fee_receiver_factor,
        MAX(CASE WHEN key_name = 'uiFeeAmount' THEN value END) AS ui_fee_amount,
        MAX(CASE WHEN key_name = 'isIncrease' THEN value END) AS is_increase,
        MAX(CASE WHEN key_name = 'orderKey' THEN value END) AS order_key,
        MAX(CASE WHEN key_name = 'positionKey' THEN value END) AS position_key,
        MAX(CASE WHEN key_name = 'referralCode' THEN value END) AS referral_code

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
        topic1, 
        topic2,
        
        TRY_CAST(market AS VARCHAR) AS market,
        TRY_CAST(collateral_token AS VARCHAR) AS collateral_token,
        TRY_CAST(affiliate AS VARCHAR) AS affiliate,
        TRY_CAST(trader AS VARCHAR) AS trader,
        TRY_CAST(ui_fee_receiver AS VARCHAR) AS ui_fee_receiver,
        TRY_CAST(collateral_token_price_min AS DOUBLE) AS collateral_token_price_min,
        TRY_CAST(collateral_token_price_max AS DOUBLE) AS collateral_token_price_max,
        TRY_CAST(trade_size_usd AS DOUBLE) AS trade_size_usd,
        TRY_CAST(total_rebate_factor AS DOUBLE) AS total_rebate_factor,
        TRY_CAST(trader_discount_factor AS DOUBLE) AS trader_discount_factor,
        TRY_CAST(total_rebate_amount AS DOUBLE) AS total_rebate_amount,
        TRY_CAST(trader_discount_amount AS DOUBLE) AS trader_discount_amount,
        TRY_CAST(affiliate_reward_amount AS DOUBLE) AS affiliate_reward_amount,
        TRY_CAST(funding_fee_amount AS DOUBLE) AS funding_fee_amount,
        TRY_CAST(claimable_long_token_amount AS DOUBLE) AS claimable_long_token_amount,
        TRY_CAST(claimable_short_token_amount AS DOUBLE) AS claimable_short_token_amount,
        TRY_CAST(latest_funding_fee_amount_per_size AS DOUBLE) AS latest_funding_fee_amount_per_size,
        TRY_CAST(latest_long_token_claimable_funding_amount_per_size AS DOUBLE) AS latest_long_token_claimable_funding_amount_per_size,
        TRY_CAST(latest_short_token_claimable_funding_amount_per_size AS DOUBLE) AS latest_short_token_claimable_funding_amount_per_size,
        TRY_CAST(borrowing_fee_usd AS DOUBLE) AS borrowing_fee_usd,
        TRY_CAST(borrowing_fee_amount AS DOUBLE) AS borrowing_fee_amount,
        TRY_CAST(borrowing_fee_receiver_factor AS DOUBLE) AS borrowing_fee_receiver_factor,
        TRY_CAST(borrowing_fee_amount_for_fee_receiver AS DOUBLE) AS borrowing_fee_amount_for_fee_receiver,
        TRY_CAST(position_fee_factor AS DOUBLE) AS position_fee_factor,
        TRY_CAST(protocol_fee_amount AS DOUBLE) AS protocol_fee_amount,
        TRY_CAST(position_fee_receiver_factor AS DOUBLE) AS position_fee_receiver_factor,
        TRY_CAST(fee_receiver_amount AS DOUBLE) AS fee_receiver_amount,
        TRY_CAST(fee_amount_for_pool AS DOUBLE) AS fee_amount_for_pool,
        TRY_CAST(position_fee_amount_for_pool AS DOUBLE) AS position_fee_amount_for_pool,
        TRY_CAST(position_fee_amount AS DOUBLE) AS position_fee_amount,
        TRY_CAST(total_cost_amount AS DOUBLE) AS total_cost_amount,
        TRY_CAST(ui_fee_receiver_factor AS DOUBLE) AS ui_fee_receiver_factor,
        TRY_CAST(ui_fee_amount AS DOUBLE) AS ui_fee_amount,
        TRY_CAST(is_increase AS BOOLEAN) AS is_increase,
        TRY_CAST(order_key AS VARCHAR) AS order_key,
        TRY_CAST(position_key AS VARCHAR) AS position_key,
        TRY_CAST(referral_code AS VARCHAR) AS referral_code
        
    FROM evt_data AS ED
    LEFT JOIN evt_data_parsed AS EDP
        ON ED.tx_hash = EDP.tx_hash
            AND ED.index = EDP.index
)

-- process all columns
SELECT 
    blockchain,
    block_time,
    block_number,
    tx_hash,
    index,
    contract_address,
    event_name,
    msg_sender,
    topic1,
    topic2,
    
    ED.market AS market,
    ED.collateral_token,
    affiliate,
    trader,
    ui_fee_receiver,
    
    collateral_token_price_min / POWER(10, 30 - collateral_token_decimals) AS collateral_token_price_min,
    collateral_token_price_max / POWER(10, 30 - collateral_token_decimals) AS collateral_token_price_max,    
    trade_size_usd / POWER(10, 30) AS trade_size_usd,
    total_rebate_factor / POWER(10, 30) AS total_rebate_factor,
    trader_discount_factor / POWER(10, 30) AS trader_discount_factor,
    total_rebate_amount / POWER(10, collateral_token_decimals) AS total_rebate_amount,
    trader_discount_amount / POWER(10, collateral_token_decimals) AS trader_discount_amount,
    affiliate_reward_amount / POWER(10, collateral_token_decimals) AS affiliate_reward_amount,
    funding_fee_amount / POWER(10, collateral_token_decimals + 15) AS funding_fee_amount,
    claimable_long_token_amount / POWER(10, long_token_decimals + 15) AS claimable_long_token_amount,
    claimable_short_token_amount / POWER(10, short_token_decimals + 15) AS claimable_short_token_amount,
    latest_funding_fee_amount_per_size / POWER(10, collateral_token_decimals + 15) AS latest_funding_fee_amount_per_size,
    latest_long_token_claimable_funding_amount_per_size / POWER(10, long_token_decimals + 15) AS latest_long_token_claimable_funding_amount_per_size,
    latest_short_token_claimable_funding_amount_per_size / POWER(10, short_token_decimals + 15) AS latest_short_token_claimable_funding_amount_per_size,
    borrowing_fee_usd / POWER(10, 30) AS borrowing_fee_usd,
    borrowing_fee_amount / POWER(10, collateral_token_decimals) AS borrowing_fee_amount,
    borrowing_fee_receiver_factor / POWER(10, 30) AS borrowing_fee_receiver_factor,
    borrowing_fee_amount_for_fee_receiver / POWER(10, collateral_token_decimals) AS borrowing_fee_amount_for_fee_receiver,
    position_fee_factor / POWER(10, 30) AS position_fee_factor,
    protocol_fee_amount / POWER(10, collateral_token_decimals) AS protocol_fee_amount,
    position_fee_receiver_factor / POWER(10, 30) AS position_fee_receiver_factor,
    fee_receiver_amount / POWER(10, collateral_token_decimals) AS fee_receiver_amount,
    fee_amount_for_pool / POWER(10, collateral_token_decimals) AS fee_amount_for_pool,
    position_fee_amount_for_pool / POWER(10, collateral_token_decimals) AS position_fee_amount_for_pool,
    position_fee_amount / POWER(10, collateral_token_decimals) AS position_fee_amount,
    total_cost_amount / POWER(10, collateral_token_decimals) AS total_cost_amount,
    ui_fee_receiver_factor / POWER(10, 30) AS ui_fee_receiver_factor,
    ui_fee_amount / POWER(10, collateral_token_decimals) AS ui_fee_amount,
    is_increase,
    order_key,
    position_key,
    referral_code
    
FROM event_data AS ED
LEFT JOIN markets_data AS MD
    ON ED.market = TRY_CAST(MD.market AS VARCHAR)
LEFT JOIN {{ ref('gmx_v2_arbitrum_collateral_tokens_data') }} AS CTD
    ON ED.collateral_token = TRY_CAST(CTD.collateral_token AS VARCHAR)


