{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'position_fees_collected',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "gmx",
                                \'["ai_data_master","gmx-io"]\') }}'
  )
}}

{%- set event_name = 'PositionFeesCollected' -%}
{%- set blockchain_name = 'arbitrum' -%}
{%- set addresses = [
    ['market', 'market'],
    ['collateralToken', 'collateral_token'],
    ['affiliate', 'affiliate'],
    ['trader', 'trader'],
    ['uiFeeReceiver', 'ui_fee_receiver']
] -%}
{%- set unsigned_integers = [
    ['collateralTokenPrice.min', 'collateral_token_price_min'],
    ['collateralTokenPrice.max', 'collateral_token_price_max'],
    ['tradeSizeUsd', 'trade_size_usd'],
    ['totalRebateFactor', 'total_rebate_factor'],
    ['traderDiscountFactor', 'trader_discount_factor'],
    ['totalRebateAmount', 'total_rebate_amount'],
    ['traderDiscountAmount', 'trader_discount_amount'],
    ['affiliateRewardAmount', 'affiliate_reward_amount'],
    ['fundingFeeAmount', 'funding_fee_amount'],
    ['claimableLongTokenAmount', 'claimable_long_token_amount'],
    ['claimableShortTokenAmount', 'claimable_short_token_amount'],
    ['latestFundingFeeAmountPerSize', 'latest_funding_fee_amount_per_size'],
    ['latestLongTokenClaimableFundingAmountPerSize', 'latest_long_token_claimable_funding_amount_per_size'],
    ['latestShortTokenClaimableFundingAmountPerSize', 'latest_short_token_claimable_funding_amount_per_size'],
    ['borrowingFeeUsd', 'borrowing_fee_usd'],
    ['borrowingFeeAmount', 'borrowing_fee_amount'],
    ['borrowingFeeReceiverFactor', 'borrowing_fee_receiver_factor'],
    ['borrowingFeeAmountForFeeReceiver', 'borrowing_fee_amount_for_fee_receiver'],
    ['positionFeeFactor', 'position_fee_factor'],
    ['protocolFeeAmount', 'protocol_fee_amount'],
    ['positionFeeReceiverFactor', 'position_fee_receiver_factor'],
    ['feeReceiverAmount', 'fee_receiver_amount'],
    ['feeAmountForPool', 'fee_amount_for_pool'],
    ['positionFeeAmountForPool', 'position_fee_amount_for_pool'],
    ['positionFeeAmount', 'position_fee_amount'],
    ['totalCostAmount', 'total_cost_amount'],
    ['uiFeeReceiverFactor', 'ui_fee_receiver_factor'],
    ['uiFeeAmount', 'ui_fee_amount']
] -%}
{%- set booleans = [
    ['isIncrease', 'is_increase']
] -%}
{%- set bytes32 = [
    ['orderKey', 'order_key'],
    ['positionKey', 'position_key'],
    ['referralCode', 'referral_code']
] -%}

-- get all markets
WITH markets_data AS (
    SELECT
        MCE.market_token AS market,
        ERC20_IT.decimals AS index_token_decimals,
        ERC20_LT.decimals AS long_token_decimals,
        ERC20_ST.decimals AS short_token_decimals  
    FROM {{ ref('gmx_v2_arbitrum_MarketCreated') }} AS MCE
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_IT
        ON ERC20_IT.contract_address = MCE.index_token
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_LT
        ON ERC20_LT.contract_address = MCE.long_token 
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_ST
        ON ERC20_ST.contract_address = MCE.short_token
)

-- Filter relevant tokens
, collateral_tokens_data AS (
    SELECT 
        contract_address AS collateral_token, 
        decimals AS collateral_token_decimals
    FROM 
        {{ ref('gmx_v2_arbitrum_erc20') }}
)

-- get event data
, event_data AS (
    SELECT
        -- Main Variables
        '{{ blockchain_name }}' as blockchain,
        block_time,
        block_date,
        block_number, 
        tx_hash,
        index,
        tx_index,
        tx_from,
        tx_to,    
        contract_address,
        varbinary_substring(topic2, 13, 20) as account,
        '{{ event_name }}' as event_name,

        -- Extracting Addresses
        {% for var in addresses -%}
            {{ process_variable(var[0], var[1], 'address', 52, 20) }},
        {% endfor -%}

        -- Extracting Unsigned Integers
        {% for var in unsigned_integers -%}
            {% if var[0] == 'positionFeeAmount' -%}
                {{ process_variable(var[0], var[1], 'unsigned_integer', 64, 32, exception=true) }},
            {%- else -%}
                {{ process_variable(var[0], var[1], 'unsigned_integer', 64, 32) }},
            {%- endif %}            
        {% endfor -%}

        -- Extracting Booleans
        {% for var in booleans -%}
            {{ process_variable(var[0], var[1], 'boolean', 64, 32) }},
        {% endfor -%}

        -- Extracting Bytes32
        {% for var in bytes32 -%}
            {{ process_variable(var[0], var[1], 'bytes32', 64, 32) }}{%- if not loop.last -%},{%- endif %}
        {% endfor %}

    FROM
        {{ source(blockchain_name, 'logs') }}
    WHERE
        contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
        AND topic1 = keccak(to_utf8('{{ event_name }}'))
)

-- process all columns
SELECT 
    blockchain,
    block_time,
    block_date,
    block_number,
    tx_hash,
    index,
    tx_index,
    tx_from,
    tx_to,
    contract_address,
    account,
    event_name,
    
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
    CASE 
        WHEN is_increase = 1 THEN true
        ELSE false
    END AS is_increase,
    order_key,
    position_key,
    referral_code

FROM event_data AS ED
LEFT JOIN markets_data AS MD
    ON ED.market = MD.market
LEFT JOIN collateral_tokens_data AS CT
    ON ED.collateral_token = CT.collateral_token