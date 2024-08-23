{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'position_increase',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "gmx",
                                \'["ai_data_master","gmx-io"]\') }}'
  )
}}

{%- set event_name = 'PositionIncrease' -%}
{%- set blockchain_name = 'arbitrum' -%}
{%- set addresses = [
    ['market', 'market'],
    ['collateralToken', 'collateral_token']
] -%}
{%- set unsigned_integers = [
    ['sizeInUsd', 'size_in_usd'],
    ['sizeInTokens', 'size_in_tokens'],
    ['collateralAmount', 'collateral_amount'],
    ['borrowingFactor', 'borrowing_factor'],
    ['fundingFeeAmountPerSize', 'funding_fee_amount_per_size'],
    ['longTokenClaimableFundingAmountPerSize', 'long_token_claimable_funding_amount_per_size'],
    ['shortTokenClaimableFundingAmountPerSize', 'short_token_claimable_funding_amount_per_size'],
    ['executionPrice', 'execution_price'],
    ['indexTokenPrice.max', 'index_token_price_max'],
    ['indexTokenPrice.min', 'index_token_price_min'],
    ['collateralTokenPrice.max', 'collateral_token_price_max'],
    ['collateralTokenPrice.min', 'collateral_token_price_min'],
    ['sizeDeltaUsd', 'size_delta_usd'],
    ['sizeDeltaInTokens', 'size_delta_in_tokens'],
    ['orderType', 'order_type'],
    ['increasedAtTime', 'increased_at_time']
] -%}
{%- set integers = [
    ['collateralDeltaAmount', 'collateral_delta_amount'],
    ['priceImpactUsd', 'price_impact_usd'],
    ['priceImpactAmount', 'price_impact_amount']
] -%}
{%- set booleans = [
    ['isLong', 'is_long']
] -%}
{%- set bytes32 = [
    ['orderKey', 'order_key'],
    ['positionKey', 'position_key']
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
            {{ process_variable(var[0], var[1], 'unsigned_integer', 64, 32) }},
        {% endfor -%}

        -- Extracting Integers
        {% for var in integers -%}
            {{ process_variable(var[0], var[1], 'integer', 64, 32) }},
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
    CASE 
        WHEN is_long = 1 THEN true
        ELSE false
    END AS is_long,
    order_key,
    position_key
    
FROM event_data AS ED
LEFT JOIN markets_data AS MD
    ON ED.market = MD.market
LEFT JOIN collateral_tokens_data AS CTD
    ON ED.collateral_token = CTD.collateral_token  
