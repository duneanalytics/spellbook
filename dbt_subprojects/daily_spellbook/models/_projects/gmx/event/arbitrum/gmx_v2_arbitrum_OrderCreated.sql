{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'order_created',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "gmx",
                                \'["ai_data_master","gmx-io"]\') }}'
    )
}}

{%- set event_name = 'OrderCreated' -%}
{%- set blockchain_name = 'arbitrum' -%}
{%- set addresses = [
    ['account', 'account_'],
    ['receiver', 'receiver'],
    ['callbackContract', 'callback_contract'],
    ['uiFeeReceiver', 'ui_fee_receiver'],
    ['market', 'market'],
    ['initialCollateralToken', 'initial_collateral_token'],
] -%}
{%- set address_array_items = [
    ['swapPath', 'swap_path'],
] -%}
{%- set unsigned_integers = [
    ['orderType', 'order_type'],
    ['decreasePositionSwapType', 'decrease_position_swap_type'],
    ['sizeDeltaUsd', 'size_delta_usd'],
    ['initialCollateralDeltaAmount', 'initial_collateral_delta_amount'],
    ['triggerPrice', 'trigger_price'],
    ['acceptablePrice', 'acceptable_price'],
    ['executionFee', 'execution_fee'],
    ['callbackGasLimit', 'callback_gas_limit'],
    ['minOutputAmount', 'min_output_amount'],
    ['updatedAtBlock', 'updated_at_block'],
    ['updatedAtTime', 'updated_at_time'],
] -%}
{%- set booleans = [
    ['isLong', 'is_long'],
    ['shouldUnwrapNativeToken', 'should_unwrap_native_token'],
    ['isFrozen', 'is_frozen'],
] -%}
{%- set bytes32 = [
    ['key', 'key'],
] -%}

WITH event_data AS (
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
        
        -- Extracting Address
        {% for var in addresses -%}
            {{ process_variable(var[0], var[1], 'address', 52, 20) }},
        {% endfor -%}   

        -- Extracting Unsigned Integers
        {% for var in unsigned_integers -%}
            {{ process_variable(var[0], var[1], 'unsigned_integer', 64, 32) }},
        {% endfor -%} 

        -- Extracting Booleans
        {% for var in booleans -%}
            {{ process_variable(var[0], var[1], 'boolean', 64, 32) }},
        {% endfor -%} 
        
        -- Extracting Bytes32
        {% for var in bytes32 -%}
            {{ process_variable(var[0], var[1], 'bytes32', 64, 32) }},
        {% endfor -%}         

        -- Extracting Address Array Items
        {% for var in address_array_items -%}
            {{ process_variable(var[0], var[1], 'address_array_items', 32, 32) }},
        {% endfor -%} 
        data
    
    FROM
        {{ source(blockchain_name, 'logs') }}
    WHERE
        contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
        AND topic1 = keccak(to_utf8('{{ event_name }}'))
    ORDER BY block_time ASC
)

, sequence_data AS (
    SELECT
        tx_hash,
        key,
        SEQUENCE(1, TRY_CAST(swap_path_n AS INTEGER), 1) AS market_indices, 
        data
    FROM event_data
    WHERE swap_path_n > 0
    ORDER BY key ASC
)

, swap_markets_data AS (
    SELECT 
        tx_hash, 
        key,
        JSON_FORMAT(CAST(ARRAY_AGG(market) AS JSON)) AS swap_path
    FROM (
        SELECT
            tx_hash, 
            key, 
            TRY_CAST(
                varbinary_substring(
                    varbinary_substring(data, varbinary_position(data, to_utf8('swapPath')) + 32 + market_index * 32, 32), 
                    13, 20)
            AS VARCHAR) AS market
        FROM sequence_data, UNNEST(market_indices) AS t(market_index)    
    )
    GROUP BY tx_hash, key
)

-- Filter relevant tokens
, collateral_tokens_data AS (
    SELECT 
        contract_address AS collateral_token, 
        decimals AS collateral_token_decimals
    FROM 
        {{ ref('gmx_v2_arbitrum_erc20') }}
)

SELECT 
    blockchain,
    block_time,
    block_date,
    block_number,
    ED.tx_hash,
    index,
    tx_index,
    tx_from,
    tx_to,
    contract_address,
    account,
    event_name,
    
    account_,
    receiver,
    callback_contract,
    ui_fee_receiver,
    ED.market,
    ED.initial_collateral_token,

    COALESCE(CAST(SMD.swap_path AS VARCHAR), '[]') AS swap_path,
    swap_path_n, 
    
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
    trigger_price / POWER(10, 30) AS trigger_price,
    acceptable_price / POWER(10, 30) AS acceptable_price,
    execution_fee / POWER(10, 18) AS execution_fee,
    callback_gas_limit,
    min_output_amount AS min_output_amount, 

    updated_at_block,
    CASE 
        WHEN updated_at_time = 0 THEN NULL
        ELSE updated_at_time
    END AS updated_at_time,
    CASE 
        WHEN is_long = 1 THEN true
        ELSE false
    END AS is_long,
    CASE 
        WHEN should_unwrap_native_token = 1 THEN true
        ELSE false
    END AS should_unwrap_native_token,
    CASE 
        WHEN is_frozen = 1 THEN true
        ELSE false
    END AS is_frozen,    
    
    ED.key
    
FROM event_data AS ED
LEFT JOIN swap_markets_data AS SMD
    ON TRY_CAST(ED.key AS VARCHAR) = TRY_CAST(SMD.key AS VARCHAR)
        AND ED.tx_hash = SMD.tx_hash
LEFT JOIN collateral_tokens_data AS CTD
    ON ED.initial_collateral_token = CTD.collateral_token


