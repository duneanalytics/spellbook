{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'position_decrease',
    materialized = 'table'
  )
}}

{%- set event_name = 'PositionDecrease' -%}
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
    ['collateralDeltaAmount', 'collateral_delta_amount'],
    ['values.priceImpactDiffUsd', 'values_price_impact_diff_usd'],
    ['orderType', 'order_type'],
    ['decreasedAtTime', 'decreased_at_time']
] -%}
{%- set integers = [
    ['priceImpactUsd', 'price_impact_usd'],
    ['basePnlUsd', 'base_pnl_usd'],
    ['uncappedBasePnlUsd', 'uncapped_base_pnl_usd']
] -%}
{%- set booleans = [
    ['isLong', 'is_long']
] -%}
{%- set bytes32 = [
    ['orderKey', 'order_key'],
    ['positionKey', 'position_key']
] -%}

SELECT
    -- Main Variables
    'arbitrum' as blockchain,
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
    {{ source('arbitrum', 'logs') }}
WHERE
    contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
    AND topic1 = keccak(to_utf8('{{ event_name }}'))
