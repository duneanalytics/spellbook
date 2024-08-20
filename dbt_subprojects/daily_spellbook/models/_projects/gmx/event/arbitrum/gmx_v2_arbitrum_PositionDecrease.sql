{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'position_decrease',
    materialized = 'table'
  )
}}

{%- set event_name = 'PositionDecrease' -%}
    
-- Create Arrays for Each Variable Type
{%- set addresses = [
    'market',
    'collateralToken'
] -%}

{%- set unsigned_integers = [
    'sizeInUsd',
    'sizeInTokens',
    'collateralAmount',
    'borrowingFactor',
    'fundingFeeAmountPerSize',
    'longTokenClaimableFundingAmountPerSize',
    'shortTokenClaimableFundingAmountPerSize',
    'executionPrice',
    'indexTokenPrice.max',
    'indexTokenPrice.min',
    'collateralTokenPrice.max',
    'collateralTokenPrice.min',
    'sizeDeltaUsd',
    'sizeDeltaInTokens',
    'collateralDeltaAmount',
    'values.priceImpactDiffUsd',
    'orderType',
    'decreasedAtTime'
] -%}

{%- set integers = [
    'priceImpactUsd',
    'basePnlUsd',
    'uncappedBasePnlUsd'
] -%}

{%- set booleans = [
    'isLong'
] -%}

{%- set bytes32 = [
    'orderKey',
    'positionKey'
] %}

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
    '{{ event_name }}' as eventName,

    -- Extracting Addresses
    {% for var in addresses -%}
        {{ process_variable(var, 'address', 52, 20) }},
    {% endfor -%}

    -- Extracting Unsigned Integers
    {% for var in unsigned_integers -%}
        {{ process_variable(var, 'unsigned_integer', 64, 32) }},
    {% endfor -%}

    -- Extracting Integers
    {% for var in integers -%}
        {{ process_variable(var, 'integer', 64, 32) }},
    {% endfor -%}

    -- Extracting Booleans
    {% for var in booleans -%}
        {{ process_variable(var, 'boolean', 64, 32) }},
    {% endfor -%}

    -- Extracting Bytes32
    {% for var in bytes32 -%}
        {{ process_variable(var, 'bytes32', 64, 32) }}{%- if not loop.last -%},{%- endif %}
    {% endfor %}

FROM
    {{ source('arbitrum', 'logs') }}
WHERE
    contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
    AND topic1 = keccak(to_utf8('{{ event_name }}'))
