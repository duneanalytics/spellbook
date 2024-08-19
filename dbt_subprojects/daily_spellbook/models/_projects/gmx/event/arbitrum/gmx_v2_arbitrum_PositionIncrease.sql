{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'position_increase',
    materialized = 'table'
  )
}}


{% set event_name = 'PositionIncrease' %}


SELECT
    'arbitrum' as blockchain,
    block_time,
    block_date,
    block_number, 
    contract_address,
    varbinary_substring (topic2, 13, 20) as account,
    tx_hash,
    index,
    tx_index,
    tx_from,
    tx_to,

    -- Extracting Addresses
    {{event_name}},
    varbinary_substring(data, varbinary_position(data, to_utf8('market')) - 52, 20) AS market, -- market_decimals (index token, long token, short token)
    varbinary_substring(data, varbinary_position(data, to_utf8('collateralToken')) - 52, 20) AS collateral_token, -- coll_token_decimals (long token or short token)

    -- Extracting Unsigned Integers
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('sizeInUsd')) - 64, 32)) AS size_in_usd, -- 30 decimals 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('sizeInTokens')) - 64, 32)) AS size_in_tokens, -- decimals index_token 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('collateralAmount')) - 64, 32)) AS collateral_amount, -- decimals collateralToken
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('borrowingFactor')) - 64, 32)) AS borrowing_factor, -- 30 decimals 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('fundingFeeAmountPerSize')) - 64, 32)) AS funding_fee_amount_per_size, -- collateral token decimals
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('longTokenClaimableFundingAmountPerSize')) - 64, 32)) AS long_token_claimable_funding_amount_per_size, --?
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('shortTokenClaimableFundingAmountPerSize')) - 64, 32)) AS short_token_claimable_funding_amount_per_size, --?
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('executionPrice')) - 64, 32)) AS execution_price, -- 30 - decimals index_token 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('indexTokenPrice.max')) - 64, 32)) AS index_token_price_max, -- 30 - decimals index_token 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('indexTokenPrice.min')) - 64, 32)) AS index_token_price_min, -- 30 - decimals index_token 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('collateralTokenPrice.max')) - 64, 32)) AS collateral_token_price_max, -- 30 - decimals collateralToken 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('collateralTokenPrice.min')) - 64, 32)) AS collateral_token_price_min, -- 30 - decimals collateralToken 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('sizeDeltaUsd')) - 64, 32)) AS size_delta_usd,  -- 30 decimals
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('sizeDeltaInTokens')) - 64, 32)) AS size_delta_in_tokens, --  decimals index_token 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('orderType')) - 64, 32)) AS order_type, 
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('increasedAtTime')) - 64, 32)) AS increased_at_time, -- unixtime 

    -- Extracting Integers
    varbinary_to_int256(varbinary_substring(data, varbinary_position(data, to_utf8('collateralDeltaAmount')) - 64, 32)) AS collateral_delta_amount, --  decimals collateralToken 
    varbinary_to_int256(varbinary_substring(data, varbinary_position(data, to_utf8('priceImpactUsd')) - 64, 32)) AS price_impact_usd,  -- 30 decimals (order average - order price) (volume)
    varbinary_to_int256(varbinary_substring(data, varbinary_position(data, to_utf8('priceImpactAmount')) - 64, 32)) AS price_impact_amount, -- decimals index_token 

    -- Extracting Booleans
    varbinary_to_uint256(varbinary_substring(data, varbinary_position(data, to_utf8('isLong')) - 64, 32)) AS is_long,

    -- Extracting Bytes32
    varbinary_substring(data, varbinary_position(data, to_utf8('orderKey')) - 64, 32) AS order_key,
    varbinary_substring(data, varbinary_position(data, to_utf8('positionKey')) - 64, 32) AS position_key
    
FROM
    {{source('arbitrum','logs')}}
WHERE
    contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
    AND topic1 = keccak(to_utf8('{{event_name}}'))
  
