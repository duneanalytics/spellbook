{{
  config(
    schema = 'pumpswap_solana',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    incremental_predicates = [incremental_predicate('block_time')]
  )
}}

{% set project_start_date = '2025-03-15' %}

WITH pool_creation AS (
    -- Pool creation events using the provided decoding pattern
    SELECT
        block_time AS created_at,
        block_slot AS created_slot,
        tx_id AS creation_tx,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 17, 4))) AS timestamp,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 21, 4))) AS index,
        to_base58(bytearray_substring(data, 27, 32)) AS creator,
        to_base58(bytearray_substring(data, 59, 32)) AS baseMint,
        to_base58(bytearray_substring(data, 91, 32)) AS quoteMint,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 123, 1))) AS baseMintDecimals,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 124, 1))) AS quoteMintDecimals,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 125, 8))) AS baseAmountIn,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 133, 8))) AS quoteAmountIn,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 141, 8))) AS poolBaseAmount,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 149, 8))) AS poolQuoteAmount,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 157, 8))) AS minimumLiquidity,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 165, 8))) AS initialLiquidity,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 173, 8))) AS lpTokenAmountOut,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 181, 1))) AS poolBump,
        to_base58(bytearray_substring(data, 182, 32)) AS pool,
        to_base58(bytearray_substring(data, 214, 32)) AS lpMint,
        to_base58(bytearray_substring(data, 246, 32)) AS userBaseTokenAccount,
        to_base58(bytearray_substring(data, 278, 32)) AS userQuoteTokenAccount
    FROM {{ source('solana','instruction_calls') }}
    WHERE varbinary_starts_with(data, 0xe445a52e51cb9a1db1310cd2a076a774)
        AND executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
        AND tx_success = true
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% else %}
    AND block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
),

-- Join with token information to get symbols
token_info AS (
    SELECT
        token_mint_address,
        symbol,
        decimals
    FROM {{ ref('tokens_solana_fungible') }}
)

SELECT
    p.pool,
    p.created_at,
    p.created_slot,
    p.creation_tx,
    p.creator,
    p.baseMint,
    p.quoteMint,
    p.baseMintDecimals,
    p.quoteMintDecimals,
    p.baseAmountIn,
    p.quoteAmountIn,
    p.poolBaseAmount,
    p.poolQuoteAmount,
    p.minimumLiquidity,
    p.initialLiquidity,
    p.lpTokenAmountOut,
    p.poolBump,
    p.lpMint,
    tb.symbol AS base_symbol,
    tq.symbol AS quote_symbol,
    -- Convert to human-readable numbers
    p.baseAmountIn / POWER(10, p.baseMintDecimals) AS baseAmountIn_normalized,
    p.quoteAmountIn / POWER(10, p.quoteMintDecimals) AS quoteAmountIn_normalized,
    p.poolBaseAmount / POWER(10, p.baseMintDecimals) AS poolBaseAmount_normalized,
    p.poolQuoteAmount / POWER(10, p.quoteMintDecimals) AS poolQuoteAmount_normalized,
    p.lpTokenAmountOut / POWER(10, 6) AS lpTokenAmountOut_normalized -- Assuming LP tokens are 6 decimals
FROM pool_creation p
LEFT JOIN token_info tb ON tb.token_mint_address = p.baseMint
LEFT JOIN token_info tq ON tq.token_mint_address = p.quoteMint