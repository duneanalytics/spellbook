{{
  config(
    schema = 'pumpswap_solana',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
    pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-03-14' %}

WITH pools AS (
    SELECT
        pool,
        baseMint,
        quoteMint,
        baseMintDecimals,
        quoteMintDecimals
    FROM {{ ref('pumpswap_solana_pools') }}
),

swaps AS (
    SELECT
        block_time,
        block_slot,
        tx_id,
        outer_instruction_index,
        inner_instruction_index,
        tx_index,
        CASE
            WHEN varbinary_starts_with(data, 0xe445a52e51cb9a1d3e2f370aa503dc2a) THEN 'sell'
            WHEN varbinary_starts_with(data, 0xe445a52e51cb9a1d67f4521f2cf57777) THEN 'buy'
            ELSE 'unknown'
        END AS trade_type,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 17, 4))) AS timestamp,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 25, 8))) AS base_amount_out, -- From Anchor log: baseAmountOut
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 33, 8))) AS max_quote_amount_in, -- From Anchor log: maxQuoteAmountIn
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 65, 8))) AS pool_quote_token_reserves,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 73, 8))) AS quote_amount_in, -- From Anchor log: quoteAmountIn
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 89, 8))) AS lp_fee, 
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 97, 8))) AS protocol_fee_basis_points,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 105, 8))) AS protocol_fee,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 113, 8))) AS quote_amount_with_lp_fee,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 121, 8))) AS user_quote_amount_in, -- From Anchor log: userQuoteAmountIn
        to_base58(bytearray_substring(data, 129, 32)) AS pool,
        to_base58(bytearray_substring(data, 161, 32)) AS user,
        to_base58(bytearray_substring(data, 193, 32)) AS user_base_token_account,
        to_base58(bytearray_substring(data, 225, 32)) AS user_quote_token_account,
        to_base58(bytearray_substring(data, 257, 32)) AS protocol_fee_recipient,
        to_base58(bytearray_substring(data, 289, 32)) AS protocol_fee_recipient_token_account,
        outer_executing_account
    FROM {{ source('solana','instruction_calls') }}
    WHERE inner_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
        AND tx_success = true
        AND (
            varbinary_starts_with(data, 0xe445a52e51cb9a1d3e2f370aa503dc2a) OR 
            varbinary_starts_with(data, 0xe445a52e51cb9a1d67f4521f2cf57777)
        )
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
)

SELECT
    'solana' as blockchain,
    'pumpswap' as project,
    1 as version,
    CAST(date_trunc('month', sp.block_time) AS DATE) as block_month,
    sp.block_time,
    sp.block_slot,
    CASE 
        WHEN sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN 'direct'
        ELSE sp.outer_executing_account
    END as trade_source,
    CASE 
        WHEN sp.trade_type = 'buy' THEN sp.base_amount_out
        WHEN sp.trade_type = 'sell' THEN sp.quote_amount_in
        ELSE NULL
    END as token_bought_amount_raw,
    CASE 
        WHEN sp.trade_type = 'buy' THEN sp.quote_amount_in
        WHEN sp.trade_type = 'sell' THEN sp.base_amount_out
        ELSE NULL
    END as token_sold_amount_raw,
    -- Fee tier as a decimal (e.g., 0.003 for 0.3%)
    sp.protocol_fee_basis_points / 10000.0 as fee_tier,
    CASE 
        WHEN sp.trade_type = 'buy' THEN p.baseMint
        WHEN sp.trade_type = 'sell' THEN p.quoteMint
        ELSE NULL
    END as token_bought_mint_address,
    CASE 
        WHEN sp.trade_type = 'buy' THEN p.quoteMint
        WHEN sp.trade_type = 'sell' THEN p.baseMint
        ELSE NULL
    END as token_sold_mint_address,
    -- Token vaults
    CASE 
        WHEN sp.trade_type = 'buy' THEN sp.user_base_token_account
        WHEN sp.trade_type = 'sell' THEN sp.user_quote_token_account
        ELSE NULL
    END as token_bought_vault,
    CASE 
        WHEN sp.trade_type = 'buy' THEN sp.user_quote_token_account
        WHEN sp.trade_type = 'sell' THEN sp.user_base_token_account
        ELSE NULL
    END as token_sold_vault,
    sp.pool as project_program_id,
    'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' as project_main_id,
    sp.user as trader_id,
    sp.tx_id,
    sp.outer_instruction_index,
    sp.inner_instruction_index,
    sp.tx_index
FROM swaps sp
LEFT JOIN pools p ON p.pool = sp.pool
where sp.block_time < CAST('2025-04-24 09:00:00' AS TIMESTAMP)