{{
  config(
    schema = 'pumpswap_solana',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_at')],
    unique_key = ['pool']
  )
}}

{% set project_start_date = '2025-03-14' %}

WITH pool_creation AS (
    -- Pool creation events using the provided decoding pattern
    SELECT
        block_time AS created_at,
        block_slot AS created_slot,
        tx_id AS creation_tx,
        to_base58(bytearray_substring(data, 59, 32)) AS baseMint,
        to_base58(bytearray_substring(data, 91, 32)) AS quoteMint,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 123, 1))) AS baseMintDecimals,
        bytearray_to_uint256(bytearray_reverse(bytearray_substring(data, 124, 1))) AS quoteMintDecimals,
        to_base58(bytearray_substring(data, 182, 32)) AS pool
    FROM {{ source('solana','instruction_calls') }}
    WHERE varbinary_starts_with(data, 0xe445a52e51cb9a1db1310cd2a076a774)
        AND executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
        AND tx_success = true
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% else %}
    AND block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)
SELECT
    p.pool,
    p.created_at,
    p.created_slot,
    p.creation_tx,
    p.baseMint,
    p.quoteMint,
    p.baseMintDecimals,
    p.quoteMintDecimals
FROM pool_creation p