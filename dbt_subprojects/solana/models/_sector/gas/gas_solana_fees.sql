{{ config(
    schema = 'gas_solana',
    alias = 'fees',
    materialized = 'view'
) }}

WITH all_fees AS (
    SELECT
        blockchain,
        block_month,
        block_date,
        block_time,
        block_slot,
        tx_index,
        tx_hash,
        signer,
        compute_unit_price,
        compute_limit,
        currency_symbol,
        tx_fee_raw,
        tx_fee,
        tx_fee_usd,
        tx_fee_breakdown_raw,
        tx_fee_breakdown,
        tx_fee_breakdown_usd,
        tx_fee_currency,
        leader,
        'regular' AS tx_type
    FROM {{ ref('gas_solana_tx_fees') }}

    UNION ALL

    SELECT
        blockchain,
        block_month,
        block_date,
        block_time,
        block_slot,
        tx_index,
        tx_hash,
        signer,
        compute_unit_price,
        compute_limit,
        currency_symbol,
        tx_fee_raw,
        tx_fee,
        tx_fee_usd,
        tx_fee_breakdown_raw,
        tx_fee_breakdown,
        tx_fee_breakdown_usd,
        tx_fee_currency,
        leader,
        tx_type
    FROM {{ ref('gas_solana_vote_fees') }}
)

SELECT *
FROM all_fees