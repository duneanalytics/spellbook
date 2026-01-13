{{ config(
        schema = 'tokens_multichain'
        , alias = 'transfers'
        , materialized = 'view'
        )
}}

WITH

evm_transfers AS (
        SELECT 
                blockchain,
                block_time,
                block_date,
                block_month,
                block_number,
                CAST(tx_hash AS VARCHAR) AS tx_id,
                CAST("from" AS VARCHAR) AS from_address,
                CAST("to" AS VARCHAR) AS to_address,
                CAST(tx_from AS VARCHAR) AS tx_signer,
                CAST(contract_address AS VARCHAR) AS token_address,
                symbol AS token_symbol,
                token_standard,
                amount_raw,
                amount,
                amount_usd
        FROM {{ ref('tokens_evm_transfers') }}
),

solana_transfers AS (
        SELECT
                'solana' AS blockchain,
                block_time,
                block_date,
                DATE_TRUNC('month', block_date) AS block_month,
                block_slot AS block_number,
                tx_id,
                from_owner AS from_address,
                to_owner AS to_address,
                tx_signer,
                token_mint_address AS token_address,
                symbol AS token_symbol,
                token_version AS token_standard,
                amount AS amount_raw,
                amount_display AS amount,
                amount_usd
        FROM {{ source('tokens_solana', 'transfers') }}
)

SELECT * FROM evm_transfers
UNION ALL
SELECT * FROM solana_transfers;
