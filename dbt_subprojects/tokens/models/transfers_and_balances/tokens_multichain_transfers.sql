{{ config(
        schema = 'tokens_multichain'
        , alias = 'transfers'
        , materialized = 'view'
        )
}}

WITH

evm_transfers AS (
        SELECT 
                unique_key, --pending confirmation
                blockchain,
                block_time AS timestamp,
                block_date AS date,
                block_number, --not very chain agnostic
                CONCAT('0x', LOWER(TO_HEX(tx_hash))) AS tx_id,
                CONCAT('0x', LOWER(TO_HEX("from"))) AS from_address,
                CONCAT('0x', LOWER(TO_HEX(to))) AS to_address,
                CONCAT('0x', LOWER(TO_HEX(tx_from))) AS tx_signer,
                CONCAT('0x', LOWER(TO_HEX(contract_address))) AS token_address,
                symbol AS token_symbol,
                token_standard,
                amount_raw,
                amount,
                amount_usd
        FROM {{ ref('tokens_transfers') }}
        WHERE block_date = CURRENT_DATE - INTERVAL '1' DAY
        AND blockchain = 'ethereum'
),

solana_transfers AS (
        SELECT
                unique_instruction_key AS unique_key, --pending confirmation
                'solana' AS blockchain,
                block_time AS timestamp,
                block_date AS date,
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
        WHERE block_date = CURRENT_DATE - INTERVAL '1' DAY
)

SELECT * FROM evm_transfers
UNION ALL
SELECT * FROM solana_transfers;
