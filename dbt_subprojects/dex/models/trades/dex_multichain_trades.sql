{{ config(
        schema = 'dex_multichain'
        , alias = 'trades'
        , materialized = 'view'
        )
}}

WITH

evm_trades AS (
        SELECT 
                blockchain,
                block_time AS timestamp,
                block_date AS date,
                block_number, --not very chain agnostic
                CONCAT('0x', LOWER(TO_HEX(tx_hash))) AS tx_id,
                CONCAT('0x', LOWER(TO_HEX(taker))) AS trader_id,
                CONCAT('0x', LOWER(TO_HEX(tx_from))) AS tx_signer,
                project,
                CONCAT('0x', LOWER(TO_HEX(project_contract_address))) AS pool_id,
                token_pair,
                token_bought_symbol,
                token_sold_symbol,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                CONCAT('0x', LOWER(TO_HEX(token_bought_address))) AS token_bought_id,
                CONCAT('0x', LOWER(TO_HEX(token_sold_address))) AS token_sold_id
        FROM {{ ref('dex_evm_trades') }}
),

solana_trades AS (
        SELECT
                'solana' AS blockchain,
                block_time AS timestamp,
                block_date AS date,
                block_slot AS block_number,
                tx_id,
                trader_id,
                trader_id AS tx_signer,
                project,
                project_program_id AS pool_id,
                token_pair,
                token_bought_symbol,
                token_sold_symbol,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_mint_address AS token_bought_id,
                token_sold_mint_address AS token_sold_id
        FROM {{ source('dex_solana', 'trades') }}
),

sui_trades AS (
        SELECT
                'sui' AS blockchain,
                block_time AS timestamp,
                block_date AS date,
                CAST(checkpoint AS BIGINT) AS block_number,
                transaction_digest AS tx_id,
                CONCAT('0x', LOWER(TO_HEX(sender))) AS trader_id,
                CONCAT('0x', LOWER(TO_HEX(sender))) AS tx_signer,
                project,
                pool_id,
                token_pair,
                token_bought_symbol,
                token_sold_symbol,
                CAST(token_bought_amount AS DOUBLE) AS token_bought_amount,
                CAST(token_sold_amount AS DOUBLE) AS token_sold_amount,
                CAST(token_bought_amount_raw AS DOUBLE) AS token_bought_amount_raw,
                CAST(token_sold_amount_raw AS DOUBLE) AS token_sold_amount_raw,
                CAST(amount_usd AS DOUBLE) AS amount_usd,
                token_bought_address AS token_bought_id,
                token_sold_address AS token_sold_id
        FROM {{ source('dex_sui', 'trades') }}
)

SELECT * FROM evm_trades
UNION ALL
SELECT * FROM solana_trades
UNION ALL
SELECT * FROM sui_trades;
