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
)

SELECT * FROM evm_trades
UNION ALL
SELECT * FROM solana_trades;
