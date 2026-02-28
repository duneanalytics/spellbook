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
                block_time,
                block_date,
                block_month,
                block_number,
                CAST(tx_hash AS VARCHAR) AS tx_id,
                CAST(taker AS VARCHAR) AS trader_id,
                project,
                CAST(project_contract_address AS VARCHAR) AS executing_contract_address,
                token_pair,
                token_bought_symbol,
                token_sold_symbol,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                CAST(token_bought_address AS VARCHAR) AS token_bought_id,
                CAST(token_sold_address AS VARCHAR) AS token_sold_id
        FROM {{ ref('dex_evm_trades') }}
),

solana_trades AS (
        SELECT
                'solana' AS blockchain,
                block_time,
                block_date,
                block_month,
                block_slot AS block_number,
                tx_id,
                trader_id,
                project,
                project_main_id AS executing_contract_address,
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
