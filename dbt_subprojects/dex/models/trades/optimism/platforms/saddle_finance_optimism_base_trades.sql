{{ config(
    schema = 'saddle_finance_optimism',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

-- contract 0x5847f8177221268d279cf377d0e01ab3fd993628 Id mapping:
-- 0 DAI https://optimistic.etherscan.io/token/0xda10009cbd5d07dd0cecc66161fc93d7c9000da1
-- 1 USDC.e https://optimistic.etherscan.io/token/0x7f5c764cbc14f9669b88837ca1490cca17c31607
-- 2 USDT https://optimistic.etherscan.io/token/0x94b008aa00579c1307b0ef2c499ad98a8ce58e58

-- contract 0xf6c2e0adc659007ba7c48446f5a4e4e94dfe08b5 Id mapping:
-- 0 USDC.e https://optimistic.etherscan.io/token/0x7f5c764cbc14f9669b88837ca1490cca17c31607
-- 1 FRAX https://optimistic.etherscan.io/token/0x2e3d870790dc77a83dd1d18184acc7439a53f475

WITH token_swaps AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        tokensSold AS token_sold_amount_raw,
        tokensBought AS token_bought_amount_raw,
        CASE 
            WHEN contract_address = 0x5847f8177221268d279cf377d0e01ab3fd993628 THEN
                CASE soldId
                    WHEN 0 THEN 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1
                    WHEN 1 THEN 0x7f5c764cbc14f9669b88837ca1490cca17c31607
                    WHEN 2 THEN 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58
                END
            WHEN contract_address = 0xf6c2e0adc659007ba7c48446f5a4e4e94dfe08b5 THEN
                CASE soldId
                    WHEN 0 THEN 0x7f5c764cbc14f9669b88837ca1490cca17c31607
                    WHEN 1 THEN 0x2e3d870790dc77a83dd1d18184acc7439a53f475
                END
        END AS token_sold_address,
        CASE 
            WHEN contract_address = 0x5847f8177221268d279cf377d0e01ab3fd993628 THEN
                CASE boughtId
                    WHEN 0 THEN 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1
                    WHEN 1 THEN 0x7f5c764cbc14f9669b88837ca1490cca17c31607
                    WHEN 2 THEN 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58
                END
            WHEN contract_address = 0xf6c2e0adc659007ba7c48446f5a4e4e94dfe08b5 THEN
                CASE boughtId
                    WHEN 0 THEN 0x7f5c764cbc14f9669b88837ca1490cca17c31607
                    WHEN 1 THEN 0x2e3d870790dc77a83dd1d18184acc7439a53f475
                END
        END AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('saddle_finance_optimism', 'SwapFlashLoan_evt_TokenSwap') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'optimism' AS blockchain,
    'saddle_finance' AS project,
    '1' AS version,
    CAST(date_trunc('month', token_swaps.block_time) AS date) AS block_month,
    CAST(date_trunc('day', token_swaps.block_time) AS date) AS block_date,
    token_swaps.block_time,
    token_swaps.block_number,
    token_swaps.token_sold_amount_raw,
    token_swaps.token_bought_amount_raw,
    token_swaps.token_sold_address,
    token_swaps.token_bought_address,
    token_swaps.maker,
    token_swaps.taker,
    token_swaps.project_contract_address,
    token_swaps.tx_hash,
    token_swaps.evt_index
FROM
    token_swaps
