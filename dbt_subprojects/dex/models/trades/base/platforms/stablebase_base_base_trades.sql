{{ config(
    schema = 'stablebase_base',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

-- Id mapping:
-- 0 USDbC https://basescan.org/token/0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca
-- 1 DAI https://basescan.org/token/0x50c5725949a6f0c72e6c4a641f24049a917db0cb
-- 2 axlUSDC https://basescan.org/token/0xeb466342c4d449bc9f53a865d5cb90586f405215

WITH token_swaps AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        tokensSold AS token_sold_amount_raw,
        tokensBought AS token_bought_amount_raw,
        CASE soldId
            WHEN 0 THEN 0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca
            WHEN 1 THEN 0x50c5725949a6f0c72e6c4a641f24049a917db0cb
            WHEN 2 THEN 0xeb466342c4d449bc9f53a865d5cb90586f405215
        END AS token_sold_address,
        CASE boughtId
            WHEN 0 THEN 0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca
            WHEN 1 THEN 0x50c5725949a6f0c72e6c4a641f24049a917db0cb
            WHEN 2 THEN 0xeb466342c4d449bc9f53a865d5cb90586f405215
        END AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('stablebase_base', 'SwapFlashLoan_evt_TokenSwap') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'base' AS blockchain,
    'stablebase' AS project,
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
