{{
    config(
        schema = 'tapio_base',
        alias = 'trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "tapio",
                                \'["brunota20"]\') }}'
    )
}}

WITH raw_trades AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        evt_index,
        buyer AS taker,
        amounts[0] AS token_sold_amount_raw,  -- amounts[_i] is the input token amount
        amounts[1] AS token_bought_amount_raw,  -- amounts[_j] is the output token amount
        contract_address AS maker,  -- The contract is the maker
        tokens[_i] AS token_sold_address,  -- Replace _i with the correct index
        tokens[_j] AS token_bought_address,  -- Replace _j with the correct index
        feeAmount AS fee_amount,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        'tapio' AS project,
        '1' AS version,  -- Adjust version if needed
        'base' AS blockchain  -- Adjust blockchain if needed
    FROM {{ source('tapio_blockchain', 'SelfPeggingAsset_evt_TokenSwapped') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT
    blockchain,
    project,
    version,
    date_trunc('month', block_time) AS block_month,
    date_trunc('day', block_time) AS block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    contract_address AS project_contract_address,
    tx_hash,
    evt_index,
    tx_from,
    tx_to
FROM raw_trades;