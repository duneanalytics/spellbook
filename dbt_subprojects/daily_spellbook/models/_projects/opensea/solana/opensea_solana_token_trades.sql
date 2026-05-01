{{
    config(
        schema = 'opensea',
        alias = 'solana_token_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'project', 'trader_id', 'block_time', 'token_bought_symbol', 'token_bought_mint_address', 'token_sold_symbol', 'token_sold_mint_address', 'token_sold_amount', 'amount_usd', 'tx_id', 'inner_instruction_index', 'outer_instruction_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

WITH opensea_tx_ids AS (
    SELECT DISTINCT(tx_id) AS tx_id
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE CONTAINS(log_messages, 'Program log: Memo (len 8): "865d8597"')
        AND executing_account = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr'
        AND block_time > TRY_CAST('2025-05-26 00:00' AS TIMESTAMP)
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
)

SELECT 
    dst.blockchain
    , dst.project
    , dst.trader_id
    , dst.block_time
    , dst.token_bought_symbol
    , dst.token_bought_mint_address
    , dst.token_sold_symbol
    , dst.token_sold_mint_address
    , dst.token_sold_amount
    , dst.amount_usd
    , dst.tx_id 
    , dst.inner_instruction_index
    , dst.outer_instruction_index
FROM {{ source('dex_solana', 'trades') }} dst
INNER JOIN opensea_tx_ids oti 
    ON dst.tx_id = oti.tx_id
WHERE dst.block_time  > TRY_CAST('2025-05-26 00:00' AS TIMESTAMP)
{% if is_incremental() %}
    AND {{ incremental_predicate('dst.block_time') }}
{% endif %}
