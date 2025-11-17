{{
    config(
        schema = 'opensea',
        alias = 'solana_crosschain_token_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['sourceChain', 'block_time', 'tokenSymbolOnSource', 'amountReal', 'amountUsd', 'bridgor', 'tokenAddressOnSource', 'hash'],
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
    'solana' AS sourceChain
    , block_time 
    , symbol AS tokenSymbolOnSource
    , amount_display AS amountReal
    , amount_usd AS amountUsd
    , from_owner AS bridgor
    , token_mint_address AS tokenAddressOnSource
    , tst.tx_id AS hash
FROM {{ source('tokens_solana', 'transfers') }} tst
INNER JOIN opensea_tx_ids oti 
    ON tst.tx_id = oti.tx_id
WHERE to_owner = 'F7p3dFrjRTbtRp8FRF6qHLomXbKRBzpvBLjtQcfcgmNe' --relaySolver
AND block_time > TRY_CAST('2025-05-26 00:00' AS TIMESTAMP)
AND amount > 0 
{% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
{% endif %}
