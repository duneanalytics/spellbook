{{
    config(
        schema = 'curve_monad',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{# Curve on Monad: mirrors curve_ethereum_base_trades but restricted to the
   single pool family that exists here (StableSwapFactory plain pools emitting
   the V1 TokenExchange topic 0x8b3e96…). No metapools / TokenExchangeUnderlying
   and no V2 (twocrypto/tricrypto) pools have been deployed on Monad yet.

   Event data layout (32-byte words after the indexed taker in topic1):
     [1:32]   sold_id           (index into pool.coins, 0-based)
     [33:64]  sold_amount_raw
     [65:96]  bought_id
     [97:128] bought_amount_raw
#}

WITH dexs AS (
    SELECT
        l.block_number,
        l.block_time,
        p.version AS version,
        bytearray_substring(l.topic1, 13, 20)                                  AS taker,
        CAST(NULL AS VARBINARY)                                                AS maker,
        bytearray_to_uint256(bytearray_substring(l.data, 97, 32))              AS token_bought_amount_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 33, 32))              AS token_sold_amount_raw,
        CASE WHEN CAST(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) AS int) + 1 <= CARDINALITY(p.coins)
             THEN p.coins[CAST(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) AS int) + 1] END AS token_bought_address,
        CASE WHEN CAST(bytearray_to_uint256(bytearray_substring(l.data, 1,  32)) AS int) + 1 <= CARDINALITY(p.coins)
             THEN p.coins[CAST(bytearray_to_uint256(bytearray_substring(l.data, 1,  32)) AS int) + 1] END AS token_sold_address,
        l.contract_address                                                     AS project_contract_address,
        l.tx_hash,
        l.index                                                                AS evt_index
    FROM {{ source('monad', 'logs') }} l
    JOIN {{ ref('curve_monad_view_pools') }} p
        ON l.contract_address = p.pool_address
    WHERE l.topic0 = 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140 -- TokenExchange
        {% if is_incremental() %}
        AND {{ incremental_predicate('l.block_time') }}
        {% endif %}
)

SELECT
    'monad'                                            AS blockchain,
    'curve'                                            AS project,
    dexs.version                                       AS version,
    CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('DAY',   dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    -- base_trades emits raw on-chain amounts; decimal normalization happens
    -- downstream in dex.trades, same as every other curve base_trades model.
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
