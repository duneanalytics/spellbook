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
        p.coins[CAST(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) AS int) + 1] AS token_bought_address,
        p.coins[CAST(bytearray_to_uint256(bytearray_substring(l.data, 1,  32)) AS int) + 1] AS token_sold_address,
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
),
dexs_with_decimals AS (
    SELECT
        dexs.*,
        erc20_bought.decimals AS token_bought_decimals,
        erc20_sold.decimals   AS token_sold_decimals,
        COALESCE(erc20_bought.decimals, 18) AS curve_decimals_bought,
        COALESCE(erc20_sold.decimals,   18) AS curve_decimals_sold
    FROM dexs
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = dexs.token_bought_address
        AND erc20_bought.blockchain = 'monad'
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = dexs.token_sold_address
        AND erc20_sold.blockchain = 'monad'
)

SELECT
    'monad'                                                       AS blockchain,
    'curve'                                                       AS project,
    dexs_with_decimals.version                                    AS version,
    CAST(date_trunc('MONTH', dexs_with_decimals.block_time) AS date) AS block_month,
    CAST(date_trunc('DAY',   dexs_with_decimals.block_time) AS date) AS block_date,
    dexs_with_decimals.block_time,
    dexs_with_decimals.block_number,
    -- Plain V1 pools emit raw token amounts directly, so the decimal exponent is 0;
    -- keep the same shape as curve_ethereum_base_trades for future-proofing.
    CAST(
        dexs_with_decimals.token_bought_amount_raw *
        power(10, dexs_with_decimals.token_bought_decimals - dexs_with_decimals.curve_decimals_bought)
        AS UINT256
    ) AS token_bought_amount_raw,
    CAST(
        dexs_with_decimals.token_sold_amount_raw *
        power(10, dexs_with_decimals.token_sold_decimals - dexs_with_decimals.curve_decimals_sold)
        AS UINT256
    ) AS token_sold_amount_raw,
    dexs_with_decimals.token_bought_address,
    dexs_with_decimals.token_sold_address,
    dexs_with_decimals.taker,
    dexs_with_decimals.maker,
    dexs_with_decimals.project_contract_address,
    dexs_with_decimals.tx_hash,
    dexs_with_decimals.evt_index
FROM dexs_with_decimals
