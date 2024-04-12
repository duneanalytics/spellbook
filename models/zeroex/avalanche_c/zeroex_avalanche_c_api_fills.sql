{{  config(
        
        alias = 'api_fills',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: https://dune.com/queries/1855493

WITH zeroex_tx AS (
        SELECT 
            tr.tx_hash,
            tr.block_number,
            MAX(CASE
                WHEN bytearray_position(INPUT, 0x869584cd ) <> 0
                    THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0x869584cd) + 16) FOR 20)
                WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0
                    THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0xfbc019a7 ) + 16) FOR 20)
            END) AS affiliate_address
        FROM {{ source('avalanche_c', 'traces') }} tr
        WHERE tr.to IN (
                -- exchange contract
                0x61935cbdd02287b511119ddb11aeb42f1593b7ef, 
                -- forwarder addresses
                0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5,
                0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be,
                0x4ef40d1bf0983899892946830abf99eca2dbc5ce, 
                -- exchange proxy
                0xdef1c0ded9bec7f1a1670819833240f027b25eff
                )
                AND (
                    bytearray_position(INPUT, 0x869584cd ) <> 0
                    OR bytearray_position(INPUT, 0xfbc019a7 ) <> 0
                )
                
                {% if is_incremental() %}
                AND tr.block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
                {% if not is_incremental() %}
                AND tr.block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
                {% endif %}
        GROUP BY tr.tx_hash, tr.block_number
),

v4_rfq_fills_no_bridge AS (
    SELECT 
            fills.evt_tx_hash               AS tx_hash,
            fills.evt_block_number         AS block_number,
            fills.evt_index,
            fills.contract_address,
            fills.evt_block_time            AS block_time,
            fills.maker                     AS maker,
            fills.taker                     AS taker,
            fills.takerToken                AS taker_token,
            fills.makerToken                AS maker_token,
            fills.takerTokenFilledAmount    AS taker_token_amount_raw,
            fills.makerTokenFilledAmount    AS maker_token_amount_raw,
            'RfqOrderFilled'                AS type,
            zeroex_tx.affiliate_address     AS affiliate_address,
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            FALSE                           AS matcha_limit_order_flag
    FROM {{ source('zeroex_avalanche_c', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
    INNER JOIN zeroex_tx 
        ON zeroex_tx.tx_hash = fills.evt_tx_hash
        AND zeroex_tx.block_number = fills.evt_block_number
    {% if is_incremental() %}
    WHERE fills.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE fills.evt_block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
    {% endif %}
),
v4_limit_fills_no_bridge AS (
    SELECT 
            fills.evt_tx_hash AS tx_hash,
            fills.evt_block_number as block_number,
            fills.evt_index,
            fills.contract_address,
            fills.evt_block_time AS block_time,
            fills.maker AS maker,
            fills.taker AS taker,
            fills.takerToken AS taker_token,
            fills.makerToken AS maker_token,
            fills.takerTokenFilledAmount AS taker_token_amount_raw,
            fills.makerTokenFilledAmount AS maker_token_amount_raw,
            'LimitOrderFilled' AS type,
            COALESCE(zeroex_tx.affiliate_address, fills.feeRecipient) AS affiliate_address,
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            (fills.feeRecipient in 
                (0x9b858be6e3047d88820f439b240deac2418a2551,0x86003b044f70dac0abc80ac8957305b6370893ed,0x5bc2419a087666148bfbe1361ae6c06d240c6131)) 
                AS matcha_limit_order_flag 
    FROM {{ source('zeroex_avalanche_c', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
    INNER JOIN zeroex_tx 
        ON zeroex_tx.tx_hash = fills.evt_tx_hash
        AND zeroex_tx.block_number = fills.evt_block_number
    {% if is_incremental() %}
    WHERE fills.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE fills.evt_block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
    {% endif %}
),
otc_fills AS (
    SELECT 
            fills.evt_tx_hash               AS tx_hash,
            fills.evt_block_number          AS block_number,
            fills.evt_index,
            fills.contract_address,
            fills.evt_block_time            AS block_time,
            fills.maker                     AS maker,
            fills.taker                     AS taker,
            fills.takerToken                AS taker_token,
            fills.makerToken                AS maker_token,
            fills.takerTokenFilledAmount    AS taker_token_amount_raw,
            fills.makerTokenFilledAmount    AS maker_token_amount_raw,
            'OtcOrderFilled'                AS type,
            zeroex_tx.affiliate_address     AS affiliate_address,
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            FALSE                           AS matcha_limit_order_flag
    FROM {{ source('zeroex_avalanche_c', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = fills.evt_tx_hash
        AND zeroex_tx.block_number = fills.evt_block_number
    {% if is_incremental() %}
    WHERE fills.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE fills.evt_block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
    {% endif %}

),
/*
ERC20BridgeTransfer AS (
    SELECT 
            logs.tx_hash,
            INDEX                                   AS evt_index,
            logs.contract_address,
            block_time                              AS block_time,
            '0x' || substring(DATA, 283, 40)        AS maker,
            '0x' || substring(DATA, 347, 40)        AS taker,
            '0x' || substring(DATA, 27, 40)         AS taker_token,
            '0x' || substring(DATA, 91, 40)         AS maker_token,
            bytea2numeric(substring(DATA, 155, 40)) AS taker_token_amount_raw,
            bytea2numeric(substring(DATA, 219, 40)) AS maker_token_amount_raw,
            'ERC20BridgeTransfer'                   AS type,
            zeroex_tx.affiliate_address             AS affiliate_address,
            TRUE                                    AS swap_flag,
            FALSE                                   AS matcha_limit_order_flag
    FROM {{ source('avalanche_c', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = '0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9'
    
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
  --  AND block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}

), 
BridgeFill AS (
    SELECT 
            logs.tx_hash,
            INDEX                                           AS evt_index,
            logs.contract_address,
            block_time                                      AS block_time,
            '0x' || substring(DATA, 27, 40)                 AS maker,
            0xdef1c0ded9bec7f1a1670819833240f027b25eff    AS taker,
            '0x' || substring(DATA, 91, 40)                 AS taker_token,
            '0x' || substring(DATA, 155, 40)                AS maker_token,
            bytea2numeric('0x' || substring(DATA, 219, 40)) AS taker_token_amount_raw,
            bytea2numeric('0x' || substring(DATA, 283, 40)) AS maker_token_amount_raw,
            'BridgeFill'                                    AS type,
            zeroex_tx.affiliate_address                     AS affiliate_address,
            TRUE                                            AS swap_flag,
            FALSE                                           AS matcha_limit_order_flag
    FROM {{ source('avalanche_c', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = '0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9'
        AND contract_address = 0xdb6f1920a889355780af7570773609bd8cb1f498

        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not is_incremental() %}
  --      AND block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
        {% endif %}
), */
NewBridgeFill AS (
    SELECT 
            logs.tx_hash as tx_hash,
            logs.block_number as    block_number,
            INDEX                                           AS evt_index,
            logs.contract_address,
            block_time                                      AS block_time,
            bytearray_substring(DATA, 13, 20)                 AS maker,
            0xdef1c0ded9bec7f1a1670819833240f027b25eff    AS taker,
            bytearray_substring(DATA, 45, 20)                 AS taker_token,
            bytearray_substring(DATA, 77, 20)                AS maker_token,
            bytearray_to_uint256(bytearray_substring(DATA, 109, 20)) AS taker_token_amount_raw,
            bytearray_to_uint256(bytearray_substring(DATA, 141, 20)) AS maker_token_amount_raw,
            'BridgeFill'                                 AS type,
            zeroex_tx.affiliate_address                     AS affiliate_address,
            TRUE                                            AS swap_flag,
            FALSE                                           AS matcha_limit_order_flag
    FROM {{ source('avalanche_c' ,'logs') }} logs
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = logs.tx_hash
        AND zeroex_tx.block_number = logs.block_number
    WHERE topic0 = 0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8
        AND contract_address = 0xdb6f1920a889355780af7570773609bd8cb1f498

        {% if is_incremental() %}
        AND logs.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not is_incremental() %}
        AND logs.block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
        {% endif %}
),

direct_PLP AS (
    SELECT 
            plp.evt_tx_hash as tx_hash,
            plp.evt_block_number as block_number,
            plp.evt_index               AS evt_index,
            plp.contract_address,
            plp.evt_block_time          AS block_time,
            provider                    AS maker,
            recipient                   AS taker,
            inputToken                  AS taker_token,
            outputToken                 AS maker_token,
            inputTokenAmount            AS taker_token_amount_raw,
            outputTokenAmount           AS maker_token_amount_raw,
            'LiquidityProviderSwap'     AS type,
            zeroex_tx.affiliate_address AS affiliate_address,
            TRUE                        AS swap_flag,
            FALSE                       AS matcha_limit_order_flag
    FROM {{ source('zeroex_avalanche_c', 'ExchangeProxy_evt_LiquidityProviderSwap') }} plp
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = plp.evt_tx_hash
        AND zeroex_tx.block_number = plp.evt_block_number
    {% if is_incremental() %}
    WHERE plp.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE plp.evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}
),

all_tx AS (
    SELECT *
    FROM direct_PLP 
    UNION ALL 
    SELECT *
    FROM NewBridgeFill 
    UNION ALL
    SELECT *
    FROM v4_rfq_fills_no_bridge
    UNION ALL
    SELECT *
    FROM v4_limit_fills_no_bridge
    UNION ALL
    SELECT *
    FROM otc_fills 
)

SELECT 
        all_tx.tx_hash,
        all_tx.block_number,
        all_tx.evt_index,
        all_tx.contract_address,
        all_tx.block_time,
        cast(date_trunc('day', all_tx.block_time) AS date) AS block_date,
        cast(date_trunc('month', all_tx.block_time) AS date) AS block_month,
        maker,
        CASE
            WHEN taker = 0xdef1c0ded9bec7f1a1670819833240f027b25eff THEN tx."from"
            ELSE taker
        END AS taker, -- fix the user masked by ProxyContract issue
        taker_token,
        ts.symbol AS taker_symbol,
        maker_token,
        ms.symbol AS maker_symbol,
        CASE WHEN lower(ts.symbol) > lower(ms.symbol) THEN concat(ms.symbol, '-', ts.symbol) ELSE concat(ts.symbol, '-', ms.symbol) END AS token_pair,
        taker_token_amount_raw / pow(10, tp.decimals) AS taker_token_amount,
        taker_token_amount_raw,
        maker_token_amount_raw / pow(10, mp.decimals) AS maker_token_amount,
        maker_token_amount_raw,
        all_tx.type,
        max(affiliate_address) over (partition by all_tx.tx_hash) as affiliate_address,
        swap_flag,
        matcha_limit_order_flag,
        CASE WHEN maker_token IN (0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7,0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664,0xc7198437980c041c805a1edcba50c1ce5db95118,
            0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7,0xd586e7f844cea2f87f50152665bcbc2c279d8d70, 0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab,0x1c20e891bab6b1727d14da358fae2984ed9b59eb) AND  mp.price IS NOT NULL
             THEN (all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price
             WHEN taker_token IN (0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7,0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664,0xc7198437980c041c805a1edcba50c1ce5db95118,
                0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7,0xd586e7f844cea2f87f50152665bcbc2c279d8d70, 0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab,0x1c20e891bab6b1727d14da358fae2984ed9b59eb)    AND  tp.price IS NOT NULL
             THEN (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price
             ELSE COALESCE((all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price)
             END AS volume_usd, 
        tx."from" AS tx_from,
        tx.to AS tx_to,
        'avalanche_c' AS blockchain
FROM all_tx
INNER JOIN {{ source('avalanche_c', 'transactions')}} tx
    ON all_tx.tx_hash = tx.hash
    AND all_tx.block_number = tx.block_number
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}

LEFT JOIN {{ source('prices', 'usd') }} tp 
    ON date_trunc('minute', all_tx.block_time) = tp.minute
    AND CASE
            WHEN all_tx.taker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
            ELSE all_tx.taker_token
        END = tp.contract_address
    AND tp.blockchain = 'avalanche_c'
    {% if is_incremental() %}
    AND tp.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND tp.minute >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}

LEFT JOIN {{ source('prices', 'usd') }} mp 
    ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
    AND CASE
            WHEN all_tx.maker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
            ELSE all_tx.maker_token
        END = mp.contract_address
    AND mp.blockchain = 'avalanche_c'
    {% if is_incremental() %}
    AND mp.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND mp.minute >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}

LEFT OUTER JOIN {{ source('tokens', 'erc20') }} ts ON ts.contract_address = taker_token and ts.blockchain = 'avalanche_c'
LEFT OUTER JOIN {{ source('tokens', 'erc20') }} ms ON ms.contract_address = maker_token and ms.blockchain = 'avalanche_c'
