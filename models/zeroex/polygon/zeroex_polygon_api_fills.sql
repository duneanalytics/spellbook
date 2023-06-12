{{  config(

        alias='api_fills',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: https://dune.com/queries/1684477

WITH zeroex_tx AS (
    SELECT distinct 
            tr.tx_hash,
            max('0x' || CASE
                        WHEN POSITION('869584cd' IN INPUT) <> 0
                        THEN SUBSTRING(INPUT
                                FROM (position('869584cd' IN INPUT) + 32)
                                FOR 40)
                        WHEN POSITION('fbc019a7' IN INPUT) <> 0
                        THEN SUBSTRING(INPUT
                                FROM (position('fbc019a7' IN INPUT) + 32)
                                FOR 40)
            END) AS affiliate_address,
            tr.block_number as block_number,
            tr.block_time as block_time
        FROM {{ source('polygon', 'traces') }} tr
        WHERE tr.to IN (
                -- exchange contract
                '0x61935cbdd02287b511119ddb11aeb42f1593b7ef', 
                -- forwarder addresses
                '0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5',
                '0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be',
                '0x4ef40d1bf0983899892946830abf99eca2dbc5ce', 
                -- exchange proxy
                '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
                )
                AND (
                        POSITION('869584cd' IN INPUT) <> 0
                        OR POSITION('fbc019a7' IN INPUT) <> 0
                    )
                
                {% if is_incremental() %}
                AND block_time >= date_trunc('day', now() - interval '1 week') 
                {% endif %}
                {% if not is_incremental() %}
                AND block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
            group by tr.tx_hash, tr.to, tr.from, tr.block_number, tr.block_time
),

v4_rfq_fills_no_bridge AS (
    SELECT 
            fills.evt_tx_hash               AS tx_hash,
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
            zeroex_tx.block_number,
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            FALSE                           AS matcha_limit_order_flag
    FROM {{ source('zeroex_polygon', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = fills.evt_tx_hash
        AND zeroex_tx.block_number = fills.evt_block_number
        AND zeroex_tx.block_time = fills.evt_block_time

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{zeroex_v4_start_date}}'
    {% endif %}
),

v4_limit_fills_no_bridge AS (
    SELECT 
            fills.evt_tx_hash AS tx_hash,
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
            zeroex_tx.block_number,
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            (fills.feeRecipient in 
                ('0x9b858be6e3047d88820f439b240deac2418a2551','0x86003b044f70dac0abc80ac8957305b6370893ed','0x5bc2419a087666148bfbe1361ae6c06d240c6131')) 
                AS matcha_limit_order_flag 
    FROM {{ source('zeroex_polygon', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = fills.evt_tx_hash
        AND zeroex_tx.block_number = fills.evt_block_number
        AND zeroex_tx.block_time = fills.evt_block_time

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{zeroex_v4_start_date}}'
    {% endif %}
),

otc_fills AS (
    SELECT 
            fills.evt_tx_hash               AS tx_hash,
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
            zeroex_tx.block_number,
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            FALSE                           AS matcha_limit_order_flag
    FROM {{ source('zeroex_polygon', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = fills.evt_tx_hash
        AND zeroex_tx.block_number = fills.evt_block_number
        AND zeroex_tx.block_time = fills.evt_block_time

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{zeroex_v4_start_date}}'
    {% endif %}

),

ERC20BridgeTransfer AS (
    SELECT 
            logs.tx_hash,
            INDEX                                   AS evt_index,
            logs.contract_address,
            zeroex_tx.block_time                    AS block_time,
            '0x' || substring(DATA, 283, 40)        AS maker,
            '0x' || substring(DATA, 347, 40)        AS taker,
            '0x' || substring(DATA, 27, 40)         AS taker_token,
            '0x' || substring(DATA, 91, 40)         AS maker_token,
            bytea2numeric(substring(DATA, 155, 40)) AS taker_token_amount_raw,
            bytea2numeric(substring(DATA, 219, 40)) AS maker_token_amount_raw,
            'ERC20BridgeTransfer'                   AS type,
            zeroex_tx.affiliate_address             AS affiliate_address,
            zeroex_tx.block_number,
            TRUE                                    AS swap_flag,
            FALSE                                   AS matcha_limit_order_flag
    FROM {{ source('polygon', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic1 = '0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9'
    
    {% if is_incremental() %}
    AND zeroex_tx.block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND zeroex_tx.block_time >= '{{zeroex_v3_start_date}}'
    {% endif %}

), 
/*
BridgeFill AS (
    SELECT 
            logs.tx_hash,
            INDEX                                           AS evt_index,
            logs.contract_address,
            block_time                                      AS block_time,
            '0x' || substring(DATA, 27, 40)                 AS maker,
            '0xdef1c0ded9bec7f1a1670819833240f027b25eff'    AS taker,
            '0x' || substring(DATA, 91, 40)                 AS taker_token,
            '0x' || substring(DATA, 155, 40)                AS maker_token,
            bytea2numeric('0x' || substring(DATA, 219, 40)) AS taker_token_amount_raw,
            bytea2numeric('0x' || substring(DATA, 283, 40)) AS maker_token_amount_raw,
            'BridgeFill'                                    AS type,
            zeroex_tx.affiliate_address                     AS affiliate_address,
            TRUE                                            AS swap_flag,
            FALSE                                           AS matcha_limit_order_flag
    FROM {{ source('polygon', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic1 = '0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9'
        AND contract_address = '0xdb6f1920a889355780af7570773609bd8cb1f498'

        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND block_time >= '{{zeroex_v4_start_date}}'
        {% endif %}
),*/
NewBridgeFill AS (
    SELECT 
        logs.tx_hash,
        logs.INDEX                                           AS evt_index,
        logs.contract_address,
        logs.block_time                                      AS block_time,
        '0x' || substring(logs.DATA, 27, 40)                 AS maker,
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff'         AS taker,
        '0x' || substring(logs.DATA, 91, 40)                 AS taker_token,
        '0x' || substring(logs.DATA, 155, 40)                AS maker_token,
        bytea2numeric('0x' || substring(logs.DATA, 219, 40)) AS taker_token_amount_raw,
        bytea2numeric('0x' || substring(logs.DATA, 283, 40)) AS maker_token_amount_raw,
        'BridgeFill'                                         AS type,
        zeroex_tx.affiliate_address                          AS affiliate_address,
        zeroex_tx.block_number,
        TRUE                                                 AS swap_flag,
        FALSE                                                AS matcha_limit_order_flag
    FROM {{ source('polygon' ,'logs') }} logs
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = logs.tx_hash
        AND zeroex_tx.block_number = logs.block_number
        AND zeroex_tx.block_time = logs.block_time

    WHERE logs.topic1 = '0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8'
        AND logs.contract_address = '0xdb6f1920a889355780af7570773609bd8cb1f498'

        {% if is_incremental() %}
        AND logs.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND logs.block_time >= '{{zeroex_v4_start_date}}'
        {% endif %}
),

direct_PLP AS (
    SELECT 
            plp.evt_tx_hash             AS tx_hash,
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
            zeroex_tx.block_number,
            TRUE                        AS swap_flag,
            FALSE                       AS matcha_limit_order_flag
    FROM {{ source('zeroex_polygon', 'ExchangeProxy_evt_LiquidityProviderSwap') }} plp
    INNER JOIN zeroex_tx
        ON zeroex_tx.tx_hash = plp.evt_tx_hash
        AND zeroex_tx.block_number = plp.evt_block_number
        AND zeroex_tx.block_time = plp.evt_block_time

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{zeroex_v3_start_date}}'
    {% endif %}
), 
/*
direct_uniswapv3 AS (
    SELECT 
            swap.evt_tx_hash                                                                        AS tx_hash,
            swap.evt_index,
            swap.contract_address,
            swap.evt_block_time                                                                     AS block_time,
            swap.contract_address                                                                   AS maker,
            LAST_VALUE(swap.recipient) OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
            CASE WHEN amount0 < '0' THEN pair.token1 ELSE pair.token0 END                           AS taker_token,
            CASE WHEN amount0 < '0' THEN pair.token0 ELSE pair.token1 END                           AS maker_token,
            CASE
                WHEN amount0 < '0' THEN abs(swap.amount1)
                ELSE abs(swap.amount0) END                                                          AS taker_token_amount_raw,
            CASE
                WHEN amount0 < '0' THEN abs(swap.amount0)
                ELSE abs(swap.amount1) END                                                          AS maker_token_amount_raw,
            'Uniswap V3 Direct'                                                                     AS type,
            zeroex_tx.affiliate_address                                                             AS affiliate_address,
            TRUE                                                                                    AS swap_flag,
            FALSE                                                                                   AS matcha_limit_order_flag
    FROM {{ source('uniswap_v3_polygon', 'UniswapV3Pool_evt_Swap') }} swap
   LEFT JOIN {{ source('uniswap_v3_polygon', 'Factory_evt_PoolCreated') }} pair ON pair.pool = swap.contract_address
   INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = swap.evt_tx_hash
   WHERE sender = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'

        {% if is_incremental() %}
        AND swap.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND swap.evt_block_time >= '{{zeroex_v4_start_date}}'
        {% endif %}

), */
all_tx AS (
    /*
    SELECT *
    FROM direct_uniswapv3
    UNION ALL
    */
    SELECT *
    FROM direct_PLP
    UNION ALL
    SELECT *
    FROM ERC20BridgeTransfer
    /* UNION ALL
    SELECT *
    FROM BridgeFill */
    UNION ALL 
    SELECT *
    FROM v4_limit_fills_no_bridge
    UNION ALL 
    SELECT *
    FROM NewBridgeFill 
    UNION ALL
    SELECT *
    FROM v4_rfq_fills_no_bridge
    UNION ALL
    SELECT *
    FROM otc_fills 
)

SELECT distinct 
        all_tx.tx_hash,
        all_tx.block_number, 
        all_tx.evt_index,
        all_tx.contract_address,
        all_tx.block_time,
        try_cast(date_trunc('day', all_tx.block_time) AS date) AS block_date,
        maker,
        tx.from AS taker, -- fix the user masked by ProxyContract issue
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
        CASE WHEN maker_token IN ('0x2791bca1f2de4661ed88a30c99a7a9449aa84174','0x7ceb23fd6bc0add59e62ac25578270cff1b9f619','0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270','0xc2132d05d31c914a87c6611c10748aeb04b58e8f',
            '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6','0x8f3cf7ad23cd3cadbd9735aff958023239c6a063','0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4') AND  mp.price IS NOT NULL
             THEN (all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price
             WHEN taker_token IN ('0x2791bca1f2de4661ed88a30c99a7a9449aa84174','0x7ceb23fd6bc0add59e62ac25578270cff1b9f619','0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270','0xc2132d05d31c914a87c6611c10748aeb04b58e8f',
                '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6','0x8f3cf7ad23cd3cadbd9735aff958023239c6a063','0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4')  AND  tp.price IS NOT NULL
             THEN (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price
             ELSE COALESCE((all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price)
             END AS volume_usd,
        tx.from AS tx_from,
        tx.to AS tx_to,
        'polygon' AS blockchain
FROM all_tx


INNER JOIN {{ source('polygon', 'transactions')}} tx ON all_tx.tx_hash = tx.hash

{% if is_incremental() %}
AND tx.block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
AND tx.block_time >= '{{zeroex_v3_start_date}}'
{% endif %}


LEFT JOIN {{ source('prices', 'usd') }} tp ON date_trunc('minute', all_tx.block_time) = tp.minute
AND CASE
        WHEN all_tx.taker_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x0000000000000000000000000000000000001010'
        WHEN all_tx.taker_token = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' THEN '0x0000000000000000000000000000000000001010'
        ELSE all_tx.taker_token
    END = tp.contract_address
AND tp.blockchain = 'polygon'

{% if is_incremental() %}
AND tp.minute >= date_trunc('day', now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
AND tp.minute >= '{{zeroex_v3_start_date}}'
{% endif %}

LEFT JOIN {{ source('prices', 'usd') }} mp ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
AND CASE
        WHEN all_tx.maker_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x0000000000000000000000000000000000001010'
        WHEN all_tx.taker_token = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' THEN '0x0000000000000000000000000000000000001010'
        ELSE all_tx.maker_token
    END = mp.contract_address
AND mp.blockchain = 'polygon'

{% if is_incremental() %}
AND mp.minute >= date_trunc('day', now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
AND mp.minute >= '{{zeroex_v3_start_date}}'
{% endif %}

LEFT OUTER JOIN {{ ref('tokens_erc20') }} ts ON ts.contract_address = taker_token and ts.blockchain = 'polygon'
LEFT OUTER JOIN {{ ref('tokens_erc20') }} ms ON ms.contract_address = maker_token and ms.blockchain = 'polygon'

WHERE all_tx.tx_hash != '0x34ee112f3d601e4bb2f19f7744e86f9b4f65ed6c44dfe48db1c560d6b1c34bef' -- exclude tx with wrong decimals data
