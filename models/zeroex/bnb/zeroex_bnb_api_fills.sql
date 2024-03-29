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

-- Test Query here: https://dune.com/queries/2274187
WITH zeroex_tx AS (
    SELECT tx_hash,
           max(affiliate_address) as affiliate_address
    FROM (
        SELECT
            v3.evt_tx_hash AS tx_hash,
            CASE
                WHEN takerAddress = 0x63305728359c088a52b0b0eeec235db4d31a67fc THEN takerAddress
                ELSE CAST(NULL as varbinary)
            END AS affiliate_address
        FROM
            {{ source('zeroex_v2_bnb', 'Exchange_evt_Fill') }} v3
        WHERE
            ( -- nuo
                v3.takerAddress = 0x63305728359c088a52b0b0eeec235db4d31a67fc
                OR -- contains a bridge order
                (
                    v3.feeRecipientAddress = 0x1000000000000000000000000000000000000011
                    AND bytearray_substring(v3.makerAssetData, 1, 4) = 0xdc1600f3
                )
            )
            {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
            {% endif %}

        UNION ALL

        SELECT
            tr.tx_hash AS tx_hash,
            CASE
                WHEN bytearray_position(INPUT, 0x869584cd ) <> 0
                    THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0x869584cd) + 16) FOR 20)
                WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0
                    THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0xfbc019a7 ) + 16) FOR 20)
            END AS affiliate_address            
        FROM {{ source('bnb', 'traces') }} tr
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
                AND block_time >= date_trunc('day', now() - interval '7' day)
                {% else %}
                AND block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
                {% endif %}
    ) temp
    group by tx_hash

),/*
direct_sushiswap AS (
    SELECT
            swap.evt_tx_hash AS tx_hash,
            swap.evt_index,
            swap.contract_address,
            swap.evt_block_time AS block_time,
            swap.contract_address AS maker,
            LAST_VALUE(swap.to) OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
            CASE WHEN swap.amount0In > swap.amount0Out THEN pair.token0 ELSE pair.token1 END AS taker_token,
            CASE WHEN swap.amount0In > swap.amount0Out THEN pair.token1 ELSE pair.token0 END AS maker_token,
            CASE WHEN swap.amount0In > swap.amount0Out THEN 
                CASE WHEN swap.amount0In >= swap.amount0Out THEN cast(swap.amount0In - swap.amount0Out as uint256) ELSE cast(0 as uint256) END ELSE 
                CASE WHEN swap.amount1In >= swap.amount1Out THEN cast(swap.amount1In - swap.amount1Out as uint256) ELSE cast(0 as uint256) END END AS taker_token_amount_raw,
            CASE WHEN swap.amount0In > swap.amount0Out THEN 
                CASE WHEN swap.amount1Out >= swap.amount1In THEN cast(swap.amount1Out - swap.amount1In as uint256) ELSE cast(0 as uint256) END ELSE 
                CASE WHEN swap.amount0Out >= swap.amount0In THEN cast(swap.amount0Out - swap.amount0In as uint256) ELSE cast(0 as uint256) END END AS maker_token_amount_raw,

            'Sushiswap Direct' AS type,
            zeroex_tx.affiliate_address AS affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
   FROM {{ source('sushi_bnb', 'UniswapV2Pair_evt_Swap') }} swap
   LEFT JOIN {{ source('sushi_bnb', 'UniswapV2Factory_evt_PairCreated') }} pair ON pair.pair = swap.contract_address
   JOIN zeroex_tx ON zeroex_tx.tx_hash = swap.evt_tx_hash
   WHERE sender = 0xdef1c0ded9bec7f1a1670819833240f027b25eff

        {% if is_incremental() %}
        AND swap.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not is_incremental() %}
        AND swap.evt_block_time >= cast('{{zeroex_v3_start_date}}' as date)
        {% endif %}
),*/
v2_fills_no_bridge AS (
    SELECT
            fills.evt_tx_hash                                                          AS tx_hash,
            fills.evt_index,
            fills.contract_address,
            evt_block_time                                                             AS block_time,
            fills.makerAddress                                                         AS maker,
            fills.takerAddress                                                         AS taker,
            bytearray_substring(fills.takerAssetData, 17, 20)                          AS taker_token,
            bytearray_substring(fills.makerAssetData, 17, 20)                          AS maker_token,
            fills.takerAssetFilledAmount                                               AS taker_token_amount_raw,
            fills.makerAssetFilledAmount                                               AS maker_token_amount_raw,
            'Fill'                                                                     AS type,
            COALESCE(zeroex_tx.affiliate_address, fills.feeRecipientAddress)           AS affiliate_address,
            (zeroex_tx.tx_hash IS NOT NULL)                                            AS swap_flag,
            (fills.feeRecipientAddress = 0x86003b044f70dac0abc80ac8957305b6370893ed) AS matcha_limit_order_flag
    FROM {{ source('zeroex_v2_bnb', 'Exchange_evt_Fill') }} fills
    JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash
    WHERE (bytearray_substring(makerAssetData, 1, 4) != 0xdc1600f3)
        AND (zeroex_tx.tx_hash IS NOT NULL
        OR fills.feeRecipientAddress = 0x86003b044f70dac0abc80ac8957305b6370893ed)

        {% if is_incremental() %}
         AND evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
         AND evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
        {% endif %}

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
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            FALSE                           AS matcha_limit_order_flag
    FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
    JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
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
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            (fills.feeRecipient = 0x9b858be6e3047d88820f439b240deac2418a2551) AS matcha_limit_order_flag
    FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
    JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
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
            (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
            FALSE                           AS matcha_limit_order_flag
    FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
    JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
    {% endif %}

),
ERC20BridgeTransfer AS (
    SELECT
            logs.tx_hash as tx_hash, 
            INDEX                                   AS evt_index,
            logs.contract_address,
            block_time                              AS block_time,
            bytearray_substring(DATA, 141, 20)      AS maker,
            bytearray_substring(DATA, 173, 20)      AS taker,
            bytearray_substring(DATA, 13, 20)       AS taker_token,
            bytearray_substring(DATA, 45, 20)       AS maker_token,
            bytearray_to_uint256(bytearray_substring(DATA, 77, 20)) AS taker_token_amount_raw,
            bytearray_to_uint256(bytearray_substring(DATA, 109, 20)) AS maker_token_amount_raw,
            'ERC20BridgeTransfer'                   AS type,
            zeroex_tx.affiliate_address             AS affiliate_address,
            TRUE                                    AS swap_flag,
            FALSE                                   AS matcha_limit_order_flag
    FROM {{ source('bnb', 'logs') }} logs
    JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = 0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9

    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}

),
BridgeFill AS (
    SELECT
            distinct logs.tx_hash,
            INDEX AS evt_index,
            logs.contract_address,
            block_time AS block_time,
            bytearray_substring(DATA, 13, 20) AS maker,
            0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
            bytearray_substring(DATA, 45, 20) AS taker_token,
            bytearray_substring(DATA, 77, 20) AS maker_token,
            bytearray_to_uint256(bytearray_substring(DATA, 109, 20)) AS taker_token_amount_raw,
            bytearray_to_uint256(bytearray_substring(DATA, 141, 20)) AS maker_token_amount_raw,
            'BridgeFill' AS type,
            zeroex_tx.affiliate_address AS affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
    FROM {{ source('bnb', 'logs') }} logs
    JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = 0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9
        AND contract_address = 0xdb6f1920a889355780af7570773609bd8cb1f498

        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
        {% endif %}
),

NewBridgeFill AS (
    SELECT
            logs.tx_hash,
            INDEX AS evt_index,
            logs.contract_address,
            block_time AS block_time,
            bytearray_substring(DATA, 13, 20) AS maker,
            0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
            bytearray_substring(DATA, 45, 20) AS taker_token,
            bytearray_substring(DATA, 77, 20) AS maker_token,
            bytearray_to_uint256(bytearray_substring(DATA, 109, 20)) AS taker_token_amount_raw,
            bytearray_to_uint256(bytearray_substring(DATA, 141, 20)) AS maker_token_amount_raw,
            'NewBridgeFill' AS type,
            zeroex_tx.affiliate_address AS affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
    FROM {{ source('bnb' ,'logs') }} logs
    JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = 0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8
        AND contract_address = 0xdb6f1920a889355780af7570773609bd8cb1f498
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
        {% endif %}
),
direct_PLP AS (
    SELECT
            plp.evt_tx_hash,
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
    FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_LiquidityProviderSwap') }} plp
    JOIN zeroex_tx ON zeroex_tx.tx_hash = plp.evt_tx_hash

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}

),
uni_v2_swap as (
    SELECT
        s.tx_hash
        , s.index as evt_index
        , s.contract_address
        , s.block_time
        , bytearray_SUBSTRING(DATA, 13, 20) AS maker
        , 0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker
        , (bytearray_to_uint256(bytearray_substring(DATA, 45, 20))) AS taker_token_amount_raw
        , (bytearray_to_uint256(bytearray_substring(DATA, 77, 20))) AS maker_token_amount_raw
        , 'direct_uniswapv2' AS TYPE
        , z.affiliate_address AS affiliate_address
        , TRUE AS swap_flag
        , FALSE AS matcha_limit_order_flag
    FROM {{ source('bnb', 'logs') }} s
    JOIN zeroex_tx z on z.tx_hash = s.tx_hash 
    WHERE topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 -- all the uni v2 swap event
        and topic1 = 0x000000000000000000000000def1c0ded9bec7f1a1670819833240f027b25eff -- 0x EP
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        AND block_time >= TIMESTAMP '{{zeroex_v4_start_date}}'
        {% endif %}
         
    
)  
, uni_v2_pair_creation as (
    SELECT
        bytearray_substring(data,13,20) as pair,
        bytearray_substring(topic1, 13, 20) AS makerToken,
        bytearray_substring(topic2, 13, 20) AS takerToken,
        row_number() over (partition by bytearray_substring(creation.data, 13, 20) order by block_time ) rn 
    FROM {{ source('bnb', 'logs') }} creation
    WHERE creation.topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9  -- all the uni v2 pair creation event
        
      
   
) , 
direct_uniswapv2 as (

select s.tx_hash, 
    s.evt_index,
    s.contract_address,
    s.block_time,
    maker, 
    taker,
    takerToken taker_token,
    makerToken maker_token,
    taker_token_amount_raw,
    maker_token_amount_raw,
    type,
    affiliate_address,
    swap_flag,
    matcha_limit_order_flag

from uni_v2_swap s 

join uni_v2_pair_creation creation on s.contract_address = creation.pair 
where rn = 1
),  

all_tx AS (
    SELECT *
    FROM direct_uniswapv2
    /*union ALL SELECT * 
    from direct_sushiswap */
    UNION ALL SELECT *
    FROM direct_PLP
    UNION ALL SELECT *
    FROM ERC20BridgeTransfer
    UNION ALL SELECT *
    FROM BridgeFill
    UNION ALL SELECT *
    FROM NewBridgeFill
    UNION ALL SELECT *
    FROM v2_fills_no_bridge
    UNION ALL SELECT *
    FROM v4_rfq_fills_no_bridge
    UNION ALL SELECT *
    FROM v4_limit_fills_no_bridge
    UNION ALL SELECT *
    FROM otc_fills
)

SELECT distinct
        all_tx.tx_hash,
        tx.block_number,
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
        taker_token_amount_raw / pow(10, ts.decimals) AS taker_token_amount,
        taker_token_amount_raw,
        maker_token_amount_raw / pow(10, ms.decimals) AS maker_token_amount,
        maker_token_amount_raw,
        all_tx.type,
        max(affiliate_address) over (partition by all_tx.tx_hash) as affiliate_address,
        swap_flag,
        matcha_limit_order_flag,
        CASE WHEN maker_token IN (0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c,0x55d398326f99059ff775485246999027b3197955,
                                0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d,0x7083609fce4d1d8dc0c979aab8c869ea2c873402,0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3)
             THEN (all_tx.maker_token_amount_raw / pow(10, ms.decimals)) * mp.price
             WHEN taker_token IN (0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c,0x55d398326f99059ff775485246999027b3197955,
                                0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d,0x7083609fce4d1d8dc0c979aab8c869ea2c873402,0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3)
             THEN (all_tx.taker_token_amount_raw / pow(10, ts.decimals)) * tp.price
             ELSE COALESCE((all_tx.maker_token_amount_raw / pow(10, ms.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, ts.decimals)) * tp.price)
             END AS volume_usd,
        tx."from" AS tx_from,
        tx.to AS tx_to,
        'bnb' AS blockchain
FROM all_tx
INNER JOIN {{ source('bnb', 'transactions')}} tx ON all_tx.tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND tx.block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} tp ON date_trunc('minute', all_tx.block_time) = tp.minute
    AND CASE
            WHEN all_tx.taker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
            WHEN all_tx.taker_token = 0x0000000000000000000000000000000000000000 THEN 0x55d398326f99059ff775485246999027b3197955
            ELSE all_tx.taker_token
        END = tp.contract_address
    AND tp.blockchain = 'bnb'
    {% if is_incremental() %}
    AND tp.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND tp.minute >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} mp ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
    AND CASE
            WHEN all_tx.maker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
            WHEN all_tx.maker_token = 0x0000000000000000000000000000000000000000 THEN 0x55d398326f99059ff775485246999027b3197955
            ELSE all_tx.maker_token
        END = mp.contract_address
    AND mp.blockchain = 'bnb'
    {% if is_incremental() %}
    AND mp.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND mp.minute >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}
LEFT OUTER JOIN {{ source('tokens', 'erc20') }} ts  ON ts.contract_address = case
                    WHEN all_tx.taker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
                    WHEN all_tx.taker_token = 0x0000000000000000000000000000000000000000 THEN 0x55d398326f99059ff775485246999027b3197955
                    ELSE all_tx.taker_token end
                AND ts.blockchain = 'bnb'
LEFT OUTER JOIN {{ source('tokens', 'erc20') }} ms ON ms.contract_address =
                case 
                    WHEN all_tx.maker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
                    WHEN all_tx.maker_token = 0x0000000000000000000000000000000000000000 THEN 0x55d398326f99059ff775485246999027b3197955
                    ELSE all_tx.maker_token end 
                AND ms.blockchain = 'bnb'