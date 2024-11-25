{% macro zeroex_evt_fills_txs(blockchain,zeroex_v3_start_date) %}
{%- set table_prefix = 'zeroex_v3_' + blockchain -%}

SELECT
    v3.evt_tx_hash AS tx_hash,
    CASE
        WHEN takerAddress = 0x63305728359c088a52b0b0eeec235db4d31a67fc THEN takerAddress
        ELSE NULL
    END AS affiliate_address,
    NULL AS is_gasless,
    evt_block_time AS block_time
FROM {{ source(table_prefix, 'Exchange_evt_Fill') }} v3
WHERE (  -- nuo
    v3.takerAddress = 0x63305728359c088a52b0b0eeec235db4d31a67fc
    OR  -- contains a bridge order
    (
        v3.feeRecipientAddress = 0x1000000000000000000000000000000000000011
        AND bytearray_substring(v3.makerAssetData, 1, 4) = 0xdc1600f3
    )
)
{% if is_incremental() %}
AND {{ incremental_predicate('evt_block_time') }}
{% else %}
AND evt_block_time >= cast('{{zeroex_v3_start_date}}' as date)
{% endif %}
{% endmacro %}


{% macro zeroex_v1_txs(blockchain,zeroex_v3_start_date) %}
SELECT
    tr.tx_hash,
    CASE
        WHEN bytearray_position(INPUT, 0x869584cd) <> 0 THEN
            SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0x869584cd) + 16) FOR 20)
        WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0 THEN
            SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0xfbc019a7) + 16) FOR 20)
    END AS affiliate_address,
    CASE
        WHEN (varbinary_position(input, 0x3d8d4082) <> 0 OR varbinary_position(input, 0x4f948110) <> 0)
        THEN 1
    END AS is_gasless,
    block_time
FROM {{ source(blockchain, 'traces') }} tr
WHERE tr.to IN (
    0x61935cbdd02287b511119ddb11aeb42f1593b7ef,  -- exchange contract
    0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5,  -- forwarder address
    0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be,  -- forwarder address
    0x4ef40d1bf0983899892946830abf99eca2dbc5ce,  -- forwarder address
    0xdef1c0ded9bec7f1a1670819833240f027b25eff   -- exchange proxy
)
AND (
    bytearray_position(INPUT, 0x869584cd) <> 0
    OR bytearray_position(INPUT, 0xfbc019a7) <> 0
)
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% else %}
AND block_time >= cast('{{zeroex_v3_start_date}}' as date)
{% endif %}
{% endmacro %}


{% macro v3_fills_no_bridge(blockchain,zeroex_v3_start_date) %}
{%- set table_prefix = 'zeroex_v3_' + blockchain -%}

SELECT
    fills.evt_tx_hash AS tx_hash,
    fills.evt_index,
    fills.contract_address,
    evt_block_time AS block_time,
    fills.makerAddress AS maker,
    fills.takerAddress AS taker,
    bytearray_substring(fills.takerAssetData, 17, 20) AS taker_token,
    bytearray_substring(fills.makerAssetData, 17, 20) AS maker_token,
    CAST(fills.takerAssetFilledAmount AS int256) AS taker_token_amount_raw,
    CAST(fills.makerAssetFilledAmount AS int256) AS maker_token_amount_raw,
    'Fill' AS type,
    COALESCE(zeroex_tx.affiliate_address, fills.feeRecipientAddress) AS affiliate_address,
    (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
    (fills.feeRecipientAddress = 0x86003b044f70dac0abc80ac8957305b6370893ed) AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'Exchange_evt_Fill') }} fills
INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = fills.evt_tx_hash AND fills.evt_block_time = zeroex_tx.block_time
WHERE
    -- Exclude bridge orders
    (bytearray_substring(makerAssetData, 1, 4) <> 0xdc1600f3)
    AND
    (
        -- Include transactions with a matching tx_hash in zeroex_tx or specific feeRecipientAddress
        zeroex_tx.tx_hash IS NOT NULL
        OR fills.feeRecipientAddress = 0x86003b044f70dac0abc80ac8957305b6370893ed
    )

    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not is_incremental() %}
    AND evt_block_time >= CAST('{{zeroex_v3_start_date}}' AS DATE)
    {% endif %}
{% endmacro %}


{% macro v4_rfq_fills_no_bridge(blockchain, zeroex_v4_start_date) %}
{%- set table_prefix = 'zeroex_' + blockchain -%}
SELECT
    fills.evt_tx_hash AS tx_hash,
    fills.evt_index,
    fills.contract_address,
    fills.evt_block_time AS block_time,
    fills.maker,
    fills.taker,
    fills.takerToken AS taker_token,
    fills.makerToken AS maker_token,
    CAST(fills.takerTokenFilledAmount AS int256) AS taker_token_amount_raw,
    CAST(fills.makerTokenFilledAmount AS int256) AS maker_token_amount_raw,
    'RfqOrderFilled' AS type,
    zeroex_tx.affiliate_address,
    (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'ExchangeProxy_evt_RfqOrderFilled') }} fills
LEFT JOIN zeroex_tx
    ON zeroex_tx.tx_hash = fills.evt_tx_hash
    AND fills.evt_block_time = zeroex_tx.block_time

{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% else %}
WHERE evt_block_time >= CAST('{{zeroex_v4_start_date}}' AS DATE)
{% endif %}
{% endmacro %}


{% macro v4_limit_fills_no_bridge(blockchain, zeroex_v4_start_date) %}
{%- set table_prefix = 'zeroex_' + blockchain -%}
SELECT
    fills.evt_tx_hash AS tx_hash,
    fills.evt_index,
    fills.contract_address,
    fills.evt_block_time AS block_time,
    fills.maker,
    fills.taker,
    fills.takerToken AS taker_token,
    fills.makerToken AS maker_token,
    CAST(fills.takerTokenFilledAmount AS int256) AS taker_token_amount_raw,
    CAST(fills.makerTokenFilledAmount AS int256) AS maker_token_amount_raw,
    'LimitOrderFilled' AS type,
    COALESCE(zeroex_tx.affiliate_address, fills.feeRecipient) AS affiliate_address,
    (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
    (fills.feeRecipient IN (
        0x9b858be6e3047d88820f439b240deac2418a2551,
        0x86003b044f70dac0abc80ac8957305b6370893ed,
        0x5bc2419a087666148bfbe1361ae6c06d240c6131
    )) AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'ExchangeProxy_evt_LimitOrderFilled') }} fills
LEFT JOIN zeroex_tx
    ON zeroex_tx.tx_hash = fills.evt_tx_hash
    AND fills.evt_block_time = zeroex_tx.block_time

{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% else %}
WHERE evt_block_time >= CAST('{{zeroex_v4_start_date}}' AS DATE)
{% endif %}
{% endmacro %}

{% macro otc_fills(blockchain, zeroex_v4_start_date) %}
{%- set table_prefix = 'zeroex_' + blockchain -%}
SELECT
    fills.evt_tx_hash AS tx_hash,
    fills.evt_index,
    fills.contract_address,
    fills.evt_block_time AS block_time,
    fills.maker,
    0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
    fills.takerToken AS taker_token,
    fills.makerToken AS maker_token,
    CAST(fills.takerTokenFilledAmount AS int256) AS taker_token_amount_raw,
    CAST(fills.makerTokenFilledAmount AS int256) AS maker_token_amount_raw,
    'OtcOrderFilled' AS type,
    zeroex_tx.affiliate_address,
    (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'ExchangeProxy_evt_OtcOrderFilled') }} fills
LEFT JOIN zeroex_tx
    ON zeroex_tx.tx_hash = fills.evt_tx_hash
    AND fills.evt_block_time = zeroex_tx.block_time
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% else %}
WHERE evt_block_time >= CAST('{{zeroex_v4_start_date}}' AS DATE)
{% endif %}
{% endmacro %}


{% macro ERC20BridgeTransfer(blockchain, zeroex_v3_start_date) %}
SELECT
    logs.tx_hash,
    logs.index AS evt_index,
    logs.contract_address,
    logs.block_time,
    bytearray_substring(data, 141, 20) AS maker,
    bytearray_substring(data, 173, 20) AS taker,
    bytearray_substring(data, 13, 20) AS taker_token,
    bytearray_substring(data, 45, 20) AS maker_token,
    bytearray_to_int256(bytearray_substring(data, 77, 20)) AS taker_token_amount_raw,
    bytearray_to_int256(bytearray_substring(data, 109, 20)) AS maker_token_amount_raw,
    'ERC20BridgeTransfer' AS type,
    zeroex_tx.affiliate_address,
    TRUE AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(blockchain, 'logs') }} logs
JOIN zeroex_tx
    ON zeroex_tx.tx_hash = logs.tx_hash
    AND logs.block_time = zeroex_tx.block_time
WHERE topic0 = 0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9

{% if is_incremental() %}
AND {{ incremental_predicate('logs.block_time') }}
{% else %}
AND logs.block_time >= CAST('{{zeroex_v3_start_date}}' AS DATE)
{% endif %}
{% endmacro %}


{% macro BridgeFill(blockchain, zeroex_v4_start_date) %}
SELECT
    logs.tx_hash,
    logs.index AS evt_index,
    logs.contract_address,
    logs.block_time,
    bytearray_substring(data, 13, 20) AS maker,
    0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
    bytearray_substring(data, 45, 20) AS taker_token,
    bytearray_substring(data, 77, 20) AS maker_token,
    bytearray_to_int256(bytearray_substring(data, 109, 20)) AS taker_token_amount_raw,
    bytearray_to_int256(bytearray_substring(data, 141, 20)) AS maker_token_amount_raw,
    'BridgeFill' AS type,
    zeroex_tx.affiliate_address,
    TRUE AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(blockchain, 'logs') }} logs
JOIN zeroex_tx
    ON zeroex_tx.tx_hash = logs.tx_hash
    AND logs.block_time = zeroex_tx.block_time
WHERE topic0 = 0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9
    AND contract_address = 0x22f9dcf4647084d6c31b2765f6910cd85c178c18

{% if is_incremental() %}
AND {{ incremental_predicate('logs.block_time') }}
{% else %}
AND logs.block_time >= CAST('{{zeroex_v4_start_date}}' AS DATE)
{% endif %}
{% endmacro %}

{% macro NewBridgeFill(blockchain, zeroex_v4_start_date) %}
SELECT
    logs.tx_hash,
    logs.index AS evt_index,
    logs.contract_address,
    logs.block_time,
    bytearray_substring(data, 13, 20) AS maker,
    0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
    bytearray_substring(data, 45, 20) AS taker_token,
    bytearray_substring(data, 77, 20) AS maker_token,
    bytearray_to_int256(bytearray_substring(data, 109, 20)) AS taker_token_amount_raw,
    bytearray_to_int256(bytearray_substring(data, 141, 20)) AS maker_token_amount_raw,
    'NewBridgeFill' AS type,
    zeroex_tx.affiliate_address,
    TRUE AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(blockchain, 'logs') }} logs
JOIN zeroex_tx
    ON zeroex_tx.tx_hash = logs.tx_hash
    AND logs.block_time = zeroex_tx.block_time
WHERE topic0 = 0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8
    AND contract_address = 0x22f9dcf4647084d6c31b2765f6910cd85c178c18

{% if is_incremental() %}
AND {{ incremental_predicate('logs.block_time') }}
{% else %}
AND logs.block_time >= CAST('{{zeroex_v4_start_date}}' AS DATE)
{% endif %}
{% endmacro %}

{% macro direct_PLP(blockchain, zeroex_v3_start_date) %}
{%- set table_prefix = 'zeroex_' + blockchain -%}
SELECT
    plp.evt_tx_hash,
    plp.evt_index AS evt_index,
    plp.contract_address,
    plp.evt_block_time AS block_time,
    provider AS maker,
    recipient AS taker,
    inputToken AS taker_token,
    outputToken AS maker_token,
    CAST(inputTokenAmount AS int256) AS taker_token_amount_raw,
    CAST(outputTokenAmount AS int256) AS maker_token_amount_raw,
    'LiquidityProviderSwap' AS type,
    zeroex_tx.affiliate_address,
    TRUE AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'ExchangeProxy_evt_LiquidityProviderSwap') }} plp
JOIN zeroex_tx
    ON zeroex_tx.tx_hash = plp.evt_tx_hash
    AND plp.evt_block_time = zeroex_tx.block_time

{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% else %}
WHERE evt_block_time >= CAST('{{zeroex_v3_start_date}}' AS DATE)
{% endif %}
{% endmacro %}

{% macro direct_uniswapv2(blockchain, zeroex_v3_start_date) %}
{%- set table_prefix = 'uniswap_v2_' + blockchain -%}
SELECT
    swap.evt_tx_hash AS tx_hash,
    swap.evt_index,
    swap.contract_address,
    swap.evt_block_time AS block_time,
    swap.contract_address AS maker,
    0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN pair.token0
        ELSE pair.token1
    END AS taker_token,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN pair.token1
        ELSE pair.token0
    END AS maker_token,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN
            CASE
                WHEN swap.amount0In >= swap.amount0Out THEN CAST(swap.amount0In - swap.amount0Out AS int256)
                ELSE CAST(0 AS int256)
            END
        ELSE
            CASE
                WHEN swap.amount1In >= swap.amount1Out THEN CAST(swap.amount1In - swap.amount1Out AS int256)
                ELSE CAST(0 AS int256)
            END
    END AS taker_token_amount_raw,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN
            CASE
                WHEN swap.amount1Out >= swap.amount1In THEN CAST(swap.amount1Out - swap.amount1In AS int256)
                ELSE CAST(0 AS int256)
            END
        ELSE
            CASE
                WHEN swap.amount0Out >= swap.amount0In THEN CAST(swap.amount0Out - swap.amount0In AS int256)
                ELSE CAST(0 AS int256)
            END
    END AS maker_token_amount_raw,
    'Uniswap V2 Direct' AS type,
    zeroex_tx.affiliate_address,
    TRUE AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'Pair_evt_Swap') }} swap
LEFT JOIN {{ source(table_prefix, 'Factory_evt_PairCreated') }} pair
    ON pair.pair = swap.contract_address
JOIN zeroex_tx
    ON zeroex_tx.tx_hash = swap.evt_tx_hash
    AND swap.evt_block_time = zeroex_tx.block_time
WHERE sender = 0xdef1c0ded9bec7f1a1670819833240f027b25eff
    {% if is_incremental() %}
    AND {{ incremental_predicate('swap.evt_block_time') }}
    {% else %}
    AND swap.evt_block_time >= CAST('{{zeroex_v3_start_date}}' AS DATE)
    {% endif %}
{% endmacro %}

{% macro direct_sushiswap(blockchain, zeroex_v3_start_date) %}
{%- set table_prefix = 'sushi_' + blockchain -%}
SELECT
    swap.evt_tx_hash AS tx_hash,
    swap.evt_index,
    swap.contract_address,
    swap.evt_block_time AS block_time,
    swap.contract_address AS maker,
    LAST_VALUE(swap.to) OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN pair.token0
        ELSE pair.token1
    END AS taker_token,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN pair.token1
        ELSE pair.token0
    END AS maker_token,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN
            CASE
                WHEN swap.amount0In >= swap.amount0Out THEN CAST(swap.amount0In - swap.amount0Out AS int256)
                ELSE CAST(0 AS int256)
            END
        ELSE
            CASE
                WHEN swap.amount1In >= swap.amount1Out THEN CAST(swap.amount1In - swap.amount1Out AS int256)
                ELSE CAST(0 AS int256)
            END
    END AS taker_token_amount_raw,
    CASE
        WHEN swap.amount0In > swap.amount0Out THEN
            CASE
                WHEN swap.amount1Out >= swap.amount1In THEN CAST(swap.amount1Out - swap.amount1In AS int256)
                ELSE CAST(0 AS int256)
            END
        ELSE
            CASE
                WHEN swap.amount0Out >= swap.amount0In THEN CAST(swap.amount0Out - swap.amount0In AS int256)
                ELSE CAST(0 AS int256)
            END
    END AS maker_token_amount_raw,
    'Sushiswap Direct' AS type,
    zeroex_tx.affiliate_address,
    TRUE AS swap_flag,
    FALSE AS matcha_limit_order_flag,
    is_gasless
FROM {{ source(table_prefix, 'Pair_evt_Swap') }} swap
LEFT JOIN {{ source(table_prefix, 'Factory_evt_PairCreated') }} pair
    ON pair.pair = swap.contract_address
JOIN zeroex_tx
    ON zeroex_tx.tx_hash = swap.evt_tx_hash
    AND swap.evt_block_time = zeroex_tx.block_time
WHERE sender = 0xdef1c0ded9bec7f1a1670819833240f027b25eff
    {% if is_incremental() %}
    AND {{ incremental_predicate('swap.evt_block_time') }}
    {% else %}
    AND swap.evt_block_time >= CAST('{{zeroex_v3_start_date}}' AS DATE)
    {% endif %}
{% endmacro %}


{% macro direct_uniswapv3(blockchain,zeroex_v4_start_date) %}
{%- set table_prefix = 'uniswap_v3_' + blockchain -%}
    SELECT
        swap.evt_tx_hash                                                AS tx_hash,
        swap.evt_index,
        swap.contract_address,
        swap.evt_block_time                                             AS block_time,
        swap.contract_address                                           AS maker,
        0xdef1c0ded9bec7f1a1670819833240f027b25eff                      AS taker,
        CASE
            WHEN amount0 < CAST(0 AS int256) THEN pair.token1
            ELSE pair.token0
        END                                                             AS taker_token,
        CASE
            WHEN amount0 < CAST(0 AS int256) THEN pair.token0
            ELSE pair.token1
        END                                                             AS maker_token,
        CASE
            WHEN amount0 < CAST(0 AS int256) THEN ABS(swap.amount1)
            ELSE ABS(swap.amount0)
        END                                                             AS taker_token_amount_raw,
        CASE
            WHEN amount0 < CAST(0 AS int256) THEN ABS(swap.amount0)
            ELSE ABS(swap.amount1)
        END                                                             AS maker_token_amount_raw,
        'Uniswap V3 Direct'                                             AS type,
        zeroex_tx.affiliate_address                                     AS affiliate_address,
        TRUE                                                            AS swap_flag,
        FALSE                                                           AS matcha_limit_order_flag,
        is_gasless
    FROM {{ source(table_prefix, 'Pair_evt_Swap') }} swap
    LEFT JOIN {{ source(table_prefix, 'Factory_evt_PoolCreated') }} pair
        ON pair.pool = swap.contract_address
    JOIN zeroex_tx
        ON zeroex_tx.tx_hash = swap.evt_tx_hash
        AND swap.evt_block_time = zeroex_tx.block_time
    WHERE sender = 0xdef1c0ded9bec7f1a1670819833240f027b25eff
        {% if is_incremental() %}
        AND {{ incremental_predicate('swap.evt_block_time') }}
        {% endif %}
        {% if not is_incremental() %}
        AND swap.evt_block_time >= CAST('{{zeroex_v4_start_date}}' AS DATE)
        {% endif %}
{% endmacro %}

{% macro trade_details(blockchain, zeroex_v3_start_date) %}
WITH results AS (
    SELECT
        all_tx.tx_hash,
        tx.block_number,
        all_tx.evt_index,
        all_tx.contract_address,
        all_tx.block_time,
        CAST(DATE_TRUNC('day', all_tx.block_time) AS DATE) AS block_date,
        CAST(DATE_TRUNC('month', all_tx.block_time) AS DATE) AS block_month,
        maker,
        CASE
            WHEN is_gasless = 1 AND VARBINARY_POSITION(data, 0x3a46c4e1) <> 0 THEN VARBINARY_SUBSTRING(data, 81, 20)
            WHEN is_gasless = 1 AND VARBINARY_POSITION(data, 0xa98fcbf1) <> 0 THEN VARBINARY_SUBSTRING(data, 81, 20)
            WHEN is_gasless = 1 AND VARBINARY_POSITION(data, 0x3d8d4082) <> 0 THEN VARBINARY_SUBSTRING(data, 177, 20)
            WHEN taker = 0xdef1c0ded9bec7f1a1670819833240f027b25eff THEN tx."from"
            ELSE taker
        END AS taker,
        taker_token,
        taker_token AS token_sold_address,
        ts.symbol AS taker_symbol,
        maker_token,
        maker_token AS token_bought_address,
        ms.symbol AS maker_symbol,
        CASE
            WHEN LOWER(ts.symbol) > LOWER(ms.symbol) THEN CONCAT(ms.symbol, '-', ts.symbol)
            ELSE CONCAT(ts.symbol, '-', ms.symbol)
        END AS token_pair,
        taker_token_amount_raw / POW(10, tp.decimals) AS taker_token_amount,
        taker_token_amount_raw / POW(10, tp.decimals) AS token_sold_amount,
        CAST(taker_token_amount_raw AS UINT256) AS taker_token_amount_raw,
        maker_token_amount_raw / POW(10, mp.decimals) AS maker_token_amount,
        maker_token_amount_raw / POW(10, mp.decimals) AS token_bought_amount,
        CAST(maker_token_amount_raw AS UINT256) AS maker_token_amount_raw,
        all_tx.type,
        MAX(affiliate_address) OVER (PARTITION BY all_tx.tx_hash) AS affiliate_address,
        swap_flag,
        matcha_limit_order_flag,
        tx."from" AS tx_from,
        tx.to AS tx_to,
        '{{ blockchain }}' AS blockchain
    FROM all_tx
    INNER JOIN {{ source(blockchain, 'transactions') }} tx
        ON all_tx.tx_hash = tx.hash AND all_tx.block_time = tx.block_time
        {% if is_incremental() %}
        AND {{ incremental_predicate('tx.block_time') }}
        {% endif %}
        {% if not is_incremental() %}
        AND tx.block_time >= CAST('{{ zeroex_v3_start_date }}' AS DATE)
        {% endif %}
    LEFT JOIN {{ source('prices', 'usd') }} tp
        ON DATE_TRUNC('minute', all_tx.block_time) = tp.minute
        AND CASE
            WHEN all_tx.taker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE all_tx.taker_token
        END = tp.contract_address
        AND tp.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('tp.minute') }}
        {% endif %}
        {% if not is_incremental() %}
        AND tp.minute >= CAST('{{ zeroex_v3_start_date }}' AS DATE)
        {% endif %}
    LEFT JOIN {{ source('prices', 'usd') }} mp
        ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
        AND CASE
            WHEN all_tx.maker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE all_tx.maker_token
        END = mp.contract_address
        AND mp.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('mp.minute') }}
        {% endif %}
        {% if not is_incremental() %}
        AND mp.minute >= CAST('{{ zeroex_v3_start_date }}' AS DATE)
        {% endif %}
    LEFT JOIN {{ source('tokens', 'erc20') }} ts
        ON ts.contract_address = taker_token AND ts.blockchain = '{{ blockchain }}'
    LEFT JOIN {{ source('tokens', 'erc20') }} ms
        ON ms.contract_address = maker_token AND ms.blockchain = '{{ blockchain }}'
),

results_usd AS (
    {{
        add_amount_usd(
            trades_cte = 'results'
        )
    }}
)
select * from results_usd 
{% endmacro %}
