{% macro zeroex_tx_cte(blockchain, start_date) %}
    SELECT
        tx_hash,
        max(affiliate_address) as affiliate_address,
        is_gasless
    FROM (
        SELECT
            tr.tx_hash,
            CASE
                WHEN bytearray_position(INPUT, 0x869584cd) <> 0 THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0x869584cd) + 16) FOR 20)
                WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0 THEN SUBSTRING(INPUT FROM (bytearray_position(INPUT, 0xfbc019a7) + 16) FOR 20)
            END AS affiliate_address,
            case when (varbinary_position(input, 0x3d8d4082) <> 0 or varbinary_position(input, 0x4f948110) <> 0) then 1 else 0 end as is_gasless
        FROM {{ source(blockchain, 'traces') }} tr
        WHERE tr.to IN (
            0xdef1c0ded9bec7f1a1670819833240f027b25eff,
            0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5,
            0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be,
            0x4ef40d1bf0983899892946830abf99eca2dbc5ce,
            0xdef189deaef76e379df891899eb5a00a94cbc250
        )
        AND (
            bytearray_position(INPUT, 0x869584cd) <> 0
            OR bytearray_position(INPUT, 0xfbc019a7) <> 0
        )
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not is_incremental() %}
        AND block_time >= cast('{{ start_date }}' as date)
        {% endif %}
    ) temp
    group by tx_hash, is_gasless
{% endmacro %}

{% macro zeroex_main_events_cte(blockchain, start_date, contract_address) %}
    SELECT
        logs.tx_hash,
        logs.block_number AS block_number,
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
        FALSE AS matcha_limit_order_flag,
        is_gasless
    FROM {{ source(blockchain, 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 IN (
        0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9,
        0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8
    )
    AND contract_address = {{ contract_address }}
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not is_incremental() %}
    AND block_time >= cast('{{ start_date }}' as date)
    {% endif %}
{% endmacro %}

{% macro zeroex_api_fills(blockchain, native_token_address, wrapped_native_token_address, stablecoin_addresses) %}
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
        WHEN is_gasless = 1 THEN CASE WHEN (varbinary_substring(data,177,19)) = 0x00000000000000000000000000000000000000 THEN varbinary_substring(data,81,20) ELSE (varbinary_substring(data,177,20)) END
        WHEN taker = 0xdef1c0ded9bec7f1a1670819833240f027b25eff THEN tx."from"
        ELSE taker
    END AS taker,
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

    CASE
        WHEN maker_token IN (
            {% for address in stablecoin_addresses %}
                {{ "0x" + address[2:] }}{% if not loop.last %},{% endif %}
            {% endfor %}
        )
        THEN (all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price
        WHEN taker_token IN (
            {% for address in stablecoin_addresses %}
                {{ "0x" + address[2:] }}{% if not loop.last %},{% endif %}
            {% endfor %}
        )
        THEN (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price
        ELSE COALESCE((all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price)
    END AS volume_usd,

    tx."from" AS tx_from,
    tx.to AS tx_to,
    '{{ blockchain }}' AS blockchain
FROM all_tx
INNER JOIN {{ source(blockchain, 'transactions') }} tx ON all_tx.tx_hash = tx.hash
LEFT JOIN {{ source('prices', 'usd') }} tp ON date_trunc('minute', all_tx.block_time) = tp.minute
    AND CASE
        WHEN all_tx.taker_token = {{ native_token_address }} THEN {{ wrapped_native_token_address }}
        ELSE all_tx.taker_token
    END = tp.contract_address
    AND tp.blockchain = '{{ blockchain }}'
LEFT JOIN {{ source('prices', 'usd') }} mp ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
    AND CASE
        WHEN all_tx.maker_token = {{ native_token_address }} THEN {{ wrapped_native_token_address }}
        ELSE all_tx.maker_token
    END = mp.contract_address
    AND mp.blockchain = '{{ blockchain }}'
LEFT OUTER JOIN {{ source('tokens', 'erc20') }} ts ON ts.contract_address = taker_token and ts.blockchain = '{{ blockchain }}'
LEFT OUTER JOIN {{ source('tokens', 'erc20') }} ms ON ms.contract_address = maker_token and ms.blockchain = '{{ blockchain }}'
{% if is_incremental() %}
WHERE {{ incremental_predicate('all_tx.block_time') }}
{% endif %}
{% endmacro %}
