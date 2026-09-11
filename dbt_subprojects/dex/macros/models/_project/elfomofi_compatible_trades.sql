{% macro elfomofi_compatible_trades(blockchain, logs, start_date) %}

-- ElfomoTrade(uint256 indexed quoteId, uint256 indexed partnerId, address executor,
--             address receiver, address fromToken, address toToken, uint256 fromAmount, uint256 toAmount)
-- The indexed IDs are in topics; data contains six ABI words beginning with executor.
SELECT
    '{{ blockchain }}' AS blockchain,
    'elfomofi' AS project,
    '1' AS version,
    CAST(date_trunc('month', t.block_time) AS date) AS block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    varbinary_to_uint256(varbinary_substring(t.data, 161, 32)) AS token_bought_amount_raw,
    varbinary_to_uint256(varbinary_substring(t.data, 129, 32)) AS token_sold_amount_raw,
    varbinary_substring(t.data, 109, 20) AS token_bought_address,
    varbinary_substring(t.data, 77, 20) AS token_sold_address,
    varbinary_substring(t.data, 45, 20) AS taker,
    t.contract_address AS maker,
    t.contract_address AS project_contract_address,
    t.tx_hash,
    t.index AS evt_index
FROM {{ logs }} t
WHERE t.contract_address = 0xf0f0f0f0fb0d738452efd03a28e8be14c76d5f73
    AND t.topic0 = 0xbe65a3f1f381da16732df786f571604a72b7c122cff3ae2b355566ddf01e2528
    {% if is_incremental() -%}
    AND {{ incremental_predicate('t.block_date') }}
    AND {{ incremental_predicate('t.block_time') }}
    {% else -%}
    AND t.block_date >= DATE '{{ start_date }}'
    {% endif -%}

{% endmacro %}
