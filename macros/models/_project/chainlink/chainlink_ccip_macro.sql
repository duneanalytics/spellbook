{% macro 
    chainlink_ccip_tokens_transferred(
        blockchain
    ) 
%}

SELECT DISTINCT
    le.blockchain,
    le.block_time,
    le.tx_hash,
    COALESCE(le.total_tokens / POWER(
        CAST(10 AS double precision),
        CAST(t.decimals AS double precision)
    ), 0) AS total_tokens,
    COALESCE(sr.destination_blockchain, 'UNKNOWN') AS destination_blockchain,
    te.contract_address AS token,
    COALESCE(t.symbol, 'UNKNOWN') AS token_symbol,
    COALESCE(p.price * (
        le.total_tokens / POWER(
            CAST(10 AS double precision),
            CAST(t.decimals AS double precision)
        )
    ), 0) AS token_price
FROM
    {{ref('chainlink_' ~ blockchain ~ '_ccip_tokens_transferred_logs')}} le
    INNER JOIN {{ref('chainlink_' ~ blockchain ~ '_ccip_send_requested')}} sr ON le.tx_hash = sr.tx_hash
    AND le.block_time = sr.evt_block_time
    {% if is_incremental() %}
        AND {{ incremental_predicate('sr.evt_block_time') }}
    {% endif %}
    INNER JOIN  {{ source('erc20_' ~ blockchain, 'evt_transfer') }} te ON le.contract_address = te.to
    AND te.evt_tx_hash = le.tx_hash
    AND te.evt_block_time = le.block_time
    {% if is_incremental() %}
        AND {{ incremental_predicate('te.evt_block_time') }}
    {% endif %}
    LEFT JOIN {{ source('tokens', 'erc20') }} t ON te.contract_address = t.contract_address
    AND t.blockchain = le.blockchain
    LEFT JOIN {{ source('prices', 'usd') }} p ON te.contract_address = p.contract_address
    AND p.blockchain = le.blockchain
    AND p.minute = DATE_TRUNC ('minute', le.block_time)
    {% if is_incremental() %}
        AND {{ incremental_predicate('p.minute') }}
    {% endif %}


{% endmacro %}