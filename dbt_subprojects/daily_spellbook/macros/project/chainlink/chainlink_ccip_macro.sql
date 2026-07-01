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
    -- CUR2-2973: inline chainlink_<chain>_ccip_send_requested instead of ref-ing the view. The view
    -- groups its source logs by tx_hash and exposes evt_block_time = MAX(block_time), so the post-agg
    -- incremental_predicate('sr.evt_block_time') (kept below as authoritative) cannot reach the
    -- <chain>.logs scan and every incremental run full-scans all history (~100B rows) to merge a few
    -- rows. Pushing incremental_predicate('block_time') into each UNION arm prunes the Delta logs scan
    -- via block_time file-skipping; sound because one tx = one block, so MAX(block_time) = block_time
    -- per tx_hash group and the same tx_hash groups are selected.
    INNER JOIN (
        SELECT
            MAX(block_time) AS evt_block_time,
            MAX(destination_blockchain) AS destination_blockchain,
            tx_hash
        FROM (
            SELECT
                ccip_logs_v1.block_time,
                ccip_logs_v1.destination_blockchain,
                ccip_logs_v1.tx_hash
            FROM {{ ref('chainlink_' ~ blockchain ~ '_ccip_send_requested_logs_v1') }} ccip_logs_v1
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('ccip_logs_v1.block_time') }}
            {% endif %}

            UNION ALL

            SELECT
                ccip_logs_v1_2.block_time,
                ccip_logs_v1_2.destination_blockchain,
                ccip_logs_v1_2.tx_hash
            FROM {{ ref('chainlink_' ~ blockchain ~ '_ccip_send_requested_logs_v1_2') }} ccip_logs_v1_2
            {% if is_incremental() %}
            WHERE {{ incremental_predicate('ccip_logs_v1_2.block_time') }}
            {% endif %}
        ) combined_logs
        GROUP BY tx_hash
    ) sr ON le.tx_hash = sr.tx_hash
    AND le.block_time = sr.evt_block_time
    {% if is_incremental() %}
        AND {{ incremental_predicate('sr.evt_block_time') }}
    {% endif %}
    INNER JOIN  {{ source('erc20_' ~ blockchain, 'evt_Transfer') }} te ON le.contract_address = te.to
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