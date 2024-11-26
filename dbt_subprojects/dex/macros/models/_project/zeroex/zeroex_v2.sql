{% macro zeroex_settler_txs_cte(blockchain, start_date) %}
WITH tbl_addresses AS (
    SELECT 
        varbinary_to_int256 (topic1) as token_id, 
        bytearray_substring(logs.topic3,13,20) as settler_address,
        block_time AS begin_block_time, 
        block_number AS begin_block_number
    FROM 
        {{ source( blockchain, 'logs') }}
    WHERE 
        contract_address = 0x00000000000004533fe15556b1e086bb1a72ceae 
        and topic0 = 0xaa94c583a45742b26ac5274d230aea34ab334ed5722264aa5673010e612bc0b2
),

tbl_end_times AS (
    SELECT 
        *, 
        LEAD(begin_block_time) OVER (PARTITION BY token_id ORDER BY begin_block_time) AS end_block_time,
        LEAD(begin_block_number) OVER (PARTITION BY token_id ORDER BY begin_block_time) AS end_block_number
    FROM
        tbl_addresses
),

result_0x_settler_addresses AS (
    SELECT
        *
    FROM
        tbl_end_times
    WHERE
        settler_address != 0x0000000000000000000000000000000000000000
),

settler_trace_data AS (
    SELECT
        tr.tx_hash,
        block_number,
        block_time,
        "to" AS contract_address,
        varbinary_substring(input,1,4) AS method_id,
        varbinary_substring(input,varbinary_position(input,0xfd3ad6d4)+132,32) tracker,
        a.settler_address
    FROM
        {{ source(blockchain, 'traces') }} AS tr
    JOIN
        result_0x_settler_addresses a ON a.settler_address = tr.to AND tr.block_time > a.begin_block_time
    WHERE
        (a.settler_address IS NOT NULL OR tr.to in (0x0000000000001fF3684f28c67538d4D072C22734,0x0000000000005E88410CcDFaDe4a5EfaE4b49562,0x000000000000175a8b9bC6d539B3708EEd92EA6c))
        AND (varbinary_position(input,0x1fff991f) <> 0 OR  varbinary_position(input,0xfd3ad6d4) <> 0 )
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% else %}
            AND block_time >= DATE '{{start_date}}'
        {% endif %}
),

settler_txs AS (
    SELECT
        tx_hash,
        block_time,
        block_number,
        method_id,
        contract_address,
        settler_address,
        MAX(varbinary_substring(tracker,2,12)) AS zid,
        CASE
            WHEN method_id = 0x1fff991f THEN MAX(varbinary_substring(tracker,14,3))
            WHEN method_id = 0xfd3ad6d4 THEN MAX(varbinary_substring(tracker,13,3))
        END AS tag
    FROM
        settler_trace_data
    GROUP BY
        1,2,3,4,5,6
)

SELECT * FROM settler_txs
{% endmacro %}

{% macro zeroex_v2_trades(blockchain, start_date, is_direct=true) %}
WITH tbl_all_logs AS (
    SELECT
        logs.tx_hash,
        logs.block_time,
        logs.block_number,
        index,
        {% if is_direct %} tx_from 
            {% else %} case when (varbinary_substring(logs.topic1, 13, 20) in (tx_from)) then varbinary_substring(logs.topic1, 13, 20) end
        {% endif %}
         as taker,
        case when (varbinary_substring(logs.topic1, 13, 20) in (tx_from, settler_address)) then logs.contract_address end as taker_token_, 
        case when (varbinary_substring(logs.topic2, 13, 20) in (settler_address, tx_from)) then logs.contract_address end as maker_token_,
        first_value(try_cast(bytearray_to_uint256(bytearray_substring(DATA, 22,11)) AS int256)) OVER (PARTITION BY logs.tx_hash ORDER BY index) AS taker_amount,
        try_cast(bytearray_to_uint256(bytearray_substring(DATA, 22,11)) AS int256) AS maker_amount,
        method_id,
        tag,
        st.settler_address,
        zid,
        st.settler_address AS contract_address,
        topic1,
        topic2,
        tx_to
    FROM
        {{ source(blockchain, 'logs') }} AS logs
    JOIN
        zeroex_tx st ON st.tx_hash = logs.tx_hash
            AND logs.block_time = st.block_time
            AND st.block_number = logs.block_number
    WHERE 1=1
        {% if is_incremental() %}
            AND {{ incremental_predicate('logs.block_time') }}
        {% else %}
            AND logs.block_time >= DATE '{{start_date}}'
        {% endif %}
        AND topic0 IN (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65,
                   0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
                   0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
        AND zid != 0xa00000000000000000000000

        {% if is_direct %}
            AND (logs.tx_to = settler_address)
            AND (tx_from in (bytearray_substring(logs.topic2,13,20), bytearray_substring(logs.topic1,13,20))
                OR tx_to in (bytearray_substring(logs.topic2,13,20), bytearray_substring(logs.topic1,13,20))
                )
        {% endif %}
        {% if not is_direct %}
            AND logs.tx_to != settler_address
            and ( settler_address in (bytearray_substring(logs.topic2,13,20), bytearray_substring(logs.topic1,13,20))
            OR tx_from in (bytearray_substring(logs.topic2,13,20), bytearray_substring(logs.topic1,13,20))
            OR tx_to in (bytearray_substring(logs.topic2,13,20), bytearray_substring(logs.topic1,13,20))
            )
        {% endif %}
),

tbl_valid_logs AS (
    SELECT
        *
        ,LAST_VALUE(maker_token_) IGNORE NULLS OVER (PARTITION BY tx_hash ORDER BY index
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS maker_token
        ,FIRST_VALUE(taker_token_) IGNORE NULLS OVER (PARTITION BY tx_hash ORDER BY index
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS taker_token
        ,ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY index DESC) AS rn
    FROM
        tbl_all_logs
    WHERE
        taker_token != maker_token
)

SELECT * FROM tbl_valid_logs
WHERE rn = 1
{% endmacro %}

{% macro zeroex_v2_trades_detail(blockchain, start_date) %}
WITH tokens AS (
    SELECT DISTINCT token, te.*
    FROM (
        SELECT maker_token AS token FROM tbl_trades
        UNION ALL
        SELECT taker_token FROM tbl_trades
    ) t
    JOIN {{ source('tokens', 'erc20') }} AS te ON te.contract_address = t.token
    WHERE te.blockchain = '{{blockchain}}'
),

prices AS (
    SELECT DISTINCT pu.*
    FROM {{ source('prices', 'usd') }} AS pu
    JOIN tbl_trades ON (pu.contract_address IN (taker_token, maker_token)) AND DATE_TRUNC('minute', block_time) = minute
    WHERE
        pu.blockchain = '{{blockchain}}'
        {% if is_incremental() %}
            AND {{ incremental_predicate('pu.minute') }}
        {% else %}
            AND pu.minute >= DATE '{{start_date}}'
        {% endif %}
),

results AS (
    SELECT
        '{{blockchain}}' AS blockchain,
        trades.block_time,
        trades.block_number,
        zid,
        trades.contract_address,
        method_id,
        trades.tx_hash,
        "from" AS tx_from,
        "to" AS tx_to,
        trades.index AS tx_index,
        CASE WHEN varbinary_substring(tr.data,1,4) = 0x500c22bc THEN "from" ELSE taker END AS taker,
        CAST(NULL AS varbinary) AS maker,
        taker_token,
        taker_token AS token_sold_address,
        pt.price,
        COALESCE(tt.symbol, pt.symbol) AS taker_symbol,
        taker_amount AS taker_token_amount_raw,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) AS taker_token_amount,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) AS token_sold_amount,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) * pt.price AS taker_amount,
        maker_token,
        maker_token AS token_bought_address,
        COALESCE(tm.symbol, pm.symbol) AS maker_symbol,
        maker_amount AS maker_token_amount_raw,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) AS maker_token_amount,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) AS token_bought_amount,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) * pm.price AS maker_amount,
        tag
    FROM
        tbl_trades trades
    JOIN
        {{ source(blockchain, 'transactions') }} tr ON tr.hash = trades.tx_hash AND tr.block_time = trades.block_time AND tr.block_number = trades.block_number
        {% if is_incremental() %}
            AND {{ incremental_predicate('tr.block_time') }}
        {% else %}
            AND tr.block_time >= DATE '{{start_date}}'
        {% endif %}
    LEFT JOIN
        tokens tt ON tt.blockchain = '{{blockchain}}' AND tt.contract_address = taker_token
    LEFT JOIN
        tokens tm ON tm.blockchain = '{{blockchain}}' AND tm.contract_address = maker_token
    LEFT JOIN
        prices pt ON pt.blockchain = '{{blockchain}}' AND pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN
        prices pm ON pm.blockchain = '{{blockchain}}' AND pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
),

results_usd AS (
    {{
        add_amount_usd(
            trades_cte = 'results'
        )
    }}
)

SELECT
        '{{blockchain}}' AS blockchain,
        '0x-API' AS project,
        'v2' AS version,
        DATE_TRUNC('day', block_time) block_date,
        DATE_TRUNC('month', block_time) AS block_month,
        block_time,
        block_number,
        taker_symbol,
        maker_symbol,
        CASE WHEN LOWER(taker_symbol) > LOWER(maker_symbol) THEN CONCAT(maker_symbol, '-', taker_symbol) ELSE CONCAT(taker_symbol, '-', maker_symbol) END AS token_pair,
        taker_token_amount,
        maker_token_amount,
        taker_token_amount_raw,
        maker_token_amount_raw,
        amount_usd as volume_usd,
        taker_token,
        maker_token,
        taker,
        maker,
        tag,
        zid,
        tx_hash,
        tx_from,
        tx_to,
        tx_index AS evt_index,
        (ARRAY[-1]) AS trace_address,
        'settler' AS type,
        TRUE AS swap_flag,
        contract_address

FROM results_usd
order by block_time desc
{% endmacro %}

{% macro zeroex_v2_trades_direct(blockchain, start_date) %}
{{ zeroex_v2_trades(blockchain, start_date, true) }}
{% endmacro %}

{% macro zeroex_v2_trades_indirect(blockchain, start_date) %}
{{ zeroex_v2_trades(blockchain, start_date, false) }}
{% endmacro %}

{% macro zeroex_v2_trades_fills_count(blockchain, start_date) %}
    WITH signatures AS (
        SELECT DISTINCT signature
        FROM {{ source(blockchain, 'logs_decoded') }} l
        JOIN tbl_trades tt ON tt.tx_hash = l.tx_hash AND l.block_time = tt.block_time AND l.block_number = tt.block_number
        WHERE event_name IN ('TokenExchange', 'OtcOrderFilled', 'SellBaseToken', 'Swap', 'BuyGem', 'DODOSwap', 'SellGem', 'Submitted')
        {% if is_incremental() %}
            AND {{ incremental_predicate('l.block_time') }}
        {% else %}
            AND l.block_time >= DATE '{{start_date}}'
        {% endif %}
    )
    SELECT tt.tx_hash, tt.block_number, tt.block_time, COUNT(*) AS fills_within
    FROM {{ source(blockchain, 'logs') }} l
    JOIN signatures ON signature = topic0
    JOIN tbl_trades tt ON tt.tx_hash = l.tx_hash AND l.block_time = tt.block_time AND l.block_number = tt.block_number
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('l.block_time') }}
    {% else %}
        WHERE l.block_time >= DATE '{{start_date}}'
    {% endif %}
    GROUP BY 1,2,3
{% endmacro %}