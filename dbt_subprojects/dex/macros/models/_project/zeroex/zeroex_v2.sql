{% macro zeroex_settler_txs_cte(blockchain, start_date) %}
WITH tbl_addresses AS (
    SELECT 
        token_id, 
        "to" AS settler_address,
        block_time AS begin_block_time, 
        block_number AS begin_block_number
    FROM 
        {{ source('nft', 'transfers') }}
    WHERE 
        contract_address = 0x00000000000004533fe15556b1e086bb1a72ceae 
        AND blockchain = '{{ blockchain }}'
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
        case when varbinary_substring(input,17,6) in (0x,0x000000000000) then "from"
            else first_value(varbinary_substring(input,17,20)) over (partition by tr.tx_hash order by trace_address desc) 
            end as taker,
        a.settler_address,
        trace_address
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
        (varbinary_substring(tracker,2,12)) AS zid,
        CASE
            WHEN method_id = 0x1fff991f THEN (varbinary_substring(tracker,12,3))
            WHEN method_id = 0xfd3ad6d4 THEN (varbinary_substring(tracker,13,3))
        END AS tag,
        taker,
        row_number() over (partition by tx_hash order by trace_address desc) rn
    FROM
        settler_trace_data
    
)

SELECT * FROM settler_txs
{% endmacro %}

{% macro zeroex_v2_trades(blockchain, start_date) %}
WITH tbl_all_logs AS (
    SELECT
        logs.tx_hash,
        logs.block_time,
        logs.block_number,
        index,
        logs.contract_address,
        topic0,
        topic1,
        topic2,
        method_id,
        tag,
        st.settler_address,
        zid,
        tx_to,
        tx_from,
        taker,
        tx_index,
        (try_cast(bytearray_to_uint256(bytearray_substring(logs.DATA, 22,11)) as int256)) as amount
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
        AND ( 
                topic0 IN (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65,
                   0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
                   0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
                OR logs.contract_address = settler_address
        )
        
        AND zid != 0xa00000000000000000000000
        
),
cow_trades as (
    with base_logs as (
     select distinct block_time, 
            block_number, 
            tx_hash, 
            settler_address, 
            logs.contract_address, 
            topic0, 
            topic1, 
            topic2, 
            tx_from, 
            tx_to, 
            index, 
            taker, 
            amount as taker_amount,
            tx_index, 
            evt_index, 
            buy_token_address as maker_token, 
            atoms_bought as maker_amount, 
            logs.contract_address as taker_token,
            settler_address as contract_address 
    FROM {{ source('cow_protocol_ethereum', 'trades') }} AS trades 
    JOIN tbl_all_logs as logs using (block_time, block_number, tx_hash)
    where trades.sell_token_address = logs.contract_address and trades.atoms_sold = logs.amount  
        AND block_time > TIMESTAMP '2024-07-15'  
    ),
    base_logs_rn as (
        select *, 
            row_number() over (partition by tx_hash order by evt_index) cow_trade_rn
        from base_logs
        )
    select 
        b.block_time,
        b.block_number,
        b.tx_hash,
        tx_from as taker,
        maker_token,
        maker_amount,
        taker_token,
        taker_amount,
        tx_to,
        tx_from,
        tx_index,
        b.settler_address,
        zid,
        tag,
        s.settler_address as contract_address 
        from base_logs_rn b
        join zeroex_tx s on rn = cow_trade_rn 
            and b.block_time = s.block_time 
            and b.tx_hash = s.tx_hash 
            and b.block_number = s.block_number 
        
),
taker_logs as (
    with tbl_base as (
    select 
        logs.block_time, 
        logs.block_number, 
        logs.tx_hash, 
        logs.index,
        logs.contract_address as taker_token,
        amount as taker_amount,
        row_number() over (partition by logs.tx_hash order by (logs.index)) rn,
        bytearray_substring(logs.topic1,13,20) as taker__
    from tbl_all_logs logs
    where taker != 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 
        and logs.block_time > TIMESTAMP '2024-07-15' 
    )
    select * from tbl_base 
    where rn = 1 
),
maker_logs as (
    with tbl_all as (
    select distinct
        logs.block_time, 
        logs.block_number, 
        logs.tx_hash, 
        logs.index,
        logs.contract_address as maker_token,
        logs.tx_to, 
        logs.tx_from,
        logs.tx_index,
        settler_address,
        zid,
        tag,
        amount as maker_amount,
        row_number() over (partition by logs.tx_hash order by logs.index desc ) rn,
        taker
    from tbl_all_logs as logs 
    join taker_logs tl on tl.tx_hash = logs.tx_hash and  bytearray_substring(logs.topic2,13,20) in (tl.taker__, tx_from)
    WHERE  topic0 in (0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef, 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65) 
    )
    select * from tbl_all 
    where rn = 1 
),
tbl_trades as (

select  block_time,
        block_number,
        tx_hash,
        case when tx_to = settler_address or taker in (0x0000000000001ff3684f28c67538d4d072c22734,
                                                    0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
                                                    0x000000000000175a8b9bC6d539B3708EEd92EA6c) then tx_from else taker end as taker,
        maker_token,
        maker_amount,
        taker_token,
        taker_amount,
        tx_to,
        tx_from,
        tx_index,
        settler_address,
        zid,
        tag,
        settler_address as contract_address 
    from taker_logs
    join maker_logs using (block_time, block_number, tx_hash)

    union 

    select * from cow_trades
)
select * from tbl_trades 

{% endmacro %}

{% macro zeroex_v2_trades_detail(blockchain, start_date) %}
WITH tokens AS (
    SELECT DISTINCT token, te.*
    FROM (
        SELECT maker_token AS token FROM zeroex_v2_trades
        UNION ALL
        SELECT taker_token FROM zeroex_v2_trades
    ) t
    JOIN {{ source('tokens', 'erc20') }} AS te ON te.contract_address = t.token
    WHERE te.blockchain = '{{blockchain}}'
),

prices AS (
    SELECT DISTINCT pu.*
    FROM {{ source('prices', 'usd') }} AS pu
    JOIN zeroex_v2_trades ON (pu.contract_address IN (taker_token, maker_token)) AND DATE_TRUNC('minute', block_time) = minute
    WHERE
        pu.blockchain = '{{blockchain}}'
        {% if is_incremental() %}
            AND {{ incremental_predicate('pu.minute') }}
        {% else %}
            AND pu.minute >= DATE '{{start_date}}'
        {% endif %}
),

fills AS (
    WITH signatures AS (
        SELECT DISTINCT signature
        FROM {{ source(blockchain, 'logs_decoded') }} l
        JOIN zeroex_v2_trades tt ON tt.tx_hash = l.tx_hash AND l.block_time = tt.block_time AND l.block_number = tt.block_number
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
    JOIN zeroex_v2_trades tt ON tt.tx_hash = l.tx_hash AND l.block_time = tt.block_time AND l.block_number = tt.block_number
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('l.block_time') }}
    {% else %}
        WHERE l.block_time >= DATE '{{start_date}}'
    {% endif %}
    GROUP BY 1,2,3
),

results AS (
    SELECT
        '{{blockchain}}' AS blockchain,
        trades.block_time,
        trades.block_number,
        zid,
        trades.contract_address,
        trades.tx_hash,
        "from" AS tx_from,
        "to" AS tx_to,
        tx_index,
        taker,
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
        tag,
        fills_within
    FROM
        zeroex_v2_trades trades
    LEFT JOIN
        fills f ON f.tx_hash = trades.tx_hash AND f.block_time = trades.block_time AND f.block_number = trades.block_number
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
        'settler' AS version,
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

