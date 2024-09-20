{{  config(
    schema = 'zeroex_base',
    alias = 'settler_trades',
    materialized='incremental',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'tx_hash', 'evt_index'],
    on_schema_change='sync_all_columns',
    file_format ='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

{% set zeroex_settler_start_date = '2024-07-15' %}

WITH tbl_addresses AS (
    SELECT 
        blockchain, 
        token_id, 
        to AS settler_address, 
        block_time AS begin_block_time, 
        block_number AS begin_block_number
    FROM 
        {{ source('nft', 'transfers') }}
    WHERE 
        contract_address = 0x00000000000004533fe15556b1e086bb1a72ceae 
        AND blockchain = 'base'
        and block_time > TIMESTAMP '2024-05-23'
),

tbl_end_times AS (
    SELECT 
        *, 
        LEAD(begin_block_time) OVER (PARTITION BY blockchain, token_id ORDER BY begin_block_time) AS end_block_time,
        LEAD(begin_block_number) OVER (PARTITION BY blockchain, token_id ORDER BY begin_block_number) AS end_block_number
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
    FROM (
        SELECT
            tr.tx_hash, 
            block_number, 
            block_time, 
            "to" AS contract_address,
            varbinary_substring(input,1,4) AS method_id,
            varbinary_substring(input,varbinary_position(input,0xfd3ad6d4)+132,32) tracker,
            a.settler_address
        FROM 
            {{ source('base', 'traces') }} AS tr
        JOIN 
            result_0x_settler_addresses a ON a.settler_address = tr.to AND a.blockchain = 'base' AND tr.block_time > a.begin_block_time
        WHERE 
            (a.settler_address IS NOT NULL OR tr.to = 0xca11bde05977b3631167028862be2a173976ca11)
            AND varbinary_substring(input,1,4) IN (0x1fff991f, 0xfd3ad6d4)
            {% if is_incremental() %}
                AND {{ incremental_predicate('block_time') }}
            {% else %}
                AND block_time >= DATE '{{zeroex_settler_start_date}}'
            {% endif %}
    ) 
    GROUP BY 
        1,2,3,4,5,6
),
tbl_trades as (

with tbl_all_logs AS (
    SELECT  
        logs.tx_hash, 
        logs.block_time, 
        logs.block_number,
        index, 
        case when ( (varbinary_substring(logs.topic2, 13, 20) = logs.tx_from)  OR
                    (varbinary_substring(logs.topic2, 13, 20) = first_value(bytearray_substring(logs.topic1,13,20)) over (partition by logs.tx_hash order by index) ) or
                     topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65) 
                then 1 end as valid,
        coalesce(bytearray_substring(logs.topic2,13,20), first_value(bytearray_substring(logs.topic1,13,20)) over (partition by logs.tx_hash order by index) ) as taker,
        logs.contract_address as maker_token,  
        first_value(logs.contract_address) over (partition by logs.tx_hash order by index) as taker_token, 
        first_value(try_cast(bytearray_to_uint256(bytearray_substring(DATA, 22,11)) as int256) ) over (partition by logs.tx_hash order by index) as taker_amount, 
        try_cast(bytearray_to_uint256(bytearray_substring(DATA, 22,11)) as int256) as maker_amount, 
        method_id, 
        tag,  
        st.settler_address, 
        zid, 
        st.settler_address as contract_address 
    FROM 
        {{ source('base', 'logs') }} AS logs
    JOIN 
        settler_txs st ON st.tx_hash = logs.tx_hash 
            AND logs.block_time = st.block_time 
            AND st.block_number = logs.block_number
            AND ( (st.settler_address = bytearray_substring(logs.topic1,13,20))  
                or (st.settler_address= bytearray_substring(logs.topic2,13,20)) 
                or logs.tx_from = bytearray_substring(logs.topic1,13,20) 
                or logs.tx_from = bytearray_substring(logs.topic2,13,20) 
                 ) 
    WHERE 
        topic0 IN ( 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65,
                    0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
                    0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c ) 
        and topic1 != 0x0000000000000000000000000000000000000000000000000000000000000000      
        and zid != 0xa00000000000000000000000
        {% if is_incremental() %}
            AND {{ incremental_predicate('logs.block_time') }}
        {% else %}
            AND logs.block_time >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
    ),
    tbl_valid_logs as (
        select *,
            row_number() over (partition by tx_hash order by valid, index) rn 
        from tbl_all_logs 
        where taker_token != maker_token
    )
    select * from tbl_valid_logs where rn = 1
),

tokens AS (
    with token_list as (
        select 
            distinct maker_token as token
        from 
            tbl_trades 
        
        union distinct 
        
        select 
            distinct taker_token as token 
        from tbl_trades 
        ) 

        select * 
        from 
            token_list tl 
        join 
            {{ source('tokens', 'erc20') }} AS te ON te.contract_address = tl.token
        WHERE 
            te.blockchain = 'base'
),

prices AS (
    SELECT DISTINCT 
        pu.* 
    FROM 
        {{ source('prices', 'usd') }} AS  pu
    JOIN 
        tbl_trades ON (pu.contract_address = taker_token  OR pu.contract_address = maker_token) AND date_trunc('minute',block_time) = minute
    WHERE 
        pu.blockchain = 'base'
        {% if is_incremental() %}
            AND {{ incremental_predicate('minute') }}
        {% else %}
            AND minute >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
),

fills as (
        with signatures as (
        select distinct signature  
        from {{ source('base', 'logs_decoded') }}  l
        join tbl_trades tt on tt.tx_hash = l.tx_hash and l.block_time = tt.block_time and l.block_number = tt.block_number 
        and event_name in ('TokenExchange', 'OtcOrderFilled', 'SellbaseToken', 'Swap', 'BuyGem', 'DODOSwap', 'SellGem', 'Submitted')
        WHERE  1=1 
        {% if is_incremental() %}
            AND {{ incremental_predicate('l.block_time') }}
        {% else %}
            AND l.block_time >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
        )
        
        select tt.tx_hash, tt.block_number, tt.block_time, count(*) fills_within
        from {{ source('base', 'logs') }}  l
        join signatures on signature = topic0 
        join  tbl_trades tt on tt.tx_hash = l.tx_hash and l.block_time = tt.block_time and l.block_number = tt.block_number 
        WHERE 1=1 
        {% if is_incremental() %}
            AND {{ incremental_predicate('l.block_time') }}
        {% else %}
            AND l.block_time >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
        group by 1,2,3
        ),

results AS (
    SELECT
        trades.block_time,
        trades.block_number,
        zid,
        trades.contract_address,
        method_id,
        trades.tx_hash,
        "from" AS tx_from,
        "to" AS tx_to,
        trades.index AS tx_index,
        case when varbinary_substring(tr.data,1,4) = 0x500c22bc then "from" else taker end as taker,
        CAST(NULL AS varbinary) AS maker,
        taker_token,
        pt.price,
        COALESCE(tt.symbol, pt.symbol) AS taker_symbol,
        taker_amount AS taker_token_amount_raw,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) AS taker_token_amount,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) * pt.price AS taker_amount,
        maker_token,
        COALESCE(tm.symbol, pm.symbol)  AS maker_symbol,
        maker_amount AS maker_token_amount_raw,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) AS maker_token_amount,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) * pm.price AS maker_amount,
        tag,
        -1 as fills_within
    FROM 
        tbl_trades trades
    JOIN 
        {{ source('base', 'transactions') }} tr ON tr.hash = trades.tx_hash AND tr.block_time = trades.block_time AND tr.block_number = trades.block_number
    LEFT JOIN 
        fills f ON f.tx_hash = trades.tx_hash AND f.block_time = trades.block_time AND f.block_number = trades.block_number 
    LEFT JOIN 
        tokens tt ON tt.blockchain = 'base' AND tt.contract_address = taker_token
    LEFT JOIN 
        tokens tm ON tm.blockchain = 'base' AND tm.contract_address = maker_token
    LEFT JOIN 
        prices pt ON pt.blockchain = 'base' AND pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN 
        prices pm ON pm.blockchain = 'base' AND pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
    WHERE 
        1=1 
        {% if is_incremental() %}
            AND {{ incremental_predicate('tr.block_time') }}
        {% else %}
            AND tr.block_time >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
),

results_usd AS (
    SELECT
        'base' AS blockchain,
        '0x API' AS project,
        'settler' AS version,
        DATE_TRUNC('day', block_time) block_date,
        DATE_TRUNC('month', block_time) AS block_month,
        block_time,
        taker_symbol,
        maker_symbol,
        CASE WHEN LOWER(taker_symbol) > LOWER(maker_symbol) THEN CONCAT(maker_symbol, '-', taker_symbol) ELSE CONCAT(taker_symbol, '-', maker_symbol) END AS token_pair,
        taker_token_amount,
        maker_token_amount,
        taker_token_amount_raw,
        maker_token_amount_raw,
        CASE WHEN maker_token IN   (0x4200000000000000000000000000000000000006,
                                    0x833589fcd6edb6e08f4c7c32d4f71b54bda02913,
                                    0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,
                                    0x5d0af35b4f6f4715961b56168de93bf0062b173d,
                                    0x50c5725949a6f0c72e6c4a641f24049a917db0cb) AND  maker_amount IS NOT NULL
            THEN maker_amount
            WHEN taker_token IN   (0x4200000000000000000000000000000000000006,
                                    0x833589fcd6edb6e08f4c7c32d4f71b54bda02913,
                                    0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,
                                    0x5d0af35b4f6f4715961b56168de93bf0062b173d,
                                    0x50c5725949a6f0c72e6c4a641f24049a917db0cb)  AND taker_amount IS NOT NULL
            THEN taker_amount
            ELSE COALESCE(maker_amount, taker_amount)
            END AS volume_usd,
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
        fills_within,
        contract_address
    FROM 
        results
)

SELECT 
    * 
FROM 
    results_usd
ORDER BY 
    block_time DESC
