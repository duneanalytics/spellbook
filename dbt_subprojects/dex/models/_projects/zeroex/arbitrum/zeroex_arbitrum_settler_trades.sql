{{  config(
    schema = 'zeroex_arbitrum',
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
        AND blockchain = 'arbitrum'
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
        MAX(varbinary_substring(tracker,1,12)) AS zid,
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
            settler_address,
            varbinary_substring(input,1,4) AS method_id,
            varbinary_substring(input,varbinary_position(input,0xfd3ad6d4)+132,32) tracker
        FROM 
            {{ source('arbitrum', 'traces') }} AS tr
        JOIN 
            result_0x_settler_addresses a ON a.settler_address = tr.to AND a.blockchain = 'arbitrum' AND tr.block_time > a.begin_block_time
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

tbl_all_logs AS (
    SELECT  
        logs.tx_hash, 
        logs.block_time, 
        logs.block_number,
        ROW_NUMBER() OVER (PARTITION BY logs.tx_hash ORDER BY index) rn_first, 
        index,
        CASE
            WHEN varbinary_substring(logs.topic2, 13, 20) = logs.tx_from THEN 1
            WHEN first_value(logs.topic1) over (partition by logs.tx_hash order by index) = logs.topic2 THEN 1 
            ELSE 0 
        END maker_tkn,
        bytearray_to_int256(bytearray_substring(DATA, 22,11)) value,
        logs.contract_address AS token, 
        zid, 
        st.contract_address,
        method_id, 
        tag
    FROM 
        {{ source('arbitrum', 'logs') }} AS logs
    JOIN 
        settler_txs st ON st.tx_hash = logs.tx_hash AND logs.block_time = st.block_time AND st.block_number = logs.block_number
    WHERE 
        topic0 IN (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65,
        0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
        0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
        and  not(tag = 0x000000 and zid = 0xa00000000000000000000000)
        {% if is_incremental() %}
            AND {{ incremental_predicate('logs.block_time') }}
        {% else %}
            AND logs.block_time >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
),

tbl_maker_token AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY index DESC) rn_last, 
        token AS maker_token, 
        tx_hash, 
        block_time, 
        block_number, 
        index
    FROM 
        tbl_all_logs
    WHERE 
        maker_tkn = 1
),

tbl_trades AS (
    SELECT
        ta.tx_hash, 
        ta.block_time, 
        ta.block_number,
        zid,
        method_id,
        tag, 
        contract_address,
        SUM(value) FILTER (WHERE rn_first = 1) AS taker_amount,
        MAX(token) FILTER (WHERE rn_first = 1) AS taker_token,
        SUM(value) FILTER (WHERE rn_last = 1) AS maker_amount,
        MAX(maker_token) FILTER (WHERE rn_last = 1) AS maker_token
    FROM 
        tbl_all_logs ta
    LEFT JOIN 
        tbl_maker_token mkr ON ta.tx_hash = mkr.tx_hash AND ta.block_time = mkr.block_time AND ta.block_number = mkr.block_number AND ta.index = mkr.index AND mkr.rn_last = 1
    GROUP BY 
        1,2,3,4,5,6,7
),

tokens AS (
    SELECT DISTINCT 
        te.* 
    FROM 
        {{ source('tokens', 'erc20') }} AS te
    JOIN 
        tbl_trades ON te.contract_address = taker_token OR te.contract_address = maker_token
    WHERE 
        te.blockchain = 'arbitrum'
),

prices AS (
    SELECT DISTINCT 
        pu.* 
    FROM 
        {{ source('prices', 'usd') }} AS  pu
    JOIN 
        tbl_trades ON (pu.contract_address = taker_token  OR pu.contract_address = maker_token) AND date_trunc('minute',block_time) = minute
    WHERE 
        pu.blockchain = 'arbitrum'
        {% if is_incremental() %}
            AND {{ incremental_predicate('minute') }}
        {% else %}
            AND minute >= DATE '{{zeroex_settler_start_date}}'
        {% endif %}
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
        index AS tx_index,
        CASE
            WHEN varbinary_substring(data,17,10) != 0x00000000000000000000 AND varbinary_substring(data,17,1) != 0x  THEN varbinary_substring(data,17,20)
            WHEN varbinary_substring(data,177,10) != 0x00000000000000000000  THEN varbinary_substring(data,177,20)
            WHEN varbinary_substring(data,277,10) != 0x00000000000000000000  THEN varbinary_substring(data,277,20)
            WHEN varbinary_substring(data,629,10) != 0x00000000000000000000  THEN varbinary_substring(data,629,20)
            WHEN varbinary_substring(data,693,10) != 0x00000000000000000000  THEN varbinary_substring(data,693,20)
            WHEN varbinary_substring(data,917,10) != 0x00000000000000000000  THEN varbinary_substring(data,917,20)
            WHEN varbinary_substring(data,949,10) != 0x00000000000000000000  THEN varbinary_substring(data,949,20)
            WHEN varbinary_substring(data,981,10) != 0x00000000000000000000  THEN varbinary_substring(data,981,20)
            WHEN varbinary_substring(data,1013,10) != 0x00000000000000000000  THEN varbinary_substring(data,1013,20)
            WHEN varbinary_substring(data,1141,10) != 0x00000000000000000000  THEN varbinary_substring(data,1141,20)
            WHEN varbinary_substring(data,1273,10) != 0x00000000000000000000  THEN varbinary_substring(data,1273,20)
            WHEN varbinary_substring(data,1749,4) != 0x00000000  THEN varbinary_substring(data,1749,20)
            WHEN varbinary_substring(data,1049,4) != 0x00000000  THEN varbinary_substring(data,1049,20)
            WHEN varbinary_substring(data,17,4) != 0x00000000  THEN varbinary_substring(data,17,20)
        END AS taker ,
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
        data,
        varbinary_substring(data, varbinary_length(data) -  CASE
            WHEN varbinary_position (data,0xc4103b48be) <> 0 THEN varbinary_position(REVERSE(data), REVERSE(0xc4103b48be))
            WHEN varbinary_position (data,0xe48d68a156) <> 0 THEN varbinary_position(REVERSE(data), REVERSE(0xe48d68a156))
            WHEN varbinary_position (data,0xe422ce6ede) <> 0 THEN varbinary_position(REVERSE(data), REVERSE(0xe422ce6ede))
        END -3, 37)  taker_indicator_string
    FROM 
        tbl_trades trades
    JOIN 
        {{ source('arbitrum', 'transactions') }} tr ON tr.hash = trades.tx_hash AND tr.block_time = trades.block_time AND tr.block_number = trades.block_number
    LEFT JOIN 
        tokens tt ON tt.blockchain = 'arbitrum' AND tt.contract_address = taker_token
    LEFT JOIN 
        tokens tm ON tm.blockchain = 'arbitrum' AND tm.contract_address = maker_token
    LEFT JOIN 
        prices pt ON pt.blockchain = 'arbitrum' AND pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN 
        prices pm ON pm.blockchain = 'arbitrum' AND pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
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
        'arbitrum' AS blockchain,
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
        CASE WHEN maker_token IN (0x82af49447d8a07e3bd95bd0d56f35241523fbab1,
                                0xaf88d065e77c8cc2239327c5edb3a432268e5831,
                                0xff970a61a04b1ca14834a43f5de4533ebddb5cc8,
                                0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9,
                                0x912ce59144191c1204e64559fe8253a0e49e6548) AND  maker_amount IS NOT NULL
            THEN maker_amount
            WHEN taker_token IN (0x82af49447d8a07e3bd95bd0d56f35241523fbab1,
                                0xaf88d065e77c8cc2239327c5edb3a432268e5831,
                                0xff970a61a04b1ca14834a43f5de4533ebddb5cc8,
                                0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9,
                                0x912ce59144191c1204e64559fe8253a0e49e6548)  AND taker_amount IS NOT NULL
            THEN taker_amount
            ELSE COALESCE(maker_amount, taker_amount)
            END AS volume_usd,
        taker_token,
        maker_token,
        CASE WHEN (varbinary_substring(taker,1,4) = 0x00000000)
                OR taker IS NULL
                OR taker = taker_token
                OR taker = contract_address
                OR taker = 0xdef1c0ded9bec7f1a1670819833240f027b25eff
                OR varbinary_substring(taker_indicator_string, 18,20) != contract_address
            THEN varbinary_substring(taker_indicator_string, 18,20) ELSE taker END AS taker,
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
        -1 AS fills_within,
        contract_address
    FROM 
        results
)
 
SELECT DISTINCT 
    * 
FROM 
    results_usd
ORDER BY 
    block_time DESC