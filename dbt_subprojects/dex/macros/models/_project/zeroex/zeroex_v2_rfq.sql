{% macro settler_txs_cte(blockchain, start_date) %}
WITH tbl_addresses AS (
    SELECT 
        token_id, 
        to AS settler_address, 
        block_time AS begin_block_time, 
        block_number AS begin_block_number
    FROM 
        {{ source('nft', 'transfers') }}
    WHERE 
        contract_address = 0x00000000000004533fe15556b1e086bb1a72ceae 
        AND blockchain = '{{ blockchain }}'
        and block_time >= cast('{{ start_date }}' as date)
),

tbl_end_times AS (
    SELECT 
        *, 
        LEAD(begin_block_time) OVER (partition by token_id ORDER BY begin_block_time) AS end_block_time,
        LEAD(begin_block_number) OVER (partition by token_id ORDER BY begin_block_number) AS end_block_number
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
            {{ source(blockchain, 'traces') }} AS tr
        JOIN 
            result_0x_settler_addresses a ON a.settler_address = tr.to AND tr.block_time > a.begin_block_time
        WHERE 
            (a.settler_address IS NOT NULL OR tr.to = 0xca11bde05977b3631167028862be2a173976ca11)
            AND varbinary_substring(input,1,4) IN (0x1fff991f, 0xfd3ad6d4)
            {% if is_incremental() %}
                AND {{ incremental_predicate('block_time') }}
            {% else %}
                AND block_time >= DATE '{{start_date}}'
            {% endif %}
    ) 
    GROUP BY 
        1,2,3,4,5,6
) 

select * from settler_txs
{% endmacro %}


{% macro zeroex_rfq_events(blockchain, start_date) %}

with tbl_trades_pre as (
    with tbl_all_logs as (
      SELECT  
        logs.tx_hash, 
        logs.block_time, 
        logs.block_number,
        index, 
        case when ( 
                   logs.contract_address = st.settler_address 
                    ) 
                then 1 end as valid,
        coalesce(bytearray_substring(logs.topic2,13,20), first_value(bytearray_substring(logs.topic1,13,20)) over (partition by logs.tx_hash order by index) ) as taker,
        logs.contract_address as maker_token_temp,  
        first_value(logs.contract_address) over (partition by logs.tx_hash order by index) as taker_token, 
        first_value(try_cast(bytearray_to_uint256(bytearray_substring(DATA, 22,11)) as int256) ) over (partition by logs.tx_hash order by index) as taker_amount, 
        try_cast(bytearray_to_uint256(bytearray_substring(DATA, 22,11)) as int256) as maker_amount_temp, 
        method_id, 
        tag,  
        st.settler_address, 
        zid,
        st.settler_address as contract_address,
        logs.tx_to, 
        logs.tx_from, 
        bytearray_substring(logs.topic1,13,20)
        
    FROM 
        {{ source(blockchain, 'logs') }} as logs
    JOIN 
        zeroex_tx st ON st.tx_hash = logs.tx_hash 
            AND logs.block_time = st.block_time 
            AND st.block_number = logs.block_number
            AND (logs.contract_address = st.settler_address 
                or bytearray_substring(logs.topic1,13,20) = st.settler_address 
                or bytearray_substring(logs.topic2,13,20) = st.settler_address 
            )
    WHERE 1=1
            {% if is_incremental() %}
              and   {{ incremental_predicate('logs.block_time') }}
            {% else %}
              and   logs.block_time >= DATE '{{start_date}}'
            {% endif %}
    
  
    ),
    tbl_valid_logs as (
        select * 
            ,  row_number() over (partition by tx_hash order by valid, index desc) rn
            , case when valid = 1 then lag(maker_amount_temp) over (partition by tx_hash order by index) end as  maker_amount
            , case when valid = 1 then lag(maker_token_temp) over (partition by tx_hash order by index) end as  maker_token
        from tbl_all_logs 
       
    )
    select * from tbl_valid_logs
        WHERE index IN (
             SELECT index - 1 FROM tbl_valid_logs WHERE valid = 1
            UNION
            SELECT index FROM tbl_valid_logs WHERE valid = 1
            UNION
            SELECT index + 1 FROM tbl_valid_logs WHERE valid = 1
            )
       and rn = 1
), 

tokens as (
    with token_list as (
        select distinct maker_token as token
        from tbl_trades_pre 
        union distinct 
        select distinct taker_token as token 
        from tbl_trades_pre 
        ) 
        select * 
        from token_list tl 
        join {{ source( 'tokens', 'erc20') }} as te ON te.contract_address = tl.token
        WHERE 
            te.blockchain = '{{blockchain}}'
), 

prices AS (
    SELECT DISTINCT 
        pu.* 
    FROM 
         {{ source( 'prices', 'usd') }} as pu 
    JOIN 
        tbl_trades_pre ON (pu.contract_address = taker_token  OR pu.contract_address = maker_token) AND date_trunc('minute',block_time) = minute
    WHERE 
        pu.blockchain = '{{blockchain}}'
        {% if is_incremental() %}
            and  {{ incremental_predicate('pu.minute') }}
        {% else %}
            and  pu.minute >= DATE '{{start_date}}'
        {% endif %}
       
),

tbl_trades as (

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
        tag
    FROM 
        tbl_trades_pre trades
    JOIN 
        {{ source(blockchain, 'transactions') }} tr ON tr.hash = trades.tx_hash AND tr.block_time = trades.block_time AND tr.block_number = trades.block_number
        
    LEFT JOIN 
        tokens tt ON tt.blockchain = '{{blockchain}}' AND tt.contract_address = taker_token
    LEFT JOIN 
        tokens tm ON tm.blockchain = '{{blockchain}}' AND tm.contract_address = maker_token
    LEFT JOIN 
        prices pt ON pt.blockchain = '{{blockchain}}' AND pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN 
        prices pm ON pm.blockchain = '{{blockchain}}' AND pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
    WHERE 
            {% if is_incremental() %}
                 {{ incremental_predicate('tr.block_time') }}
            {% else %}
                 tr.block_time >= DATE '{{start_date}}'
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
        trades.tx_index AS tx_index,
        case when varbinary_substring(tr.data,1,4) = 0x500c22bc then "from" else taker end as taker,
        CAST(NULL AS varbinary) AS maker,
        taker_token,
        taker_token as token_sold_address,
        pt.price,
        COALESCE(tt.symbol, pt.symbol) AS taker_symbol,
        taker_amount AS taker_token_amount_raw,
        taker_amount as token_sold_amount,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) AS taker_token_amount,
        taker_amount / POW(10,COALESCE(tt.decimals,pt.decimals)) * pt.price AS taker_amount,
        maker_token,
        maker_token as token_bought_address,
        COALESCE(tm.symbol, pm.symbol)  AS maker_symbol,
        maker_amount AS maker_token_amount_raw,
        maker_amount as token_bought_amount,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) AS maker_token_amount,
        maker_amount / POW(10,COALESCE(tm.decimals,pm.decimals)) * pm.price AS maker_amount,
        tag
    FROM 
        tbl_trades trades
    JOIN 
         {{ source(blockchain, 'transactions') }} tr ON tr.hash = trades.tx_hash AND tr.block_time = trades.block_time AND tr.block_number = trades.block_number
    
    LEFT JOIN 
        tokens tt ON tt.blockchain = '{{blockchain}}' AND tt.contract_address = taker_token
    LEFT JOIN 
        tokens tm ON tm.blockchain = '{{blockchain}}' AND tm.contract_address = maker_token
    LEFT JOIN 
        prices pt ON pt.blockchain = '{{blockchain}}' AND pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN 
        prices pm ON pm.blockchain = '{{blockchain}}' AND pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
    WHERE 
        1=1 
        
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