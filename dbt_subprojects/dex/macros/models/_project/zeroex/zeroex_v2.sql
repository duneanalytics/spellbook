{% macro zeroex_v2_trades(blockchain, start_date) %}
-- Use the materialized all_logs table instead of creating base CTEs
WITH swap_logs as (
    select  
        block_time, 
        block_number, 
        tx_hash, 
        contract_address, 
        topic1, 
        topic2, 
        tx_from as tx_from_, 
        index,
        bytearray_substring(topic2,13,20) as taker_, 
        data,
        rn,
        varbinary_to_uint256(varbinary_substring(data,85,12)) amount_out_
    from {{ ref('zeroex_all_logs') }}
    WHERE   
       block_time > TIMESTAMP '2024-07-15'  
       and log_type = 'swap'
       and blockchain = '{{ blockchain }}'
),

taker_logs as (
    with tbl_base as (
    select 
        logs.block_time, 
        logs.block_number, 
        logs.tx_hash, 
        logs.index,
        logs.contract_address as taker_token,
        amount as taker_amount
    from {{ ref('zeroex_all_logs') }} logs
    left join swap_logs st 
        ON st.tx_hash = logs.tx_hash 
        AND logs.block_time = st.block_time 
        AND st.block_number = logs.block_number 
        and (
             varbinary_position(st.data, (logs.data)) <> 0 
            or varbinary_position(st.data, ( cast(-1 * varbinary_to_int256(varbinary_substring(logs.data, varbinary_length(logs.data) - 31, 32)) AS VARBINARY))) <> 0 
          )
          
    where logs.block_time > TIMESTAMP '2024-07-15' 
        and cow_rn is null 
        and logs.blockchain = '{{ blockchain }}'
        AND (
                (
                topic0 in (0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef, 
                            0xe59fdd36d0d223c0c7d996db7ad796880f45e1936cb0bb7ac102e7082e031487) 
                and ( 
                        (
                            bytearray_substring(logs.topic2,13,20) in (st.contract_address, settler_address) 
                        and bytearray_substring(logs.topic1,13,20) in (bytearray_substring(st.topic1,13,20), tx_from, taker, tx_to, settler_address) 
                        )
                        or (
                            bytearray_substring(logs.topic2,13,20) = taker 
                        and taker = tx_to and bytearray_substring(logs.topic1,13,20) != st.contract_address ) 
                    )
             )
             or topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c 
                 and bytearray_substring(logs.topic1,13,20) in (tx_to, settler_address)  
        ) 
    )
    select *, 
        row_number() over (partition by tx_hash order by (index)) rn
    from tbl_base  
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
        amount as maker_amount,
        bundled_tx, 
        logs.taker as taker
        
    from {{ ref('zeroex_all_logs') }} as logs 
    left join swap_logs st
        ON st.tx_hash = logs.tx_hash 
        AND logs.block_time = st.block_time 
        AND st.block_number = logs.block_number
        
    WHERE  
        cow_rn is null 
        and amount != 0 
        and logs.blockchain = '{{ blockchain }}'
        and ( 
                ( topic0 in (0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef) 
                    and 
                        ( 
                        (
                            (
                            (bytearray_substring(logs.topic1,13,20) in (st.contract_address, settler_address)  
                        and (bytearray_substring(logs.topic2,13,20) in (bytearray_substring(st.topic2,13,20), tx_from, taker, settler_address, logs.contract_address))
                        )
                        or (bytearray_substring(logs.topic2,13,20) = taker and taker = tx_to ) 
                        or (bytearray_substring(logs.topic2,13,20) = st.contract_address 
                            and bytearray_substring(logs.topic1,13,20) = tx_to 
                            and bytearray_substring(logs.topic1,13,20) not in (bytearray_substring(st.topic1,13,20), tx_to ) 
                        )
                        
                    )
                    and (varbinary_position(st.data, varbinary_ltrim(logs.data)) <> 0 
                    or varbinary_position(st.data, ( cast(-1 * varbinary_to_int256(varbinary_substring(logs.data, varbinary_length(logs.data) - 31, 32)) AS VARBINARY))) <> 0 
                    or varbinary_to_uint256(logs.data) in (amount_out_) 
                    or POSITION(CAST(varbinary_to_uint256(logs.data) AS VARCHAR) IN CAST(amount_out_ AS VARCHAR)) > 0
                    ) 
                
                        )
                        or (bytearray_substring(logs.topic1,13,20) in (settler_address)  
                        and (bytearray_substring(logs.topic2,13,20) in (taker))
                        )
                    )
            )
            or (
                topic0 in (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65)
                and bytearray_substring(logs.topic1,13,20) in (tx_from, settler_address) 
            )
        )
        
    ),
    tbl_logs_rn as (
    select
        block_time,
        block_number,
        tx_hash,
        tbl_all.index,
        maker_token,
        tx_to, 
        tx_from,
        tx_index,
        settler_address,
        maker_amount,
        taker,
        
        case when bundled_tx = 1 then row_number() over (partition by tx_hash order by index) 
            else row_number() over (partition by tx_hash order by index desc) 
            end as rn
    from tbl_all 
    )
    select
        block_time, 
        block_number,
        tx_hash,
        rn, 
        tbl_logs_rn.*,
        tl.taker_token as taker_token,
        tl.taker_amount as taker_amount 
    from tbl_logs_rn
    join taker_logs tl using (block_time, block_number, tx_hash, rn)
       where taker_token != maker_token  

    
),

-- Create a common table expression to read the cow_protocol_ethereum.trades table and apply incremental filtering
base_cow_trades AS (
    SELECT
        *
    FROM 
        {{ source('cow_protocol_ethereum', 'trades') }}
    WHERE
        1 = 1
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_time >= DATE '{{start_date}}'
        {% endif %}
),

cow_trades as (
    with base_logs as (
        select      distinct block_time, block_number, tx_hash, settler_address, logs.contract_address, tx_from, tx_to, taker, amount as taker_amount,
                     tx_index, evt_index, buy_token_address as maker_token, atoms_bought as maker_amount, logs.contract_address as taker_token
    FROM base_cow_trades as trades
    JOIN {{ ref('zeroex_all_logs') }} as logs 
        ON logs.block_time = trades.block_time 
        AND logs.block_number = trades.block_number 
        AND logs.tx_hash = trades.tx_hash
        AND logs.blockchain = '{{ blockchain }}'
    where trades.sell_token_address = logs.contract_address and trades.atoms_sold = logs.amount
        ),
    base_logs_rn as (
    select  *, 
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
        evt_index,
        b.settler_address,
        zid,
        tag,
        b.settler_address as contract_address   
    from base_logs_rn b
    join zeroex_tx s on b.block_time = s.block_time 
        and b.tx_hash = s.tx_hash 
        and b.block_number = s.block_number 
        and cow_rn = cow_trade_rn
),

tbl_trades as (
select  block_time,
        block_number,
        tx_hash,
        case when st.taker in (0x0000000000001ff3684f28c67538d4d072c22734,
                            0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
                            0x000000000000175a8b9bC6d539B3708EEd92EA6c,
                            0x9008d19f58aabd9ed0d60971565aa8510560ab41,
                            0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae,
                            0xa1bea5fe917450041748dbbbe7e9ac57a4bbebab,
                            0x663DC15D3C1aC63ff12E45Ab68FeA3F0a883C251,
                            0x3a23f943181408eac424116af7b7790c94cb97a5,
                            0xa9048585166f4f7c4589ade19567bb538035ed36,
                            0x00000000009726632680fb29d3f7a9734e3010e2,
                            0xe74a8079ca6f8d11e8acb55edfe398647272a0dc,
                            0x0000000000000000000000000000000000000000) 
                        then tx_from 
                        else st.taker end as taker,
        maker_token,
        maker_amount,
        taker_token,
        taker_amount,
        tx_to,
        tx_from,
        maker_logs.index as evt_index,
        settler_address,
        zid,
        tag,
        settler_address as contract_address 
    from maker_logs
    join zeroex_tx st using (block_time, block_number, tx_hash, rn, settler_address) 
    union 
    select * from cow_trades 
)
select * from tbl_trades 

{% endmacro %}

{% macro zeroex_v2_trades_detail(blockchain, start_date) %}
WITH token_metadata AS (
    SELECT 
        blockchain, 
        contract_address, 
        symbol, 
        decimals 
    FROM {{ source('tokens', 'erc20') }}
    WHERE blockchain = '{{blockchain}}'
),

token_prices AS (
    SELECT
        blockchain,
        contract_address,
        minute,
        price,
        symbol,
        decimals 
    FROM {{ source('prices', 'usd') }}
    WHERE 
        blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
        {% else %}
        AND minute >= DATE '{{start_date}}'
        {% endif %}
), 

results AS (
    SELECT
        '{{blockchain}}' AS blockchain,
        trades.block_time,
        trades.block_number,
        zid,
        trades.contract_address,
        trades.tx_hash,
        tx_from,
        tx_to,
        evt_index,
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
        tag
    FROM
        zeroex_v2_trades trades
    LEFT JOIN
         token_metadata tt ON tt.contract_address = taker_token
    LEFT JOIN
        token_metadata tm ON tm.contract_address = maker_token
    LEFT JOIN
        token_prices pt ON pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN
        token_prices pm ON pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
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
        evt_index,
        (ARRAY[-1]) AS trace_address,
        'settler' AS type,
        TRUE AS swap_flag,
        contract_address

FROM results_usd
order by block_time desc
{% endmacro %}
