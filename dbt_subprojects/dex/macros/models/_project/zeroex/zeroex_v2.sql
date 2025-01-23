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
        taker
    FROM
        settler_trace_data
    
)

SELECT * FROM settler_txs
{% endmacro %}

{% macro zeroex_v2_trades(blockchain, start_date) %}
WITH 
swap_signatures as (
    select 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 as signature   
    union select 0x66251e495e6e69e208ab08e2bc259dbe2ef482a8c4a93b8984b03a1eb27e1b9e as signature 
    union select 0xdde2f3711ab09cdddcfee16ca03e54d21fb8cf3fa647b9797913c950d38ad693 as signature 
    union select 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67 as signature
    union select 0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83 as signature
    union select 0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b as signature
    union select 0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c as signature --tokenExchange
    union select 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140 as signature --tokenExchange
    union select 0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98 as signature --tokenExchange
    union select 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f as signature --otcOrderFilled
    union select 0x085d06ecf4c34b237767a31c0888e121d89546a77f186f1987c6b8715e1a8caa as signature --BuyGem
    union select 0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f as signature --DodoSwap
    union select 0x103ed084e94a44c8f5f6ba8e3011507c41063177e29949083c439777d8d63f60 as signature
    union select 0xa4228e1eb11eb9b31069d9ed20e7af9a010ca1a02d4855cee54e08e188fcc32c
    union select 0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f
    union select 0xdc004dbca4ef9c966218431ee5d9133d337ad018dd5b5c5493722803f75c64f7
    union select 0xa5a79273c52413fd319bf0be43c422824dc76fc4f69c671d8805d1aaf3cecc77
    union select 0x823eaf01002d7353fbcadb2ea3305cc46fa35d799cb0914846d185ac06f8ad05
    union select 0x3b841dc9ab51e3104bda4f61b41e4271192d22cd19da5ee6e292dc8e2744f713
    union select 0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db
    union select 0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499
    union select 0x176648f1f11cda284c124490086be42a926ddf0ae887ebe7b1d6b337d8942756
    union select 0x298c349c742327269dc8de6ad66687767310c948ea309df826f5bd103e19d207
    union select 0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062
    union select 0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b 
    union select 0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70
    union select 0x54787c404bb33c88e86f4baf88183a3b0141d0a848e6a9f7a13b66ae3a9b73d1
    union select 0x6ac6c02c73a1841cb185dff1fe5282ff4499ce709efd387f7fc6de10a5124320
),
tbl_all_logs AS (
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
        (try_cast(bytearray_to_uint256(bytearray_substring(logs.DATA, 21,12)) as int256)) as amount, 
        case when topic0 = signature or logs.contract_address = settler_address then 'swap' end as log_type,
        data  
    FROM
        {{ source(blockchain, 'logs') }} AS logs
    JOIN
        zeroex_tx st ON st.tx_hash = logs.tx_hash
            AND logs.block_time = st.block_time
            AND st.block_number = logs.block_number
    LEFT JOIN swap_signatures on topic0 = signature
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
                OR swap_signatures.signature is not null
        )
        
        AND zid != 0xa00000000000000000000000
        
),
swap_logs as (
    select distinct 
        block_time, 
        block_number, 
        tx_hash, 
        contract_address, 
        topic1, 
        topic2, 
        tx_from as tx_from_, 
        index,
        bytearray_substring(st.topic2,13,20) as taker_, 
        data,
        row_number() over (partition by tx_hash order by index) rn
    from tbl_all_logs st 
    WHERE   
       block_time > TIMESTAMP '2024-07-15'  
       and log_type = 'swap'
       and ( settler_address in (bytearray_substring(st.topic2,13,20), bytearray_substring(st.topic1,13,20) )
           or varbinary_position(data, settler_address) <> 0 ) 
),
valid_logs as (
    select al.* 
    from tbl_all_logs al
    join swap_logs sl on al.tx_hash = sl.tx_hash 
            and al.block_time = sl.block_time 
            and al.rn in (sl.rn-1, sl.rn-2)
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
    from valid_logs logs
    left join swap_logs st 
        ON st.tx_hash = logs.tx_hash 
        AND logs.block_time = st.block_time 
        AND st.block_number = logs.block_number 
    where logs.block_time > TIMESTAMP '2024-07-15'
        AND (
                (
                topic0 in (0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef) 
                AND ( 
                        (
                            bytearray_substring(logs.topic2,13,20) = st.contract_address 
                         OR bytearray_substring(logs.topic1,13,20) = settler_address
                         )
                        OR ( 
                            bytearray_substring(logs.topic2,13,20) = tx_to
                        AND bytearray_substring(logs.topic1,13,20) = 0x970aFf41E00833A322e1953aCe6E5Cb6A3D4ac3F  
                        )   
                 )
             )
          )  
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
        bytearray_substring(logs.topic2,13,20) as taker 
    from valid_logs as logs 
    join swap_logs st
        ON st.tx_hash = logs.tx_hash 
        AND logs.block_time = st.block_time 
        AND st.block_number = logs.block_number
        and (varbinary_position(st.data,bytearray_substring(logs.topic2,13,20)) <> 0 
            or bytearray_substring(logs.topic1,13,20) = st.contract_address) 
    WHERE  topic0 in (0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef) 
    )
    select * from tbl_all 
    where rn = 1 
),
tbl_trades as (

select  block_time,
        block_number,
        tx_hash,
        case when taker in (0x0000000000001ff3684f28c67538d4d072c22734,
                            0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
                            0x000000000000175a8b9bC6d539B3708EEd92EA6c,
                            0x9008d19f58aabd9ed0d60971565aa8510560ab41) 
                        then tx_from 
                        else taker end as taker,
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
    from taker_logs
    join maker_logs using (block_time, block_number, tx_hash)

)
select * from tbl_trades 

{% endmacro %}

{% macro zeroex_v2_trades_detail(blockchain, start_date) %}
WITH fills AS (
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
        tag,
        fills_within
    FROM
        zeroex_v2_trades trades
    LEFT JOIN
        fills f ON f.tx_hash = trades.tx_hash AND f.block_time = trades.block_time AND f.block_number = trades.block_number
    LEFT JOIN
        {{ source('tokens', 'erc20') }} tt ON tt.blockchain = '{{blockchain}}' AND tt.contract_address = taker_token
    LEFT JOIN
        {{ source('tokens', 'erc20') }} tm ON tm.blockchain = '{{blockchain}}' AND tm.contract_address = maker_token
    LEFT JOIN
        {{ source('prices', 'usd') }} pt ON pt.blockchain = '{{blockchain}}' AND pt.contract_address = taker_token AND pt.minute = DATE_TRUNC('minute', trades.block_time)
    LEFT JOIN
        {{ source('prices', 'usd') }} pm ON pm.blockchain = '{{blockchain}}' AND pm.contract_address = maker_token AND pm.minute = DATE_TRUNC('minute', trades.block_time)
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

