{% macro zeroex_v2_trades(blockchain, start_date) %}
-- Create a CTE to read the logs table and apply incremental filtering
WITH base_filtered_logs AS (
    SELECT
        *
    FROM
        {{ source(blockchain, 'logs') }} AS logs
    WHERE 1=1
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% else %}
            AND block_time >= DATE '{{start_date}}'
        {% endif %}
), 

bundled_tx_check as (
    select tx_hash, 
        block_time,
        block_number, 
        count(*) tx_cnt
        from zeroex_tx
        group by 1,2,3
), 

swap_signatures as (
    SELECT signature FROM (
        VALUES 
        (0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822),
        (0x66251e495e6e69e208ab08e2bc259dbe2ef482a8c4a93b8984b03a1eb27e1b9e),
        (0xdde2f3711ab09cdddcfee16ca03e54d21fb8cf3fa647b9797913c950d38ad693),
        (0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67),
        (0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83),
        (0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b),
        (0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c), --tokenExchange
        (0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140), --tokenExchange
        (0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98), --tokenExchange
        (0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f), --otcOrderFilled
        (0x085d06ecf4c34b237767a31c0888e121d89546a77f186f1987c6b8715e1a8caa), --BuyGem
        (0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f), --DodoSwap
        (0x103ed084e94a44c8f5f6ba8e3011507c41063177e29949083c439777d8d63f60),
        (0xa4228e1eb11eb9b31069d9ed20e7af9a010ca1a02d4855cee54e08e188fcc32c),
        (0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f),
        (0xdc004dbca4ef9c966218431ee5d9133d337ad018dd5b5c5493722803f75c64f7),
        (0xa5a79273c52413fd319bf0be43c422824dc76fc4f69c671d8805d1aaf3cecc77),
        (0x823eaf01002d7353fbcadb2ea3305cc46fa35d799cb0914846d185ac06f8ad05),
        (0x3b841dc9ab51e3104bda4f61b41e4271192d22cd19da5ee6e292dc8e2744f713),
        (0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db),
        (0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499),
        (0x176648f1f11cda284c124490086be42a926ddf0ae887ebe7b1d6b337d8942756),
        (0x298c349c742327269dc8de6ad66687767310c948ea309df826f5bd103e19d207),
        (0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062),
        (0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b),
        (0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70),
        (0x54787c404bb33c88e86f4baf88183a3b0141d0a848e6a9f7a13b66ae3a9b73d1),
        (0x6ac6c02c73a1841cb185dff1fe5282ff4499ce709efd387f7fc6de10a5124320),
        (0x1f5359759208315a45fc3fa86af1948560d8b87afdcaf1702a110ce0fbc305f3)
    ) AS t(signature)
),

tbl_all_logs AS (
    SELECT
        tx_hash,
        block_time,
        block_number,
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
        data,
        row_number() over (partition by tx_hash order by index) rn,
        cow_rn,
        case when tx_cnt > 1 then 1 else 0 end as bundled_tx
    FROM
        base_filtered_logs AS logs
    JOIN
        zeroex_tx st using (block_time, block_number, tx_hash)
    JOIN bundled_tx_check btx using (block_time, block_number, tx_hash)
    LEFT JOIN swap_signatures on topic0 = signature
    WHERE 1=1
        AND ( 
                topic0 IN (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65,
                   0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
                   0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
                OR logs.contract_address = settler_address
                OR swap_signatures.signature is not null
        )
    
        AND zid != 0xa00000000000000000000000
        and rn = 1 
),

swap_logs as (
    select  
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
        rn,
        varbinary_to_uint256(varbinary_substring(data,85,12)) amount_out_
    from tbl_all_logs st 
    WHERE   
       block_time > TIMESTAMP '2024-07-15'  
       and log_type = 'swap'
       
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
    from tbl_all_logs logs
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
        
    from tbl_all_logs as logs 
    left join swap_logs st
        ON st.tx_hash = logs.tx_hash 
        AND logs.block_time = st.block_time 
        AND st.block_number = logs.block_number
        
    WHERE  
        cow_rn is null 
        and amount != 0 
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
    JOIN tbl_all_logs as logs using (block_time, block_number, tx_hash)
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
        cast(DATE_TRUNC('month', block_time) as date) AS block_month,
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
