{{  config(

        schema = 'zeroex_ethereum',
        alias = 'settler_trades',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_settler_start_date = '2024-07-15' %}

with 
tbl_addresses as (
select blockchain, token_id, to as settler_address, block_time as begin_block_time, block_number as begin_block_number 

from {{ source('nft', 'transfers') }} 
where contract_address = 0x00000000000004533fe15556b1e086bb1a72ceae and blockchain = 'ethereum'
           
),

tbl_end_times as( 

select *, LEAD(begin_block_time) over (partition BY blockchain, token_id order by begin_block_time) as end_block_time,
LEAD(begin_block_number) over (partition BY blockchain, token_id order by begin_block_number) as end_block_number

 from tbl_addresses
 ),
 result_0x_settler_addresses as (

 select * from tbl_end_times
 where settler_address != 0x0000000000000000000000000000000000000000 
 ),

settler_txs AS (
SELECT      tx_hash,
            block_time as block_time,
            block_number,
            methodID,
            contract_address,
           max(varbinary_substring(tracker,1,12)) as zid,
            case when methodID = 0x1fff991f then max(varbinary_substring(tracker,14,3))
                when methodID = 0xfd3ad6d4 then max(varbinary_substring(tracker,13,3)) 
                end as tag

from (
 
  SELECT
    tr.tx_hash, block_number, block_time, "to" as contract_address,
    varbinary_substring(input,1,4) as methodID,
     varbinary_substring(input,varbinary_position(input,0xfd3ad6d4)+132,32) tracker
    
  FROM {{ source('ethereum', 'traces') }} AS tr
  join result_0x_settler_addresses a on a.settler_address = tr.to and a.blockchain = 'ethereum' 
        and tr.block_time > a.start_block_time and (tr.block_time < a.end_block_time OR a.end_block_time is null )
  WHERE (a.settler_address is not null or tr.to = 0xca11bde05977b3631167028862be2a173976ca11)
    and varbinary_substring(input,1,4) in (0x1fff991f, 0xfd3ad6d4)
    {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
            {% if not is_incremental() %}
            AND block_time >= cast('{{zeroex_settler_start_date}}' as date)
            {% endif %}  
    
  ) group by 1,2,3,4,5

),

tbl_all_logs as (
select  logs.tx_hash, logs.block_time, logs.block_number,
    row_number() over (partition by logs.tx_hash order by index) rn_first, index,
    case when first_value(logs.contract_address) over (partition by logs.tx_hash order by index) = logs.contract_address then 0 else 1 end maker_tkn, 
    bytearray_to_int256(bytearray_substring(DATA, 23,10)) value,
    logs.contract_address as token, zid, st.contract_address,
        methodID, tag
from {{ source('ethereum', 'logs') }} as logs 
join settler_txs st on st.tx_hash = logs.tx_hash and logs.block_time = st.block_time and st.block_number = logs.block_number 
where topic0 in (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65, 
    0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
    0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
    {% if is_incremental() %}
            AND {{ incremental_predicate('logs.block_time') }}
            {% endif %}
            {% if not is_incremental() %}
            AND logs.block_time >= cast('{{zeroex_settler_start_date}}' as date)
            {% endif %}
    ) ,

tbl_maker_token as (
    select row_number() over (partition by tx_hash order by index desc) rn_last, token as maker_token, tx_hash, block_time, block_number, index 
    from tbl_all_logs 
    where maker_tkn = 1 
    ), 

tbl_trades as (
select 
    ta.tx_hash, ta.block_time, ta.block_number,zid,methodID,tag, contract_address,
        
    sum(value) filter (where rn_first = 1) as taker_amount,
    max(token) filter (where rn_first = 1) as taker_token,
    sum(value) filter (where rn_last = 1) as maker_amount,
    max(maker_token) filter (where rn_last = 1) as maker_token
    
    
    from tbl_all_logs ta 
    left join tbl_maker_token mkr on ta.tx_hash = mkr.tx_hash and ta.block_time = mkr.block_time and ta.block_number = mkr.block_number and ta.index = mkr.index and mkr.rn_last = 1 
    group by 1,2,3,4,5,6,7
   
    ) ,
tokens as (  
        select distinct te.* from {{ source('tokens', 'erc20') }} as te 
           join tbl_trades on te.contract_address = taker_token OR te.contract_address = maker_token
           where te.blockchain = 'ethereum'
           
        ),
prices as (  
        select distinct pu.* from {{ source('prices', 'usd') }} as  pu
           join tbl_trades on (pu.contract_address = taker_token  OR pu.contract_address = maker_token) AND date_trunc('minute',block_time) = minute
            
           where pu.blockchain = 'ethereum' 
           {% if is_incremental() %}
            AND {{ incremental_predicate('minute') }}
            {% endif %}
            {% if not is_incremental() %}
            AND minute >= cast('{{zeroex_settler_start_date}}' as date)
            {% endif %}
        ), 
results as (
    select 
        trades.block_time,
        trades.block_number,
        zid,
        trades.contract_address,
        methodID,
        trades.tx_hash,
        "from" as tx_from,
        "to" as tx_to,
        index as tx_index,
            case 
                when varbinary_substring(data,17,10) != 0x00000000000000000000 and varbinary_substring(data,17,1) != 0x  then varbinary_substring(data,17,20) 
                when varbinary_substring(data,177,10) != 0x00000000000000000000  then varbinary_substring(data,177,20) 
                when varbinary_substring(data,277,10) != 0x00000000000000000000  then varbinary_substring(data,277,20) 
                when varbinary_substring(data,629,10) != 0x00000000000000000000  then varbinary_substring(data,629,20) 
                when varbinary_substring(data,693,10) != 0x00000000000000000000  then varbinary_substring(data,693,20) 
                when varbinary_substring(data,917,10) != 0x00000000000000000000  then varbinary_substring(data,917,20) 
                when varbinary_substring(data,949,10) != 0x00000000000000000000  then varbinary_substring(data,949,20) 
                when varbinary_substring(data,981,10) != 0x00000000000000000000  then varbinary_substring(data,981,20) 
                when varbinary_substring(data,1013,10) != 0x00000000000000000000  then varbinary_substring(data,1013,20)
                when varbinary_substring(data,1141,10) != 0x00000000000000000000  then varbinary_substring(data,1141,20) 
                when varbinary_substring(data,1273,10) != 0x00000000000000000000  then varbinary_substring(data,1273,20) 
                when varbinary_substring(data,1749,4) != 0x00000000  then varbinary_substring(data,1749,20) 
                when varbinary_substring(data,1049,4) != 0x00000000  then varbinary_substring(data,1049,20)
                when varbinary_substring(data,17,4) != 0x00000000  then varbinary_substring(data,17,20)
                 
            end as taker , 
        cast(null as varbinary) as maker,
        taker_token,
        pt.price,
        coalesce(tt.symbol, pt.symbol) as taker_symbol,
        taker_amount as taker_token_amount_raw,
        taker_amount / pow(10,coalesce(tt.decimals,pt.decimals)) as taker_token_amount,
        taker_amount / pow(10,coalesce(tt.decimals,pt.decimals)) * pt.price as taker_amount,
        maker_token,
        coalesce(tm.symbol, pm.symbol)  as maker_symbol,
        maker_amount as maker_token_amount_raw,
        maker_amount / pow(10,coalesce(tm.decimals,pm.decimals)) as maker_token_amount,
        maker_amount / pow(10,coalesce(tm.decimals,pm.decimals)) * pm.price as maker_amount,
        tag,data,
        varbinary_substring(data, varbinary_length(data) -  case
            when varbinary_position (data,0xc4103b48be) <> 0 then varbinary_position(REVERSE(data), REVERSE(0xc4103b48be))
            when varbinary_position (data,0xe48d68a156) <> 0 then varbinary_position(REVERSE(data), REVERSE(0xe48d68a156))
            when varbinary_position (data,0xe422ce6ede) <> 0 then varbinary_position(REVERSE(data), REVERSE(0xe422ce6ede))
            end -3, 37)  taker_indicator_string
    from tbl_trades trades
    join {{ source('ethereum', 'transactions') }} tr on tr.hash = trades.tx_hash and tr.block_time = trades.block_time and tr.block_number = trades.block_number 
    left join tokens tt on tt.blockchain = 'ethereum' and tt.contract_address = taker_token
    left join tokens tm on tm.blockchain = 'ethereum' and tm.contract_address = maker_token
    left join prices pt on pt.blockchain = 'ethereum' and pt.contract_address = taker_token and pt.minute = date_trunc('minute', trades.block_time)
    left join prices pm on pm.blockchain = 'ethereum' and pm.contract_address = maker_token and pm.minute = date_trunc('minute', trades.block_time)
    where 1=1 {% if is_incremental() %}
            AND {{ incremental_predicate('tr.block_time') }}
            {% endif %}
            {% if not is_incremental() %}
            AND tr.block_time >= cast('{{zeroex_settler_start_date}}' as date)
            {% endif %}
), 
results_usd as (

 select  
        'ethereum' as blockchain,
        '0x API' as project,
        'settler' as version,
        date_trunc('day', block_time) block_date,
        date_trunc('month', block_time) as block_month,
        block_time,
        taker_symbol,
        maker_symbol,
        CASE WHEN lower(taker_symbol) > lower(maker_symbol) THEN concat(maker_symbol, '-', taker_symbol) ELSE concat(taker_symbol, '-', maker_symbol) END AS token_pair,
        taker_token_amount,
        maker_token_amount,
        taker_token_amount_raw,
        maker_token_amount_raw,
        CASE WHEN maker_token IN (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0xdac17f958d2ee523a2206206994597c13d831ec7,
                0x4fabb145d64652a948d72533023f6e7a623c7c53,0x6b175474e89094c44da98b954eedeac495271d0f,0xae7ab96520de3a18e5e111b5eaab095312d7fe84) AND  maker_amount IS NOT NULL
             THEN maker_amount
             WHEN taker_token IN (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0xdac17f958d2ee523a2206206994597c13d831ec7,
                0x4fabb145d64652a948d72533023f6e7a623c7c53,0x6b175474e89094c44da98b954eedeac495271d0f,0xae7ab96520de3a18e5e111b5eaab095312d7fe84)  AND taker_amount IS NOT NULL
             THEN taker_amount
             ELSE COALESCE(maker_amount, taker_amount)
             END AS volume_usd,
        taker_token,
        maker_token,
        case when (varbinary_substring(taker,1,4) = 0x00000000) or taker is null or taker = taker_token 
                or taker = contract_address or taker = 0xdef1c0ded9bec7f1a1670819833240f027b25eff or varbinary_substring(taker_indicator_string, 18,20) != contract_address 
            then varbinary_substring(taker_indicator_string, 18,20) else taker end as taker,
        maker,
        tag,
        tx_hash,
        tx_from,
        tx_to,
        tx_index as evt_index,
        (array[-1]) as trace_address,
        'settler' as type,
        true as swap_flag,
        -1 as fills_within, 
        contract_address
   
    from results 
)
select distinct * from results_usd 
    order by block_time desc 