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



with 
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
  join dune."0xproject".result_0x_settler_addresses a on a.settler_address = tr.to
  WHERE (a.settler_address is not null or tr.to = 0xca11bde05977b3631167028862be2a173976ca11)
    and varbinary_substring(input,1,4) in (0x1fff991f, 0xfd3ad6d4)
    AND block_time > TIMESTAMP '2024-07-15'  
    
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
where  logs.block_time > TIMESTAMP '2024-07-15' 
    and topic0 in (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65, 
    0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
    0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
    ) ,

tbl_maker_token as (
    select row_number() over (partition by tx_hash order by index desc) rn_last, token as maker_token, tx_hash, block_time, block_number, index 
    from tbl_all_logs 
    filter where maker_tkn = 1 
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
           where pu.blockchain = 'ethereum' and minute > TIMESTAMP '2024-07-15'
        ), 