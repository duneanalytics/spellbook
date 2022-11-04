EXPLAIN EXTENDED
with iv_union_all as (
    select 'arbitrum' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from arbitrum.transactions
    union all
    select 'avalanche_c' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from avalanche_c.transactions
    union all 
    select 'bnb' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from bnb.transactions
    union all
    select 'ethereum' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from ethereum.transactions
    union all
    select 'gnosis' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from gnosis.transactions
    union all
    select 'optimism' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from optimism.transactions
    union all
    select 'polygon' blockchain, block_time,value,block_number,gas_limit,gas_price,gas_used,max_fee_per_gas,max_priority_fee_per_gas,priority_fee_per_gas,nonce,index,success,`from`,`to`,block_hash,data,hash,`type`,access_list
      from polygon.transactions
)
select 'union all' as title
      ,count(1) as cnt
      ,sum(gas_price/1e18*gas_used) as gas_fee
      ,now() as query_start
  from iv_union_all
 where block_time >= '2022-01-01'
   and block_time < '2022-02-01'
   and success