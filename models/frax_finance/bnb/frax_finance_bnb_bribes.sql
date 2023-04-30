with base_date as (
    SELECT
      week_array AS week_start,
      week(date_add('DAY',-4, week_array)) as week,
      date_add('DAY', 7, week_array) as week_end
    FROM
      UNNEST(
        SEQUENCE(
          cast('2023-01-05' AS date),
          current_date,
          INTERVAL '7' DAY
        )
      ) AS t1(week_array)
      )
,
pairs as 
(
select 
distinct
contract_address,
output_0 as token0,
output_1 as token1
from {{source('frax_finance_bnb','thena_fi_bnb.Pair_call_tokens')}} 
where (output_0 in (0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e) or 
output_1 in (0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e))
and date(call_block_time) = (
    select max(date(call_block_time)) from thena_fi_bnb.Pair_call_tokens
    where (output_0 in (0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e) or 
    output_1 in (0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e))
                            ) 
and call_success = true
)
,
reserves as (
select contract_address,
call_block_time,
output__reserve0/cast(pow(10,18) as uint256) as reserve0,
output__reserve1/cast(pow(10,18) as uint256) as reserve1,
row_number() over(partition by date(call_block_time),contract_address order by call_block_time desc) as rn
from {{source('frax_finance_bnb','thena_fi_bnb.Pair_call_getReserves')}} 
where date(call_block_time) >= date('2023-01-04')
and contract_address in (select distinct contract_address from pairs)
),
TVL as (
select 
 a.call_block_time as block_time
,a.contract_address 
,b.token0 
,b.token1
,case when token0 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then reserve0
 when token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then reserve1
 end as Frax_reserve
 ,case when token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then reserve0
 when token1 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then reserve1
 end as FrxETH_reserve
from reserves a
left join pairs b
on a.contract_address = b.contract_address 
where rn = 1
)
,
prices_raw as (
select *,
row_number() over(partition by date(hour),contract_address order by hour desc) as rn
from {{ref('dex_prices')}}
where blockchain = 'ethereum'
and contract_address in  (0x5e8422345238f34275888049021821e8e08caa1f,0x853d955aCEf822Db058eb8505911ED77F175b99e, 0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0)
and date(hour)>= date('2023-01-04')

),

prices as (
select 
day
,contract_address
,median_price as price
from prices_raw
where rn = 1
)
,
TVL_USD as (
select
date(block_time) as block_time,
a.contract_address,
Frax_reserve,
FrxETH_reserve,
cast(Frax_reserve as double) * b.price as Frax_TVL_USD,
cast(FrxETH_reserve as double) * c.price as FrxETH_TVL_USD
from TVL a 
left join (select day, price from prices where contract_address = 0x853d955acef822db058eb8505911ed77f175b99e) b
on date(a.block_time) = b.day
left join (select day, price from prices where contract_address = 0x5e8422345238f34275888049021821e8e08caa1f) c
on date(a.block_time) = c.day
)
,
TVL_sum as (
select date(block_time) as block_time,
sum(Frax_reserve) as Frax_reserve,
sum(FrxETH_reserve) as FrxETH_reserve,
sum(Frax_TVL_USD) as Frax_TVL,
sum(FrxETH_TVL_USD) as FrxETH_TVL
from TVL_USD
group by 1
)
,
cte as (
select * from (VALUES   
(0x486B3cbE3fac82492220f646D890cA805BE2726a, 0x8a420aaca0c92e3f97cdcfdd852e01ac5b609452, 'sAMM-ETH/frxETH' ),
(0xbA86cb779d30fe8Adcd64f15931A63f6bae261DD, 0x49ad051f4263517bd7204f75123b7c11af9fd31c, 'sAMM-MAI/FRAX'),
(0x068875E5BBe89Ab1eF0A8E62e5650107875f0C39, 0xc2d245919db52ea4d5d269b0c710056639176ead, 'sAMM-sfrxETH/frxETH'),
(0x7AF074B8312462b6FeD7a170a44fd0188FCa3ceE, 0x8d65dbe7206a768c466073af0ab6d76f9e14fc6d, 'sAMM-USDT/FRAX' ),
(0xE00680165abb4dAdEeD75fC4332BA7d2b809832A, 0x314d95096e49fde9ebb68ad7e162b6edd8d4352a, 'vAMM-BNBx/FRAX' ),
(0x2e28f2a1113e3cDE6C417114EA3da13fD5b09291, 0x0d8401cbc650e82d1f21a7461efc6409ef55c4db, 'vAMM-frxETH/FRAX'),
(0xdF6b30f954Bc8f1c798Fe5784aC8D6508Ae544de, 0x338ca7ed1d6bede03799a36a6f90e107d24dc6ad, 'sAMM-FRAX/CUSD'),
(0x6d39e2A90f55276734AABC51C978D0e66De6e822, 0xfd66a4a4c921cd7194abab38655476a06fbaea05, 'sAMM-DOLA/FRAX'),
(0x0F4150Bd732D06F5cf8dBdFAa9121B94b2aaEF7c, 0x7fcfe6b06c1f6aad14884ba24a7f315c1c0c2cef, 'sAMM-FRAX/BUSD'),
(0x970E2CDe8c2A116d3F346cE2fE60dC0f188D10c4, 0x3c9bd1f914d6f0e0cd27a1f77e120c061d1fdbed, 'vAMM-FRAX/FXS')
) as my_table (bribe_address, contract_address, contract_name)
)
,
bribe_base as (
select block_time, 
a.contract_address as bribe_address,
b.contract_address as contract_address,
b.contract_name, 
round(cast(bytearray_to_uint256(bytearray_substring(data,33,32)) as double)/pow(10,18)) as amount,
from_unixtime(cast(bytearray_to_int256(bytearray_substring(data,65)) as double)) as start_time
from {{source('frax_finance_bnb','bnb.logs')}}  a
left join cte b
on a.contract_address = b.bribe_address 
where topic0 = 0x6a6f77044107a33658235d41bedbbaf2fe9ccdceb313143c947a5e76e1ec8474
and bytearray_substring(data,1,32) = 0x000000000000000000000000e48a3d7d0bc88d552f730b62c006bc925eadb9ee
and a.contract_address in (select bribe_address from cte)
)
,

bribes as (
select 
a.start_time,
sum(case when (token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e  and token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40) then amount/2
when token0 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then amount
 when token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then amount
 end ) as Frax_bribe
 ,
 sum(case  when token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e and token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then amount/2
 when token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then amount
 when token1 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then amount
 end ) as FrxETH_bribe
 from bribe_base a
left join pairs c
on a.contract_address = c.contract_address 
group by 1
)
,
rewards_received as (
select block_time as collection_time,
tx_hash,
a.contract_address,
c.contract_name,
bytearray_substring(topic2,13,21) as reward_token_collected,
round(cast(bytearray_to_uint256(data) as double)/pow(10,18)) as reward_amount,
case when bytearray_substring(topic2,13,21) = 0xe48A3d7d0Bc88d552f730B62c006bC925eadB9eE then round(cast(bytearray_to_uint256(data) as double)/pow(10,18)*b.price)
end as reward_amount_usd
from {{source('frax_finance_bnb','bnb.logs')}} a 
left join (select * from prices where contract_address = 0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0) b
on date_trunc('day',a.block_time) = b.day
left join cte c
on a.contract_address = c.bribe_address
where topic0 = 0x540798df468d7b23d11f156fdb954cb19ad414d150722a7b6d55ba369dea792e
and topic1 = 0x0000000000000000000000008811da0385ccf1848b21475a42ea4d07fc5d964a
and a.contract_address in (select bribe_address from cte)
and block_time > cast('2023-01-02' as date)
)
,

weekly_rewards as (
select
collection_time,
sum(reward_amount_usd) as FXS_collected_usd
from rewards_received 
group by 1
)

select 
week_start,
week_end,
week,
d.Frax_bribe as lwb_frax,
round(d.Frax_bribe * e.price) as lwb_frax_usd,
b.Frax_reserve as Start_Frax,
c.Frax_reserve as End_Frax,
d.FrxETH_bribe as lwb_frxETH,
round(d.FrxETH_bribe * e.price) as lwb_frxETH_usd,
f.FXS_collected_usd,
round(f.FXS_collected_usd - ((d.FrxETH_bribe * e.price)+(d.Frax_bribe * e.price))) as Gross_profit,
b.FrxETH_reserve as Start_FrxETH,
c.FrxETH_reserve as End_FrxETH,
round(b.Frax_TVL) as Start_Frax_TVL,
round(c.Frax_TVL) as End_Frax_TVL,
round(b.FrxETH_TVL) as Start_FrxETH_TVL,
round(c.FrxETH_TVL) as End_FrxETH_TVL
from base_date a
left join TVL_sum b
on a.week_start = b.block_time
left join TVL_sum c
on a.week_end = c.block_time
left join bribes d
on a.week_start = d.start_time 
left join (select day, price from prices where contract_address = 0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0) e
on date(a.week_start) = e.day
left join weekly_rewards f
on (date_add('week',1,a.week_start) <= f.collection_time and date_add('week',2,a.week_start) >= f.collection_time)
order by week_start desc