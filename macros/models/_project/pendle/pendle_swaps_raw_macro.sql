{%
  macro pendle_swaps(
    blockchain = '',
    project = '',
    version = '',
    markets = '',
    start_date = '2022-11-23'
  )
%}

with 
markets as (
    select * from {{ ref(markets) }}
    where chain = '{{blockchain}}'
    and version = '{{version}}'
    -- https://dune.com/queries/3506956
),
event_logs as (
    select * from {{ source(blockchain, 'logs') }}
    where 1=1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %} 
    and block_time > date'{{ start_date }}'
    and contract_address in (
        select market from markets
        union all
        select yt from markets
    )
    and topic0 in (
        0x829000a5bc6a12d46e30cdcecd7c56b1efd88f6d7d059da6734a04f3764557c4,
        -- Swap (index_topic_1 address caller, index_topic_2 address receiver, int256 netPtOut, int256 netSyOut, uint256 netSyFee, uint256 netSyToReserve)
        0x5d624aa9c148153ab3446c1b154f660ee7701e549fe9b62dab7171b1c80e6fa2,
        -- Burn (index_topic_1 address caller, index_topic_2 address receiver, uint256 amountPYToRedeem, uint256 amountSyOut)
        0xc0025304673122449dd60b9b0093874b0e2fd6fe57af1c7c2fbfee0ccf5ead58
        -- Mint (index_topic_1 address caller, index_topic_2 address receiverPT, index_topic_3 address receiverYT, uint256 amountSyToMint, uint256 amountPYOut)
    )
),
swap_events as (
    select 
        block_time,block_number,index,tx_hash,contract_address,
        substr(topic1, 13, 20) as caller,
        substr(topic2, 13, 20) as receiver,
        bytearray_to_int256 (bytearray_substring (data, 1+(0*32), 32)) / 1e18 as netPtOut,
        bytearray_to_int256 (bytearray_substring (data, 1+(1*32), 32)) / 1e18 as netSyOut,
        bytearray_to_int256 (bytearray_substring (data, 1+(2*32), 32)) / 1e18 as netSyFee,
        bytearray_to_int256 (bytearray_substring (data, 1+(3*32), 32)) / 1e18 as netSyToReserve
    from event_logs
    where contract_address in (
        select market from markets
    )
    and topic0 = 0x829000a5bc6a12d46e30cdcecd7c56b1efd88f6d7d059da6734a04f3764557c4
),
burn_events as (
    select 
        block_time,block_number,index,tx_hash,contract_address,
        substr(topic1, 13, 20) as caller,
        substr(topic2, 13, 20) as receiver,
        bytearray_to_int256 (bytearray_substring (data, 1+(0*32), 32)) / 1e18 as amountPYToRedeem,
        bytearray_to_int256 (bytearray_substring (data, 1+(1*32), 32)) / 1e18 as amountSyOut
    from event_logs
    where contract_address in (
        select yt from markets
    )
    and topic0 = 0x5d624aa9c148153ab3446c1b154f660ee7701e549fe9b62dab7171b1c80e6fa2
),
mint_events as (
    select 
        block_time,block_number,index,tx_hash,contract_address,
        substr(topic1, 13, 20) as caller,
        substr(topic2, 13, 20) as receiverPT,
        substr(topic2, 13, 20) as receiverYT,
        bytearray_to_int256 (bytearray_substring (data, 1+(0*32), 32)) / 1e18 as amountSyToMint,
        bytearray_to_int256 (bytearray_substring (data, 1+(1*32), 32)) / 1e18 as amountPYOut
    from event_logs
    where contract_address in (
        select yt from markets
    )
    and topic0 = 0xc0025304673122449dd60b9b0093874b0e2fd6fe57af1c7c2fbfee0ccf5ead58
),
swap_actions as (
    select 
        'swap' as action,
        -(s.netPtOut) as pt_amt,
        -(s.netSyOut) as sy_amt,
        s.netSyFee as sy_fee,
        s.netSyToReserve as reserve,
        s.receiver as user,
        -- negative means flow into pool
        -- buys are positive
        case 
            when amountSyOut is not null 
                then -(s.netPtOut)
            when amountSyToMint is not null
                then -(netPtOut)
            else null
        end as net_yt,
        -- mint
        -- amountPYOut as py_out,
        -- amountSyToMint as sy_mint,
        -- burn
        -- amountPYToRedeem as py_redeem,
        -- amountSyOut as sy_burn,
        
        s.tx_hash,
        s.index,
        s.block_time,
        s.block_number,
        s.contract_address as market
    from swap_events s
        join markets ma  
            on s.contract_address = ma.market
        left join burn_events b
            on ma.yt = b.contract_address
            and s.tx_hash = b.tx_hash
            -- and b.receiver = 0x4f43c77872Db6BA177c270986CD30c3381AF37Ee
            and s.index > b.index
            and s.index - b.index <= 2
        left join mint_events m
            on ma.yt = m.contract_address
            and s.tx_hash = m.tx_hash
            and s.index-1 = m.index
)

select *
from swap_actions