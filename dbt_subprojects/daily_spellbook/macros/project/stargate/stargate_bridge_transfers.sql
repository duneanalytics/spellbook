{% macro stargate_bridge_transfers(blockchain) %}

with assets as (
    select * from {{ ref('stargate_bridge_token_mapping') }} where chain = '{{ blockchain }}'
),

tx as (
    select 
        e.block_time, 
        e.block_number, 
        e.contract_address as pool_name,
        varbinary_substring(e.topic2, 13, 20) as user,
        varbinary_to_uint256(varbinary_substring(e.data, 1, 32)) as dest,
        varbinary_to_uint256(varbinary_substring(e.data, 65, 32)) as amount_raw,
        e.tx_hash,
        e.blockchain
    from {{ source(blockchain, 'logs') }} e
    where e.topic0 = 0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a
),

bridges as (
    select 
        t.*,
        a.token,
        t.amount_raw / power(10, a.decimals) as amount,
        a.decimals,
        t.blockchain as from_chain,
        COALESCE(a1.chain, CAST(t.dest AS varchar)) as to_chain,
        concat(
            CAST(t.blockchain AS varchar),
            ' -> ',
            COALESCE(a1.chain, CAST(t.dest AS varchar))
        ) as pathway,
        case 
            when a.token in ('USDC','USDT','m.USDT','USDC.e') then  1
            when a.token in ('ETH','WETH','mETH') then p.price
            else null
        end as price,
        case 
            when a.token in ('USDC','USDT','m.USDT','USDC.e') then  t.amount_raw / power(10, a.decimals)
            when a.token in ('ETH','WETH','mETH') then ( t.amount_raw / power(10, a.decimals)) * p.price
            else null
        end as amount_usd
    from tx t
    join assets a 
      on t.pool_name = a.pool
      and t.blockchain = a.chain
    left join {{ ref('stargate_bridge_token_mapping') }} a1 
      on t.dest = a1.endpointID
    left join {{ source('prices', 'day') }} p
      on date_trunc('day', t.block_time) = p.timestamp
     and p.blockchain = 'ethereum'
     and p.contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
)

select distinct * from bridges

{% endmacro %}
