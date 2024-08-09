{%
  macro pendle_prices_raw(
    blockchain = '',
    project = '',
    version = '',
    markets = '',
    start_date = '2022-11-23'
  )
%}

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
    )
    and topic0 in (
        0x5c0e21d57bb4cf91d8fe238d6f92e2685a695371b19209afcce6217b478f83e1
        -- UpdateImpliedRate (index_topic_1 uint256 timestamp, uint256 lnLastImpliedRate)
    )
),
implied_rate_updates as (
    select 
        m.market,
        m.PT,
        m.SY,
        m.YT,
        m.expiry,
        l.block_time as evt_block_time,
        bytearray_to_uint256(l.topic1) as timestamp,
        bytearray_to_uint256(l.data) as lnLastImpliedRate,
        
        -- u.underlying,
        bytearray_to_uint256(l.data)/1e18 as implied_rate,
        (case when bytearray_to_uint256(l.topic1) <=  m.expiry then m.expiry-bytearray_to_uint256(l.topic1) else 0 end) as time_to_maturity,
        -- refer : https://docs.pendle.finance/Developers/Oracles/IntroductionOfPtOracle
        1/(pow(
            e(),
            -- (implied_rate * time_to_maturity) / (365.0*86400)
            ((bytearray_to_uint256(l.data)/1e18) * (
                (case when bytearray_to_uint256(l.topic1) <=  m.expiry then m.expiry-bytearray_to_uint256(l.topic1) else 0 end)
            )) / (365.0*86400)
        )) as pt_to_asset
    from event_logs l 
        join markets m
            on l.contract_address = m.market
        -- join underlying_manual u
        --     on m.pt = u.pt
)

select i.*,
    1-i.pt_to_asset as yt_to_asset,
    1 as asset,
    1/(1-least(i.pt_to_asset,0)) as leverage
    -- p.price as sy_price,
    -- i.pt_to_asset*p.price as pt_price,
    -- (1-i.pt_to_asset)*p.price as yt_price,
-- from underlying_manual
from implied_rate_updates i

{% endmacro %}