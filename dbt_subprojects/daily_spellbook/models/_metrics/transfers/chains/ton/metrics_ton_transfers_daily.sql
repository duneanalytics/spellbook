{{ config(
        schema = 'metrics_ton'
        , alias = 'transfers_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}
      

with ton_prices as ( -- get price of TON for each day to estimate USD value
    select
        date_trunc('day', minute) as block_date
        , avg(price) as price
    from {{ source('prices', 'usd') }}
    where true
        and symbol = 'TON' and blockchain is null
        group by 1
), jetton_prices as (
   select jp.token_address as jetton_master,
   case
     when
        jp.token_address = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE' -- USDT
        or jp.token_address = '0:BDF3FA8098D129B54B4F73B5BAC5D1E1FD91EB054169C3916DFC8CCD536D1000' -- tsTON
        or jp.token_address = '0:CD872FA7C5816052ACDF5332260443FAEC9AACC8C21CCA4D92E7F47034D11892' -- stTON
        or jp.token_address = '0:CF76AF318C0872B58A9F1925FC29C156211782B9FB01F56760D292E56123BF87' -- hTON
    then 0 -- USDT, and LSDs are liquid and doesn't need to be limited by liquidity
     when asset_type = 'Jetton' then 1 -- other jettons needs to be checked by DEX liquidity
     else 0 -- DEX LPs and SLPs liquidity is guaranteed by their smart contracts
    end as is_need_liquidity_limit,
   jp.timestamp as block_date, avg(price_usd) as price_usd
   from {{ ref('ton_jetton_price_daily') }} jp
   group by 1, 2, 3
),
ton_flow as (
    select block_date, source as address, -1 * value as ton_flow
    from
        {{ source('ton', 'messages') }}
    where
        direction = 'in'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    
    union all

    select block_date, destination as address, value as ton_flow
    from
        {{ source('ton', 'messages') }}
    where
        direction = 'in'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), transfers_amount_ton as (
 select block_date, address,
  sum(case when ton_flow > 0 then ton_flow * price else 0 end) / 1e9 as transfer_amount_usd_received,
  sum(case when ton_flow < 0 then ton_flow * price else 0 end) / 1e9 as transfer_amount_usd_sent
  from ton_flow
  join ton_prices using(block_date)
group by 1, 2
), 
jettons_flow as (
    select block_date, jetton_master, source as address, -1 * amount as jetton_flow
    from
        {{ source('ton', 'jetton_events') }}
    where
        type = 'transfer'
        and jetton_master != upper('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c') -- pTON
        and jetton_master != upper('0:671963027f7f85659ab55b821671688601cdcf1ee674fc7fbbb1a776a18d34a3') -- pTON
        and not tx_aborted
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    
    union all

    select block_date, jetton_master, destination as address, amount as jetton_flow
    from
        {{ source('ton', 'jetton_events') }}
    where
        type = 'transfer'
        and jetton_master != upper('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c') -- pTON
        and jetton_master != upper('0:671963027f7f85659ab55b821671688601cdcf1ee674fc7fbbb1a776a18d34a3') -- pTON
        and not tx_aborted
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), daily_liquidity as (
 -- since we are using dex derived prices we can encounter a situation where the amount of tokens being transferred
 -- is much higher than the amount of liquidity in the pools. To mitigate this we will use the daily liquidity of the tokens
 -- and use it as the upper bound for the amount of tokens being transferred.
  select block_date, jetton_master, sum(tvl_usd) as total_token_tvl_usd from (
    select block_date, pool, jetton_left, jetton_right, avg(tvl_usd) as tvl_usd from {{ source('ton', 'dex_pools') }} 
    where tvl_usd > 0
    group by 1, 2, 3, 4)
  cross join unnest(array[jetton_left, jetton_right]) as t(jetton_master)
  group by 1, 2
),
transfers_amount_jetton as (
 select block_date, address,
  sum(case
    when jetton_flow > 0 and is_need_liquidity_limit = 0 -- treat volume as is
        then jetton_flow * price_usd
    when jetton_flow > 0 and is_need_liquidity_limit = 1 -- limit volume by total token TVL to avoid price manipulation for low-liquidity tokens
        then least(jetton_flow * price_usd, coalesce(total_token_tvl_usd, 0))
    else 0 end) as transfer_amount_usd_received,
  sum(case
    when jetton_flow < 0 and is_need_liquidity_limit = 0 -- treat volume as is
        then jetton_flow * price_usd
    when jetton_flow < 0 and is_need_liquidity_limit = 1 -- limit volume by total token TVL to avoid price manipulation for low-liquidity tokens
        then -1 * least(abs(jetton_flow) * price_usd, coalesce(total_token_tvl_usd, 0))
    else 0 end) as transfer_amount_usd_sent
  from jettons_flow
  join jetton_prices using(jetton_master, block_date)
  left join daily_liquidity using(block_date, jetton_master)
group by 1, 2
), transfers_amount as (
  select * from transfers_amount_jetton
  union all
  select * from transfers_amount_ton
), net_transfers as (
  select block_date, address, 
  sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received,
  sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent,
  sum(coalesce(transfer_amount_usd_sent, 0)) + sum(coalesce(transfer_amount_usd_received, 0)) as net_transfer_amount_usd
  from transfers_amount group by 1, 2
)
select 'ton' as blockchain
    , block_date
    , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
    , sum(transfer_amount_usd_received) as transfer_amount_usd_received
    , sum(abs(transfer_amount_usd_sent)) + sum(abs(transfer_amount_usd_received)) as transfer_amount_usd
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
from net_transfers
where
    net_transfer_amount_usd > 0
group by 1, 2