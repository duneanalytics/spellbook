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
   select jp.token_address as jetton_master, jp.timestamp as block_date, avg(price_usd) as price_usd
   from {{ ref('ton_jetton_price_daily') }} jp
   group by 1, 2
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
), transfers_amount_jetton as (
 select block_date, address,
  sum(case when jetton_flow > 0 then jetton_flow * price_usd else 0 end) as transfer_amount_usd_received,
  sum(case when jetton_flow < 0 then jetton_flow * price_usd else 0 end) as transfer_amount_usd_sent
  from jettons_flow
  join jetton_prices using(jetton_master, block_date)
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
from net_transfers group by 1, 2