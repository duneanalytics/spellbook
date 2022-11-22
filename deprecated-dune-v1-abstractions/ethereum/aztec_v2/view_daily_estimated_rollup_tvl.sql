create or replace view aztec_v2.view_daily_estimated_rollup_tvl as 
with rollup_balance_changes as (
  select t.evt_block_time::date as date
    , t.symbol
    , t.contract_address as token_address
    , sum(case when t.from_type = 'Rollup' then -1 * value_norm when t.to_type = 'Rollup' then value_norm else 0 end) as net_value_norm
  from aztec_v2.view_rollup_bridge_transfers t
  where t.from_type = 'Rollup' or t.to_type = 'Rollup'
  group by 1,2,3
)
, token_balances as (
  select date
    , symbol
    , token_address
    , sum(net_value_norm) over (partition by symbol,token_address order by date asc rows between unbounded preceding and current row) as balance
    , lead(date, 1) over (partition by token_address order by date) as next_date
  from rollup_balance_changes
)
, day_series as (
  select generate_series(min(date), now()::date, interval '1 day') as date
  from token_balances
)
, token_balances_filled as (
  select d.date
    , b.symbol
    , b.token_address
    , b.balance
  from day_series d
  inner join token_balances b
        on d.date >= b.date
        and d.date < coalesce(b.next_date,now()::date + 1) -- if it's missing that means it's the last entry in the series
)
, token_tvls as (
  select b.date
    , b.symbol
    , b.token_address
    , b.balance
    , b.balance * p.avg_price_usd as tvl_usd
    , b.balance * p.avg_price_eth as tvl_eth
  from token_balances_filled b
  -- inner join dune_user_generated.table_aztec_v2_daily_bridged_tokens_prices_cached p on b.date = p.date and b.token_address = p.token_address
  inner join aztec_v2.daily_token_prices p on b.date = p.date and b.token_address = p.token_address
  
)
select * from token_tvls;