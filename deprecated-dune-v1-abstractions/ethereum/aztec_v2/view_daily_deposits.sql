drop view if exists aztec_v2.view_daily_deposits;

create or replace view aztec_v2.view_daily_deposits as 
with daily_transfers as (
    select date_trunc('day', evt_block_time) as date
        , contract_address as token_address
        , count(*) as num_tfers -- number of transfers
        , count(distinct evt_tx_hash) as num_rollups -- number of rollups
        , sum(case when spec_txn_type in ('User Deposit','User Withdrawal') then value_norm else 0 end ) as abs_value_norm
        , sum(case when spec_txn_type = 'User Deposit' then value_norm else 0 end ) as user_deposit_value_norm
        , sum(case when spec_txn_type = 'User Withdrawal' then value_norm else 0 end ) as user_withdrawal_value_norm
    from aztec_v2.view_rollup_bridge_transfers
    where spec_txn_type in ('User Deposit','User Withdrawal')
    group by 1,2
)
, daily_volume as (
    select dt.date
        , dt.token_address
        , p.symbol
        , dt.num_rollups
        , dt.num_tfers
        , dt.abs_value_norm
        , dt.abs_value_norm * p.avg_price_usd as abs_volume_usd
        , dt.abs_value_norm * p.avg_price_eth as abs_volume_eth
        , dt.user_deposit_value_norm * p.avg_price_usd as user_deposits_usd
        , dt.user_deposit_value_norm * p.avg_price_eth as user_deposits_eth
        , dt.user_withdrawal_value_norm * p.avg_price_usd as user_withdrawals_usd
        , dt.user_withdrawal_value_norm * p.avg_price_eth as user_withdrawals_eth
    from daily_transfers dt
    -- inner join dune_user_generated.table_aztec_v2_daily_bridged_tokens_prices_cached p on dt.date = p.date and dt.token_address = p.token_address
    inner join aztec_v2.daily_token_prices p on dt.date = p.date and dt.token_address = p.token_address
)
select * from daily_volume;