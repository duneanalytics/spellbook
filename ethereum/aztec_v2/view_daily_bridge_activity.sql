drop view if exists aztec_v2.view_daily_bridge_activity;

create or replace view aztec_v2.view_daily_bridge_activity as 
with daily_transfers as (
    select date_trunc('day', evt_block_time) as date
        , bridge_protocol
        , bridge_address
        , contract_address as token_address
        , count(*) as num_tfers -- number of transfers
        , count(distinct evt_tx_hash) as num_rollups -- number of rollups
        , sum(case when spec_txn_type in ('Bridge to Protocol','Protocol to Bridge') then value_norm else 0 end ) as abs_value_norm
        , sum(case when spec_txn_type = 'Bridge to Protocol' then value_norm else 0 end ) as input_value_norm
        , sum(case when spec_txn_type = 'Protocol to Bridge' then value_norm else 0 end ) as output_value_norm
    from aztec_v2.view_rollup_bridge_transfers
    where bridge_protocol is not null -- exclude all txns that don't interact with the bridges
    group by 1,2,3,4
)
, daily_volume as (
    select dt.date
        , dt.bridge_protocol
        , dt.bridge_address
        , dt.token_address
        , p.symbol
        , dt.num_rollups
        , dt.num_tfers
        , dt.abs_value_norm
        , dt.abs_value_norm * p.avg_price_usd as abs_volume_usd
        , dt.abs_value_norm * p.avg_price_eth as abs_volume_eth
        , dt.input_value_norm * p.avg_price_usd as input_volume_usd
        , dt.input_value_norm * p.avg_price_eth as input_volume_eth
        , dt.output_value_norm * p.avg_price_usd as output_volume_usd
        , dt.output_value_norm * p.avg_price_eth as output_volume_eth
    from daily_transfers dt
    -- inner join dune_user_generated.table_aztec_v2_daily_bridged_tokens_prices_cached p on dt.date = p.date and dt.token_address = p.token_address
    inner join aztec_v2.daily_token_prices p on dt.date = p.date and dt.token_address = p.token_address
)
select * from daily_volume;