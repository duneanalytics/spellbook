-- Compare distinct detected attacker trade rows, not triple counts or attacks.
-- This isolates detection before transaction enrichment and whitelist exclusion;
-- it is not a diff of the stored production tables. Widen the window deliberately.
with trades as (
    select blockchain, project, version, block_time, block_number, tx_hash
    , evt_index, project_contract_address, tx_from
    , token_sold_address, token_bought_address, amount_usd
    from dex.trades
    where block_month = date '2026-09-01'
      and block_date = date '2026-09-08'
      and block_time >= timestamp '2026-09-08 12:00:00'
      and block_time < timestamp '2026-09-08 13:00:00'
      and blockchain in (
          'arbitrum', 'avalanche_c', 'bnb', 'ethereum', 'fantom', 'gnosis',
          'optimism', 'polygon', 'base', 'celo', 'zksync', 'scroll', 'zora',
          'unichain', 'sei', 'mantle'
      )
), matched_legs as (
    select front.blockchain, front.project, front.version
    , front.project_contract_address
    , leg.tx_hash, leg.evt_index
    , front.block_number = back.block_number
      and front.block_number = victim.block_number as same_block
    from trades front
    inner join trades back on front.blockchain = back.blockchain
      and front.block_time = back.block_time
      and front.project_contract_address = back.project_contract_address
      and front.tx_from = back.tx_from
      and front.tx_hash != back.tx_hash
      and front.token_sold_address = back.token_bought_address
      and front.token_bought_address = back.token_sold_address
      and front.evt_index + 1 < back.evt_index
    inner join trades victim on front.blockchain = victim.blockchain
      and front.block_time = victim.block_time
      and front.project_contract_address = victim.project_contract_address
      and front.tx_from != victim.tx_from
      and front.token_bought_address = victim.token_bought_address
      and front.token_sold_address = victim.token_sold_address
      and victim.evt_index between front.evt_index and back.evt_index
    cross join unnest(array[
        (front.tx_hash, front.evt_index), (back.tx_hash, back.evt_index)
    ]) as leg(tx_hash, evt_index)
), membership as (
    select blockchain, project_contract_address, tx_hash, evt_index
    , bool_or(same_block) as retained
    from matched_legs
    group by 1, 2, 3, 4
), totals as (
    select t.blockchain
    , count(*) as old_attacker_rows
    , count_if(m.retained) as corrected_attacker_rows
    , count_if(not m.retained) as removed_attacker_rows
    , sum(t.amount_usd) as old_attacker_volume_usd
    , sum(if(m.retained, t.amount_usd, 0)) as corrected_attacker_volume_usd
    , count_if(t.amount_usd is null) as rows_missing_usd
    from trades t
    inner join membership m on t.blockchain = m.blockchain
      and t.project_contract_address = m.project_contract_address
      and t.tx_hash = m.tx_hash
      and t.evt_index = m.evt_index
    group by 1
), chain_coverage as (
    select blockchain, count(*) as source_trade_rows from trades group by 1
)
select c.blockchain, c.source_trade_rows
, coalesce(t.old_attacker_rows, 0) as old_attacker_rows
, coalesce(t.corrected_attacker_rows, 0) as corrected_attacker_rows
, coalesce(t.removed_attacker_rows, 0) as removed_attacker_rows
, 100e0 * t.removed_attacker_rows / nullif(t.old_attacker_rows, 0) as removed_pct
, t.old_attacker_volume_usd, t.corrected_attacker_volume_usd, t.rows_missing_usd
from chain_coverage c
left join totals t on c.blockchain = t.blockchain
order by removed_attacker_rows desc, c.blockchain
limit 16
