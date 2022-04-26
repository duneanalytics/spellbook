CREATE OR REPLACE VIEW keep3r.view_job_liquidities AS
(
  select
  add."evt_block_time" as TIMESTAMP,
    '0x' || encode(add."evt_tx_hash",'hex') as tx_hash,
    evt_index,
    'LiquidityAddition' as event,
    '0x' || encode(add."contract_address",'hex') keep3r,
    '0x' || encode(add."_job",'hex') job,
    '0x' || encode(add."_liquidity",'hex') as token,
    add."_amount" / 1e18 as amount
  from (
      SELECT *
      FROM keep3r_network."Keep3r_evt_LiquidityAddition"
      UNION
      SELECT *
      FROM keep3r_network."Keep3r_v2_evt_LiquidityAddition"
    )
  add
  UNION ALL
  select rm."evt_block_time" as TIMESTAMP,
    '0x' || encode(rm."evt_tx_hash", 'hex') as tx_hash,
    evt_index,
    'LiquidityWithdrawal' as event,
    '0x' || encode(rm."contract_address",'hex') keep3r,
    '0x' || encode(rm."_job", 'hex') job,
    '0x' || encode(rm."_liquidity", 'hex') as token,
    - rm."_amount" / 1e18 as amount
  from (
      SELECT *
      FROM keep3r_network."Keep3r_evt_LiquidityWithdrawal"
      UNION
      SELECT *
      FROM keep3r_network."Keep3r_v2_evt_LiquidityWithdrawal"
    ) rm
)
