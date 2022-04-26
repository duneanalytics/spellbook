CREATE OR REPLACE VIEW keep3r_network.view_job_liquidity_log AS (
  with job_liquidities as (
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
  ),
  df as (
    select
      *
    from
      job_liquidities

    union

    select
      migs.*,
      liqs.token as token,
      null as amount
    from
      keep3r_network.view_job_migrations migs
      inner join (
        -- generates 1 extra line per token of keep3r
        select
          distinct keep3r,
          job,
          token
        from
          job_liquidities
      ) liqs on migs.keep3r = liqs.keep3r
  ),
  migration_out as (
    select
      *,
      case when event = 'JobMigrationOut' then sum(- amount) over (
        partition by keep3r, job, token rows unbounded preceding
      ) end as migration_out
    from
      df
  ),
  migration_in as (
    select
      *,
      case when event = 'JobMigrationIn' then lag(- migration_out) over (
        partition by tx_hash,
        keep3r,
        token
        order by
          evt_index
      ) end as migration_in
    from
      migration_out
  )
  select
    timestamp,
    tx_hash,
    evt_index,
    event,
    keep3r,
    job,
    token,
    COALESCE(
      amount, migration_out, migration_in
    ) as amount
  from
    migration_in
  order by
    timestamp
)
