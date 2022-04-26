CREATE
OR REPLACE VIEW keep3r.view_job_liquidity_log AS (
  with df as (
    select
      *
    from
      keep3r.view_job_liquidities
    union
    select
      migs.*,
      liqs.token as token,
      null as amount
    from
      keep3r.view_job_migrations migs
      inner join (
        -- generates 1 extra line per token of keep3r
        select
          distinct keep3r,
          job,
          token
        from
          keep3r.view_job_liquidities
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
