{{
  config(
    schema = 'gooddollar_celo',
    alias = 'ubi_claimer_streaks',
    materialized = 'view',
    unique_key = ['claimer'],
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "gooddollar",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

ubi_claimed as (
  select distinct
    block_date,
    claimer
  from {{ ref('gooddollar_celo_ubi_claims') }}
),

streak_data as (
  select
    claimer,
    block_date,
    date_diff('day', (select min(block_date) from ubi_claimed), block_date) - 
      row_number() over (partition by claimer order by block_date) as streak_group
  from ubi_claimed
),

streak_summary as (
  select
    claimer,
    streak_group,
    min(block_date) as streak_start,
    max(block_date) as streak_end,
    date_diff('day', min(block_date), max(block_date)) + 1 as streak_length
  from streak_data
  group by claimer, streak_group
),

max_streaks as (
  select
    claimer,
    max(streak_length) as longest_daily_streak
  from streak_summary
  group by claimer
),

latest_date as (
  select max(block_date) as max_date
  from ubi_claimed
),

current_streaks as (
  select
    s.claimer,
    case
      when s.streak_end = ld.max_date then s.streak_length
      else 0
    end as current_streak
  from streak_summary s
    cross join latest_date ld
  where (s.claimer, s.streak_end) in (
    select claimer, max(streak_end)
    from streak_summary
    group by claimer
  )
),

activity_metrics as (
  select
    claimer,
    min(block_date) as first_claim_date,
    max(block_date) as last_claim_date,
    count(distinct block_date) as total_claim_days,
    date_diff('day', min(block_date), max(block_date)) + 1 as total_days_span,
    round(count(distinct block_date) * 100.0 / (date_diff('day', min(block_date), max(block_date)) + 1), 2) as claim_rate_pct,
    count(distinct streak_group) as total_streaks
  from streak_data
  group by claimer
),

recent_activity as (
  select
    claimer,
    count(*) as claims_last_30_days
  from ubi_claimed
  where block_date >= date_add('day', -30, (select max_date from latest_date))
  group by claimer
)

select
  m.claimer,
  m.longest_daily_streak,
  c.current_streak,
  a.total_claim_days,
  a.total_days_span,
  a.claim_rate_pct,
  a.first_claim_date,
  a.last_claim_date,
  date_diff('day', a.last_claim_date, (select max_date from latest_date)) as days_since_last_claim,
  a.total_streaks,
  coalesce(ra.claims_last_30_days, 0) as claims_last_30_days
from max_streaks m
  inner join current_streaks c on m.claimer = c.claimer
  inner join activity_metrics a on m.claimer = a.claimer
  left join recent_activity ra on m.claimer = ra.claimer
