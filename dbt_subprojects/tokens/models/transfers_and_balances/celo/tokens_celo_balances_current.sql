{{
    config (
        schema = 'tokens_celo',
        alias = 'balances_current',
        file_format = 'delta',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['address', 'token_address', 'token_standard'],
    )
}}

-- Deep current-balance state for tokens_celo_balances_daily_agg_base.
-- One row per (address, token_address, token_standard) = its latest balance_raw as of
-- block_time < now() - 5 days. This lets the base model seed prior_balances by reading
-- this small key-grained table instead of max_by-aggregating the full balances history
-- every hourly build. Equivalence relies on the max_by split-at-cutoff identity:
--   max_by(balance_raw, day) over all history < W
--   == max_by(balance_raw, day) over ( latest-per-key(< T-5) UNION gap[T-5, W) )
-- The base model owns the gap [T-7, W); this table owns everything below the cutoff.
--
-- Cycle note: this model ref()s the base table, so dbt builds the base FIRST and this
-- table AFTER. The base model reads this table via adapter.get_relation() (no ref edge),
-- so there is no dbt dependency cycle. The base therefore reads the PREVIOUS run's state;
-- the base's gap window absorbs that staleness.

with new_state as (
    select
        address,
        token_address,
        token_standard,
        max_by(balance_raw, day) as balance_raw,
        max(day) as day
    from {{ ref('tokens_celo_balances_daily_agg_base') }}
    where block_time < date_trunc('day', now() - interval '5' day)
    {%- if is_incremental() %}
        -- self-healing catch-up: re-absorb from the last day already in the state forward.
        -- Re-merging the boundary day is idempotent (same max_by); a missed run is caught
        -- up automatically because max(day) does not advance until the day is absorbed.
        and day >= (select coalesce(max(day), date '2000-01-01') from {{ this }})
    {%- endif %}
    group by 1, 2, 3
)

select
    address,
    token_address,
    token_standard,
    balance_raw,
    day
from new_state
