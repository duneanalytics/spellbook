{{ config(
        schema='lido_accounting_ethereum',
        alias = 'withdrawals',

        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'append'
        , post_hook='{{ hide_spells() }}'
        )
}}


with withdrawals as (
    select block_time          as time
         , block_hash
         , sum(amount) / 1e9   as total_amount
         , sum(CASE
                   WHEN amount / 1e9 BETWEEN 20 AND 32 THEN CAST(amount as double) / 1e9
                   WHEN amount / 1e9 > 32 THEN 32
                   ELSE 0 END) AS withdrawn_principal
    from {{source('ethereum', 'withdrawals')}}
    where address = 0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f
    {% if is_incremental() %}
    -- safe to window: the aggregation grain (block_time, block_hash) never spans
    -- the window boundary
    and {{ incremental_predicate('block_time') }}
    {% endif %}
    group by 1,2
)

select time as period
    , block_hash as hash
    , withdrawn_principal*POWER(10, 18) as amount
    , date_trunc('day', time) as day
from withdrawals w
where withdrawn_principal != 0
{% if is_incremental() %}
-- append-only dedup: drop blocks already inserted by a previous run inside the
-- incremental window (block_hash is unique per block)
and not exists (
    select 1
    from {{ this }} t
    where t.period = w.time
      and t.hash = w.block_hash
)
{% endif %}
