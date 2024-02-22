{{ config(
        schema = 'tokens_ethereum',
        alias = 'base_balances_daily',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['unique_key'],
        )
}}

with daily_balances as (select
    day,
    block_number,
    block_time,
    "type",
    "address",
    contract_address,
    token_id,
    amount,
    LEAD(day, 1, current_timestamp) OVER (PARTITION BY "type", "address", "contract_address", "token_id" ORDER BY day) AS next_day
from {{ ref('tokens_ethereum_base_balances_daily_agg') }} balances
{% if is_incremental() %}
WHERE {{incremental_predicate('day')}}
{% endif %}
),
days as (
    -- TODO: Start date 12 months ago
SELECT day FROM unnest(sequence(current_date - interval '12' month, current_date, interval '1' day)) AS t(day)
)
select
    d.day,
    b.block_number,
    b.block_time,
    b."type",
    b."address",
    b.contract_address,
    b.token_id,
    b.amount
from daily_balances b
join days d ON b.day <= d.day AND d.day < b.next_day

