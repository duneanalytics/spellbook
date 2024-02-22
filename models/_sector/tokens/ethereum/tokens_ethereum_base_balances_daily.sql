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

with balances_agg as (
    select *
    from (
        select
            cast(date_trunc('day', block_time) as date) as day,
            block_number,
            block_time,
            "type",
            "address",
            contract_address,
            token_id,
            amount,
            row_number() OVER (partition by date_trunc('day', block_time), type, address, contract_address, token_id order by block_number desc) as row_number
        from source('tokens_base', 'balances_ethereum_0004') balances
        {% if is_incremental() %}
        WHERE {{incremental_predicate('block_time')}}
        {% endif %}
    ) where row_number = 1
),
daily_balances as (select
    day,
    block_number,
    block_time,
    "type",
    "address",
    contract_address,
    token_id,
    amount,
    LEAD(day, 1, current_timestamp) OVER (PARTITION BY "type", "address", "contract_address", "token_id" ORDER BY day) AS next_day
from balances_agg
),
days as (
    -- TODO: Start date 12 months ago
SELECT day FROM unnest(sequence(current_date - interval '12' month, current_date, interval '1' day)) AS t(day)
)
select d.day, b.*
from daily_balances b
join days d ON b.day <= d.day AND d.day < b.next_day

