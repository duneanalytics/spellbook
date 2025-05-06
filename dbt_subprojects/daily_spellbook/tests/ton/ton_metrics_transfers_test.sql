with test_data as (
    select *
    from {{ ref('metrics_ton_transfers_daily') }}
    where block_date between TIMESTAMP '2023-01-01' and TIMESTAMP '2025-02-24'
),

test_result as (
    select case when net_transfer_amount_usd < 50 * 1e9 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
