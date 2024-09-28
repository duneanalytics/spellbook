with
    unit_tests as (
        select
            case
                when test_data_v1.tokenid = transfers_v2.tokenid then true else false
            end as test
        from {{ ref("test_view") }} transfers_v2
        join
            {{ ref("test_seed") }} test_data_v1
            on test_data_v1.evt_tx_hash = transfers_v2.evt_tx_hash
            and test_data_v1.value = abs(transfers_v2.amount)
    )
select
    count(case when test = false then 1 else null end) / count(*) as pct_mismatch,
    count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*) * 0.05
 