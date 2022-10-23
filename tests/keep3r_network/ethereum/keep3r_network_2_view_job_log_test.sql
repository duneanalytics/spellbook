
with unit_test as (
    select
        case when test.amount = round(actual.amount, 2) then true else false end as amount_test,
        case when lower(test.keeper) = lower(actual.keeper) then true else false end as keeper_test,
        case when lower(test.token) = lower(actual.token) then true else false end as token_test
    from {{ref('keep3r_network_ethereum_view_job_log')}} as actual
    inner join {{ref('keep3r_network_ethereum_view_job_log_postgres')}} as test
        on lower(actual.tx_hash) = lower(text.tx_hash)
)
select * from unit_test
where amount_test = false and keeper_test = false and token_test = false