with unit_test as(
    select
        case when lower(test.symbol) = lower(actual.symbol) then true else false end as smybol_test
    from {{ref('reaper_optimism_all_vaults')}} as actual
    inner join {{ref('reaper_optimism_all_vaults_postgres')}} as test
        on lower(actual.contract_address) = lower(test.contract_address)
)
select * from unit_test
where symbol_test = false
