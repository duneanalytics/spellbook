with unit_test as (
    select
        case when test.network = actual.network then true else false end as network_test
        case when test.drawId = actual.drawId then true else false end as drawId_test
        case when test.bitRange = actual.bitRange then true else false end as bitRange_test
        case when test.tiers1 = actual.tiers1 then true else false end as tiers1_test
        case when test.dpr = actual.dpr then true else false end as dpr_test
        case when test.prize = actual.prize then true else false end as prize_test
    from {{ref('pooltogether_v4_ethereum_prize_structure')}} as actual
    inner join {{ref('pooltogether_v4_ethereum_prize_structure_seed')}} as test
        on actual.tx_hash = test.tx_hash
)
select * from unit_test
where
    network_test = false
    or network_test = false
    or drawId_test = false
    or bitRange_test = false
    or tiers1_test = false
    or dpr_test = false
    or prize_test = false