with unit_test1 as (
    -- cErc20 mint
    select case
        when amount = 6203.590576 then true
        else false
    end as test
    from {{ ref('compound_ethereum_supply') }}
    where tx_hash = 0xbd26747069016ebea68a7f5fe27b3ad0aec9502cd91c81dcf572a94e4a70d391
        and depositor = 0x6d8bfdb4c4975bb086fc9027e48d5775f609ff88
        and token_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
),
unit_test2 as (
    -- cEther mint
    select case
        when abs(amount - 25.190814828273316) < 1e-12 then true
        else false
    end as test
    from {{ ref('compound_ethereum_supply') }}
    where tx_hash = 0x10f0743281e65468fb219df648ad333903e98067d53a695d7884deaddb5a01a2
        and depositor = 0x56178a0d5f301baf6cf3e1cd53d9863437345bf9
        and token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
),
unit_test3 as (
    -- cErc20 redeem
    select case
        when usd_amount = -4506433.947672356 then true
        else false
    end as test
    from {{ ref('compound_ethereum_supply') }}
    where tx_hash = 0x5ac3b9766a7a69f967965ce1ea8e09bfa6ab06aaac5f4cac7bd9eb0d194b6b09
        and withdrawn_to = 0x12c012ac4b947a072a1f6abb478d094094931215
        and token_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
),
unit_test4 as (
    -- cEther redeem
    select case
        when usd_amount = -25.819310507320957 then true
        else false
    end as test
    from {{ ref('compound_ethereum_supply') }}
    where tx_hash = 0xdf0042e538d1648181700514cac4df273ee2b635eb3c49ff2c3159580faf0345
        and withdrawn_to = 0xcf1383458a1b2fd705694b4cb1441dee244b09cf
        and token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
)

select * from (
    select 'test1' as test_no, * from unit_test1
    union all
    select 'test2' as test_no, * from unit_test2
    union all
    select 'test3' as test_no, * from unit_test3
    union all
    select 'test4' as test_no, * from unit_test4
)
where test = false
