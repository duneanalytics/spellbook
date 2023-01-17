with unit_test1 as (
    -- cErc20 mint
    select case
        when amount = 6203.590576 then true
        else false
    end as test
    from {{ ref('compound_v2_ethereum_supply') }}
    where evt_tx_hash = '0xbd26747069016ebea68a7f5fe27b3ad0aec9502cd91c81dcf572a94e4a70d391'
        and depositor = '0x6d8bfdb4c4975bb086fc9027e48d5775f609ff88'
        and token_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
),
unit_test2 as (
    -- cEther mint
    select case
        -- actually 25.190814828273316528
        when amount = 25.190814828273316000 then true
        else false
    end as test
    from {{ ref('compound_v2_ethereum_supply') }}
    where evt_tx_hash = '0x10f0743281e65468fb219df648ad333903e98067d53a695d7884deaddb5a01a2'
        and depositor = '0x56178a0d5f301baf6cf3e1cd53d9863437345bf9'
        and token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
)

select * from (
    select * from unit_test1
    union
    select * from unit_test2
)
where test = false
