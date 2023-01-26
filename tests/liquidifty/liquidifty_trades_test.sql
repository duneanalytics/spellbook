with unit_test1 as (
    select case
        when amount_usd = 112.528 then true
        else false
    end as test
    from {{ ref('liquidifty_trades') }}
    where evt_tx_hash = '0x00536611a351d98ca37009aa5b557c49f8f3fc00e1f78300ce77b92b4fed942b'
        and seller = '0x3b6e760ca5b5c9fc492f1533b6a4728fa4e59e2d'
        and buyer = '0x42661ba55093e4417117c6c2379727a764e12fd1'
),
unit_test2 as (
    select case
        when token_id = 6980 then true
        else false
    end as test
    from {{ ref('liquidifty_trades') }}
    where evt_tx_hash = '0xca29b9675c2dc75d0967c5ea47cadb7f2cbb20580efcae266447efd9ccbb3a71'
        and seller = '0xf84ca8ff043062acb6203ec5b1d5962675d86530'
        and nft_contract_address = '0xda216128024e122354ba20b648b8cc0a3e2be51c'
)

select * from (
    select 'test1' as test_no, * from unit_test1
    union all
    select 'test2' as test_no, * from unit_test2
)
where test = false
