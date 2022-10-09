with unit_test1
    as (select case
                 when usd_amount = 19436.28 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_borrow' )}}
        where  token_address = '0x68f180fcce6836688e9084f035309e29bf0a2095'
               and borrower = '0x290e28bf4fbb5abf8d7830496847d783f5a67007'
               and evt_block_time = '2022-10-09 08:22'
               and evt_tx_hash = '0x2b4f4770d317e5760c369ff4f3fbb17995ceb4a876ecd308008b4956fd8e7d40'),
    unit_test2
    as (select case
                 when amount = -20.25 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_borrow' )}}
        where  symbol = 'USDC'
               and repayer = '0x70371a494f73a8df658c5cd29e2c1601787e1009'
               and evt_block_time = '2022-10-09 06:39')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false