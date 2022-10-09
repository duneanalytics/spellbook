with unit_test1
    as (select case
                 when usd_amount = 12.34988436 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_supply' )}}
        where  token_address = '0x68f180fcce6836688e9084f035309e29bf0a2095'
               and depositor = '0x88b0ea576428da635d0fa9deb686765c90cfde2e'
               and evt_block_time = '2022-10-09 06:32'
               and evt_tx_hash = '0x17e3aa6de17838e3b9d6b41c7c4db396ae7c176fdd1b630d071cd21ed616fe37'),
    unit_test2
    as (select case
                 when amount = -21 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_supply' )}}
        where  symbol = 'USDC'
               and depositor = '0x4ecb5300d9ec6bca09d66bfd8dcb532e3192dda1'
               and evt_block_time = '2022-10-09 05:01')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false