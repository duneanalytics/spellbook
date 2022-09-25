with unit_test1
    as (select case
                 when usd_amount = -21150 then true
                 else false
               end as test
        from   {{ ref('aave_v2_supply' )}}
        where  token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
               and depositor = '0x4767192455266e422386d14991d697a418c63225'
               and evt_block_time = '2022-09-21 18:09'),
    unit_test2
    as (select case
                 when amount = 250 then true
                 else false
               end as test
        from   {{ ref('aave_v2_supply' )}}
        where  symbol = 'SNX'
               and depositor = '0x1e18b8b6f27568023e0b577cbee1889391b2f444'
               and evt_block_time = '2022-09-24 00:05')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false
