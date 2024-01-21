with unit_test1
    as (select case
                 when usd_amount = 1.31305 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_supply' )}}
        where  token_address = 0x4200000000000000000000000000000000000006
               and depositor = 0x76d3030728e52deb8848d5613abade88441cbc59
               and block_time = TIMESTAMP '2022-10-10 10:27'
               and tx_hash = 0x792ac57b533bc85114f70792b78891b4e1a8daf206c2e0c75a426457657777b4),
    unit_test2
    as (select case
                 when amount = -20 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_supply' )}}
        where  symbol = 'USDC'
               and depositor = 0x4ecb5300d9ec6bca09d66bfd8dcb532e3192dda1
               and block_time = TIMESTAMP '2022-10-10 10:10')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false
