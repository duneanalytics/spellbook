with unit_test1
    as (select case
                 when usd_amount = 0.999989 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_borrow' )}}
        where  token_address = 0x7f5c764cbc14f9669b88837ca1490cca17c31607
               and borrower = 0xf2eebb119be9313bafc22a031a29a94afc79e3c9
               and block_time = TIMESTAMP '2022-10-11 16:05'
               and tx_hash = 0x27b52c00cf1b28b955b48c72b406b3f41f6abe6d4dfa1ea16b682fca414cb3da),
    unit_test2
    as (select case
                 when amount = -1 then true
                 else false
               end as test
        from   {{ ref('aave_optimism_borrow' )}}
        where  symbol = 'DAI'
               and repayer = 0xa9f12ca1b27941c05e3998279c06b81b8e33ca81
               and block_time = TIMESTAMP '2022-10-11 10:11')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false
