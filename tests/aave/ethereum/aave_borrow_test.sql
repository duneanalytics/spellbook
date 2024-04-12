with unit_test1
    as (select case
                 when usd_amount = 499785 then true
                 else false
               end as test
        from   {{ ref('aave_ethereum_borrow' )}}
        where  token_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
               and borrower = 0xec46dd165ee2d4af460a9c3d01b5a4c9516c9c3f
               and block_time = TIMESTAMP '2022-09-22 10:45'
               and tx_hash = 0x4c052a2865b828ef00dd2840870c08d2074930571c00c1f57f579acbab3e25c8),
    unit_test2
    as (select case
                 when amount = -100 then true
                 else false
               end as test
        from   {{ ref('aave_ethereum_borrow' )}}
        where  symbol = 'DAI'
               and repayer = 0x0d67d6aab7dab14da3b72ca70e0b83b74ad7e88f
               and block_time = TIMESTAMP '2022-09-22 19:40')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false
