{{ config(
    schema = 'liquidifty',
    alias = 'trades',
    post_hook = '{{ expose_spells(\'["bnb", "ethereum"]\',
                                    "project",
                                    "liquidifty",
                                    \'["bizzyvinci"]\') }}'
)}}

select * from {{ ref('liquidifty_bnb_trades') }}
union all
select * from {{ ref('liquidifty_ethereum_trades') }}
