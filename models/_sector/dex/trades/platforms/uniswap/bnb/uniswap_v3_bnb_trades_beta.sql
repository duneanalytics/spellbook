{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v3_bnb',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "uniswap_v3",
                                \'["chrispearcx"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '3'
  and blockchain = 'bnb'