{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v3_base',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "uniswap_v3",
                                \'["wuligy"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '3'
  and blockchain = 'base'