{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v3_arbitrum',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "uniswap_v3",
                                \'["Henrystats"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '3'
  and blockchain = 'arbitrum'