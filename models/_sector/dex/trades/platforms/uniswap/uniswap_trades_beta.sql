{{ config(
    tags = ['dunesql'],
    schema = 'uniswap',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap",
                                \'["jeff-dude","mtitus6", "Henrystats", "chrispearcx", "wuligy", "tomfutago"]\') }}'
    )
}}

-- uniswap_v1_ethereum.base_trades --> dex_ethereum.trades --> dex.trades --> uniswap.trades
select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'