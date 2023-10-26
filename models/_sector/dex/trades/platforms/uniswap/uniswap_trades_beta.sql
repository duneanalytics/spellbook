{{ config(
    schema = 'uniswap',
    alias = 'trades_beta',
    materialized = 'view'
    )
}}

-- uniswap_v1_ethereum.base_trades --> dex_ethereum.trades --> dex.trades --> uniswap.trades
select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'