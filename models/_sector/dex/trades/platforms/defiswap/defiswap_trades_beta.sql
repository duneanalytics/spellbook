{{ config(
    schema = 'defiswap',
    alias = 'trades_beta',
    materialized = 'view'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'defiswap'