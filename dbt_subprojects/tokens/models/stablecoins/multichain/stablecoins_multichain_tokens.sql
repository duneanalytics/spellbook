{{
    config(
        schema = 'stablecoins_multichain',
        alias = 'tokens',
        materialized = 'table',
        tags = ['tokens', 'stablecoins', 'multichain', 'static']
    )
}}

select
    s.blockchain,
    cast(null as varchar) as contract_address,
    cast(s.token_mint_address as varchar) as token_mint_address,
    s.currency,
    s.backing,
    s.symbol,
    s.decimals,
    s.name
from {{ source('tokens_solana', 'spl_stablecoins') }} s

union all

select
    e.blockchain,
    cast(e.contract_address as varchar) as contract_address,
    cast(null as varchar) as token_mint_address,
    e.currency,
    e.backing,
    e.symbol,
    e.decimals,
    e.name
from {{ source('tokens', 'erc20_stablecoins') }} e

union all

select
    t.blockchain,
    cast(t.contract_address as varchar) as contract_address,
    cast(null as varchar) as token_mint_address,
    t.currency,
    cast(null as varchar) as backing,
    cast(null as varchar) as symbol,
    cast(null as integer) as decimals,
    cast(null as varchar) as name
from {{ source('tokens_tron', 'trc20_stablecoins') }} t
