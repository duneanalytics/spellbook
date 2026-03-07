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
    s.token_mint_address as token_address,
    s.currency,
    metadata.symbol,
    metadata.decimals
from {{ source('tokens_solana', 'spl_stablecoins') }} s
left join {{ source('tokens_solana', 'fungible') }} as metadata
    on metadata.token_mint_address = s.token_mint_address
    and metadata.address_prefix = lower(substring(s.token_mint_address, 1, 1))

union all

select
    e.blockchain,
    cast(e.contract_address as varchar) as token_address,
    e.currency,
    e.symbol,
    e.decimals
from {{ source('tokens', 'erc20_stablecoins') }} e

union all

select
    t.blockchain,
    cast(t.contract_address as varchar) as token_address,
    t.currency,
    tokens_erc20.symbol,
    tokens_erc20.decimals
from {{ source('tokens_tron', 'trc20_stablecoins') }} t
left join {{ source('tokens', 'erc20') }} as tokens_erc20
    on tokens_erc20.blockchain = t.blockchain
    and tokens_erc20.contract_address = t.contract_address
