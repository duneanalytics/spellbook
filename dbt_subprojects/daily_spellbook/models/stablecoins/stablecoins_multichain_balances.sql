{% set chains = [
  'abstract',
  'arbitrum',
  'avalanche_c',
  'base',
  'berachain',
  'bnb',
  'bob',
  'celo',
  'ethereum',
  'fantom',
  'flare',
  'gnosis',
  'hemi',
  'hyperevm',
  'ink',
  'kaia',
  'katana',
  'linea',
  'mantle',
  'monad',
  'opbnb',
  'optimism',
  'plasma',
  'plume',
  'polygon',
  'ronin',
  'scroll',
  'sei',
  'solana',
  'somnia',
  'sonic',
  'story',
  'taiko',
  'tron',
  'unichain',
  'worldchain',
  'xlayer',
  'zksync'
] %}

{{
  config(
    tags = ['stablecoins'],
    schema = 'stablecoins_multichain',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
        spell_type = "sector",
        spell_name = "stablecoins_multichain",
        contributors = \'["tomfutago"]\') }}'
  )
}}

select
  blockchain,
  day,
  cast(address as varchar) as address,
  token_symbol,
  cast(token_address as varchar) as token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_evm_balances') }}
union all
select
  blockchain,
  day,
  cast(address as varchar) as address,
  token_symbol,
  cast(token_address as varchar) as token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ source('stablecoins_svm', 'balances') }}
union all
select
  blockchain,
  day,
  address_varchar as address,
  token_symbol,
  cast(token_address as varchar) as token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_tron_balances') }}
