{% set chains = [
    'abstract',
    'apechain',
    'arbitrum',
    'avalanche_c',
    'b3',
    'base',
    'berachain',
    'bnb',
    'bob',
    'boba',
    'celo',
    'corn',
    'degen',
    'ethereum',
    'fantom',
    'flare',
    'flow',
    'gnosis',
    'hemi',
    'henesys',
    'hyperevm',
    'ink',
    'kaia',
    'katana',
    'linea',
    'mantle',
    'megaeth',
    'monad',
    'nova',
    'opbnb',
    'optimism',
    'peaq',
    'plasma',
    'plume',
    'polygon',
    'ronin',
    'scroll',
    'sei',
    'sepolia',
    'shape',
    'somnia',
    'sonic',
    'sophon',
    'story',
    'superseed',
    'tac',
    'taiko',
    'tron',
    'unichain',
    'viction',
    'worldchain',
    'xlayer',
    'zkevm',
    'zksync',
] %}

{{
  config(
    schema = 'stablecoins',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['blockchain', 'day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
                                  spell_type = "sector",
                                  spell_name = "stablecoins",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select *
from (
    {% for chain in chains %}
    select
        blockchain,
        day,
        address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance_raw,
        balance,
        balance_usd,
        last_updated
    from {{ ref('stablecoins_' ~ chain ~ '_balances') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('day') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
