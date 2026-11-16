{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'cronos'
    , 'ethereum'
    , 'flare'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'mezo'
    , 'monad'
    , 'morph'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'robinhood'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'somnia'
    , 'sonic'
    , 'taiko'
    , 'tempo'
    , 'tezos_evm'
    , 'unichain'
    , 'worldchain'
    , 'xlayer'
    , 'zksync'
] %}

{{ config(
    schema = 'dex'
    , alias = 'token_volumes_daily'
    , materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{% for chain in chains %}
SELECT
    blockchain
    , block_month
    , block_date
    , token_address
    , symbol
    , volume_raw
    , volume
    , volume_usd
FROM
    {{ ref('dex_'~chain~'_token_volumes_daily') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}

