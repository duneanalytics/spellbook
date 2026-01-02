{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'blast'
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'ethereum'
    , 'fantom'
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
    , 'mezo'
    , 'monad'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'unichain'
    , 'worldchain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

{{ config(
    schema = 'dex'
    , alias = 'token_volumes_daily'
    , materialized = 'view'
    , post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                    spell_type = "sector",
                                    spell_name = "dex",
                                    contributors = \'["kryptaki"]\') }}'
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

