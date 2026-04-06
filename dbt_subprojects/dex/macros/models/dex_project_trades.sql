{# Generates a UNION ALL across all chain-level dex trades tables, filtered to a specific project.
   Used by project-level trade views (e.g. aerodrome.trades, sushiswap.trades).
   Args: project (str) - project name to filter on, version (str, optional) - version filter #}
{% macro dex_project_trades(project, version=None) %}

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
    , 'megaeth'
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
    , 'tempo'
    , 'unichain'
    , 'worldchain'
    , 'xlayer'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

{% for chain in chains %}
SELECT
    blockchain
    , project
    , version
    , block_month
    , block_date
    , block_time
    , block_number
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
FROM {{ ref('dex_' ~ chain ~ '_trades') }}
WHERE project = '{{ project }}'
{% if version is not none %}
    AND version = '{{ version }}'
{% endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}

{% endmacro %}
