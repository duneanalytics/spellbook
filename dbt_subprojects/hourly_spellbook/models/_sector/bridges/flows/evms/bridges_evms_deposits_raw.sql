{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'view'
)
}}

{% set chains = [
    'abstract'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'blast'
    , 'bnb'
    , 'boba'
    , 'corn'
    , 'ethereum'
    , 'fantom'
    , 'gnosis'
    , 'flare'
    , 'hyperevm'
    , 'ink'
    , 'katana'
    , 'lens'
    , 'linea'
    , 'mantle'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'plasma'
    , 'polygon'
    , 'scroll'
    , 'sei'
    , 'sonic'
    , 'taiko'
    , 'unichain'
    , 'worldchain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT deposit_chain
    , withdrawal_chain_id
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , block_date
    , block_time
    , block_number
    , deposit_amount_raw
    , sender
    , recipient
    , deposit_token_standard
    , deposit_token_address
    , tx_from
    , tx_hash
    , evt_index
    , contract_address
    , bridge_transfer_id
    FROM {{ ref('bridges_'~chain~'_deposits') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
