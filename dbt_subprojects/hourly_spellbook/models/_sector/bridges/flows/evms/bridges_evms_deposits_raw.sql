{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'view'
)
}}

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
    , 'ethereum'
    , 'gnosis'
    , 'flare'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'lens'
    , 'linea'
    , 'mantle'
    , 'opbnb'
    , 'optimism'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'sonic'
    , 'taiko'
    , 'tron'
    , 'unichain'
    , 'worldchain'
    , 'zksync'
] %}

SELECT *
FROM (
    {% for chain in chains %}
    SELECT deposit_chain
    , withdrawal_chain
    , withdrawal_chain_id
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
