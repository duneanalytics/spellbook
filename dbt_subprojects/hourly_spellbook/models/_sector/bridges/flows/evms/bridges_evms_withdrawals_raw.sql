{{ config(
    schema = 'bridges_evms'
    , alias = 'withdrawals_raw'
    , materialized = 'view'
)
}}

{% set chains = [
    'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'blast'
    , 'bnb'
    , 'corn'
    , 'ethereum'
    , 'fantom'
    , 'gnosis'
    , 'flare'
    , 'hyperevm'
    , 'ink'
    , 'lens'
    , 'linea'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'plasma'
    , 'polygon'
    , 'scroll'
    , 'sei'
    , 'unichain'
    , 'worldchain'
    , 'zksync'
    , 'zora'
] %}

SELECT *
FROM (
        {% for chain in chains %}
        SELECT deposit_chain_id
            , deposit_chain
            , withdrawal_chain
            , bridge_name
            , bridge_version
            , block_date
            , block_time
            , block_number
            , withdrawal_amount_raw
            , sender
            , recipient
            , withdrawal_token_address
            , withdrawal_token_standard
            , tx_from
            , tx_hash
            , evt_index
            , contract_address
            , bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_withdrawals') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
