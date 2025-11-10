{{ config(
    schema = 'bridges_evms'
    , alias = 'withdrawals_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['withdrawal_chain','tx_hash','evt_index','bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set chains = [
    'arbitrum'
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
    , 'unichain'
    , 'worldchain'
    , 'zksync'
    , 'zora'
] %}

SELECT *
FROM (
        {% for chain in chains %}
        SELECT w.deposit_chain_id
        , w.deposit_chain
        , w.withdrawal_chain
        , w.bridge_name
        , w.bridge_version
        , w.block_date
        , w.block_time
        , w.block_number
        , w.withdrawal_amount_raw
        , w.sender
        , w.recipient
        , w.withdrawal_token_standard
        , w.withdrawal_token_address
        , w.tx_from
        , w.tx_hash
        , w.evt_index
        , w.contract_address
        , w.bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_withdrawals') }} w
        {% if is_incremental() %}
        LEFT JOIN {{this}} t ON t.withdrawal_chain = '{{chain}}'
            AND w.bridge_name = t.bridge_name
            AND w.bridge_version = t.bridge_version
            AND w.deposit_chain_id = t.deposit_chain_id
            AND w.tx_hash = t.tx_hash
            AND w.evt_index = t.evt_index
            AND w.bridge_transfer_id = t.bridge_transfer_id
        WHERE  {{ incremental_predicate('w.block_time') }}
        AND t.block_time IS NULL
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
