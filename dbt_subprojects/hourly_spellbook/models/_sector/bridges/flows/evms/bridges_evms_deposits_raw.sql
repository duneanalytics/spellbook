{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','tx_hash','evt_index','bridge_transfer_id']
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
FROM (
    SELECT *
        , ROW_NUMBER() OVER (PARTITION BY deposit_chain, tx_hash, evt_index, bridge_transfer_id ORDER BY block_number, block_time) AS rn
    FROM (
        {% for chain in chains %}
        SELECT d.deposit_chain
        , d.withdrawal_chain_id
        , d.withdrawal_chain
        , d.bridge_name
        , d.bridge_version
        , d.block_date
        , d.block_time
        , d.block_number
        , cast(d.deposit_amount_raw as uint256) as deposit_amount_raw
        , d.sender
        , d.recipient
        , d.deposit_token_standard
        , d.deposit_token_address
        , d.tx_from
        , d.tx_hash
        , d.evt_index
        , d.contract_address
        , d.bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_deposits') }} d
        {% if is_incremental() %}
        LEFT JOIN {{this}} t ON t.deposit_chain = '{{chain}}'
            AND d.bridge_name = t.bridge_name
            AND d.bridge_version = t.bridge_version
            AND d.withdrawal_chain_id = t.withdrawal_chain_id
            AND d.tx_hash = t.tx_hash
            AND d.evt_index = t.evt_index
            AND d.bridge_transfer_id = t.bridge_transfer_id
        WHERE {{ incremental_predicate('d.block_time') }}
        AND t.block_time IS NULL
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
    )
WHERE rn = 1
