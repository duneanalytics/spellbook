{% set blockchain = 'avalanche_c' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , partition_by = ['block_month']
    , incremental_strategy='merge'
    , unique_key = ['tx_hash', 'evt_index', 'bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_cctp_v1_deposits'
    , 'bridges_' + blockchain + '_celer_v1_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
    , 'bridges_' + blockchain + '_nitro_v1_deposits'
    , 'bridges_' + blockchain + '_avalanche_c_bitcoin_v1_deposits'
    , 'bridges_' + blockchain + '_axelar_gateway_deposits'
    , 'bridges_' + blockchain + '_allbridge_classic_deposits'
    , 'bridges_' + blockchain + '_layerzero_deposits'
] %}

{{ bridges_deposits(blockchain, bridges_platforms) }}
