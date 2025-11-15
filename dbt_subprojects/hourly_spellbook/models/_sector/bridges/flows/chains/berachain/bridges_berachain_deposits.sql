{% set blockchain = 'berachain' %}

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
    'bridges_' + blockchain + '_tether_v1_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
] %}

{{ bridges_deposits(blockchain, bridges_platforms) }}
