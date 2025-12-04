{% set blockchain = 'bnb' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'withdrawals'
    , materialized = 'incremental'
    , file_format = 'delta'
    , partition_by = ['block_month']
    , incremental_strategy='merge'
    , unique_key = ['tx_hash', 'evt_index', 'bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_across_v3_withdrawals'
    , 'bridges_' + blockchain + '_synapse_rfq_withdrawals'
    , 'bridges_' + blockchain + '_celer_v1_withdrawals'
] %}

{{ bridges_withdrawals(blockchain, bridges_platforms) }}
