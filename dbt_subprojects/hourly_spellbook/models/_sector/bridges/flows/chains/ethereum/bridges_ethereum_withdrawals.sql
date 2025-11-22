{% set blockchain = 'ethereum' %}

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
    'bridges_' + blockchain + '_cctp_v1_withdrawals'
    , 'bridges_' + blockchain + '_celer_v1_withdrawals'
    , 'bridges_' + blockchain + '_across_v2_withdrawals'
    , 'bridges_' + blockchain + '_across_v3_withdrawals'
    , 'bridges_' + blockchain + '_debridge_v1_withdrawals'
    , 'bridges_' + blockchain + '_across_v3_withdrawals'
    , 'bridges_' + blockchain + '_arbitrum_native_v1_withdrawals'
    , 'bridges_' + blockchain + '_ronin_native_v1_withdrawals'
    , 'bridges_' + blockchain + '_ronin_native_v2_withdrawals'
    , 'bridges_' + blockchain + '_lighter_v1_withdrawals'
    , 'bridges_' + blockchain + '_avalanche_c_native_v2_withdrawals'
    , 'bridges_' + blockchain + '_zkync_native_v2_withdrawals'
    , 'bridges_' + blockchain + '_blast_native_v1_withdrawals'
    , 'bridges_' + blockchain + '_sui_native_v1_withdrawals'
    , 'bridges_' + blockchain + '_synapse_rfq_withdrawals'
    , 'bridges_' + blockchain + '_polygon_native_v1_withdrawals'
    , 'bridges_' + blockchain + '_agglayer_v1_withdrawals'
] %}

{{ bridges_withdrawals(blockchain, bridges_platforms) }}
