{% set blockchain = 'ethereum' %}

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
    , 'bridges_' + blockchain + '_across_v1_deposits'
    , 'bridges_' + blockchain + '_across_v2_deposits'
    , 'bridges_' + blockchain + '_across_v3_deposits'
    , 'bridges_' + blockchain + '_arbitrum_native_v1_deposits'
    , 'bridges_' + blockchain + '_ronin_native_v1_deposits'
    , 'bridges_' + blockchain + '_ronin_native_v2_deposits'
    , 'bridges_' + blockchain + '_lighter_v1_deposits'
    , 'bridges_' + blockchain + '_avalanche_c_native_v2_deposits'
    , 'bridges_' + blockchain + '_zkync_native_v2_deposits'
    , 'bridges_' + blockchain + '_blast_native_v1_deposits'
    , 'bridges_' + blockchain + '_sui_native_v1_deposits'
    , 'bridges_' + blockchain + '_synapse_rfq_deposits'
    , 'bridges_' + blockchain + '_zkbridge_v1_deposits'
    , 'bridges_' + blockchain + '_rainbow_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v2_deposits'
    , 'bridges_' + blockchain + '_beamer_v3_deposits'
    , 'bridges_' + blockchain + '_polygon_native_v1_deposits'
    , 'bridges_' + blockchain + '_allbridge_classic_deposits'
    , 'bridges_' + blockchain + '_nitro_v1_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
    , 'bridges_' + blockchain + '_connext_v1_deposits'
    , 'bridges_' + blockchain + '_agglayer_v1_deposits'
    , 'bridges_' + blockchain + '_axelar_gateway_deposits'
    , 'bridges_' + blockchain + '_layerzero_deposits'
] %}

{{ bridges_deposits(blockchain, bridges_platforms) }}
