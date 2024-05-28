{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_received'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{addresses_events_first_received(
    blockchain = blockchain
    , token_transfers = ref('tokens_' + blockchain +'_transfers')
)}}