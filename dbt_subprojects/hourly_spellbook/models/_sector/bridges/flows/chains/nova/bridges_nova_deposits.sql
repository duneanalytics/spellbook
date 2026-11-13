{% set blockchain = 'nova' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'deposits'
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    , partition_by = ['block_month']
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_celer_v1_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
    , 'bridges_' + blockchain + '_orbiter_v1_deposits'
] %}

{{ bridges_deposits(blockchain, bridges_platforms) }}
