{% set blockchain = 'nova' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'withdrawals'
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    , partition_by = ['block_month']
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_celer_v1_withdrawals'
] %}

{{ bridges_withdrawals(blockchain, bridges_platforms) }}
