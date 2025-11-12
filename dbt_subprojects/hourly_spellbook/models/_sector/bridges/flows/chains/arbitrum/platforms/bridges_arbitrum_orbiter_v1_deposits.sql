{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'orbiter_v1_deposits',
    materialized = 'view',
    )
}}

{{cctp_v1_deposits(blockchain = blockchain
    , first_block_number = 2494021 --src: https://dune.com/queries/6197210
    )}}