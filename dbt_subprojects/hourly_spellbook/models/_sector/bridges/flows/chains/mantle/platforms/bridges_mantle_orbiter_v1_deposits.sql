{% set blockchain = 'mantle' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'orbiter_v1_deposits',
    materialized = 'view',
    )
}}

{{orbiter_v1_deposits(blockchain = blockchain
    , first_block_number = 38247
    )}}

--first_block_number src: https://dune.com/queries/6197210

