{% set blockchain = 'zkevm' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'orbiter_v1_deposits',
    materialized = 'view',
    )
}}

{{orbiter_v1_deposits(blockchain = blockchain
    , first_block_number = 9842
    )}}

--first_block_number src: https://dune.com/queries/6197210

