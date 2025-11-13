{% set blockchain = 'linea' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'orbiter_v1_deposits',
    materialized = 'view',
    )
}}

{{orbiter_v1_deposits(blockchain = blockchain
    , first_block_number = 7585
    )}}

--first_block_number src: https://dune.com/queries/6197210

