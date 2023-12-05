{% set blockchain = 'celo' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'deploys',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

{{inscription_deploys(
        blockchain = blockchain
        , all_inscriptions = ref('inscription_' + blockchain + '_all')
        , first_inscription_block = 20374169
)}}
-- First inscription block is 20374169, 2023-07-16 20:25: https://dune.com/queries/3254010