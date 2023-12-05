{% set blockchain = 'gnosis' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'mints',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscription_mints(
        blockchain = blockchain
        , all_inscriptions = ref('inscription_' + blockchain + '_all')
        , first_inscription_block = 28732751
)}}
-- First inscription block is 28732751, 2023-07-01 16:09: https://dune.com/queries/3253998