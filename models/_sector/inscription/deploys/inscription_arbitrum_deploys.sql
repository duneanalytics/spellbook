{% set blockchain = 'arbitrum' %}

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
        all_inscriptions = ref('inscription_' + blockchain + '_all')
)}}