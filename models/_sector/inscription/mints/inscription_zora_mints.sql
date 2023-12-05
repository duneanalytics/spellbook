{% set blockchain = 'zora' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'mints',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

{{inscription_mints(
        all_inscriptions = ref('inscription_' + blockchain + '_all')
)}}