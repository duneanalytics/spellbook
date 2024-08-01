{% set blockchain = 'polygon' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'deploys',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscription_deploys(
        all_inscriptions = ref('inscription_' + blockchain + '_all')
)}}