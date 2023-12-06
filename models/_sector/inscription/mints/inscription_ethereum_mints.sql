{% set blockchain = 'ethereum' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'mints',
        unique_key = ['blockchain', 'tx_hash']
        )
}}

{{inscription_mints(
        all_inscriptions = ref('inscription_' + blockchain + '_all')
)}}