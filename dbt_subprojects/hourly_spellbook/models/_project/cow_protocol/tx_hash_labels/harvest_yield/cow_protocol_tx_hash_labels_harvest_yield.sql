{{
    config(
        alias = 'tx_hash_labels_harvest_yield'
        , post_hook='{{ hide_spells() }}'
        
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_harvest_yield_ethereum') }}