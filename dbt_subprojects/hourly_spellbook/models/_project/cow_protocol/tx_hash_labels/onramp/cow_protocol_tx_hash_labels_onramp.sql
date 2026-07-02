{{
    config(
        alias = 'tx_hash_labels_onramp'
        , post_hook='{{ hide_spells() }}'
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_onramp_ethereum') }}