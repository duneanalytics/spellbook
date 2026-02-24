{{
    config(
        alias = 'tx_hash_labels_offramp'
        , post_hook='{{ hide_spells() }}'
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_offramp_ethereum') }}